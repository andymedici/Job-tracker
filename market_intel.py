"""
Market Intelligence Module
==========================
Advanced analytics and tracking for job market intelligence.

Features:
- Location expansion detection (new cities/regions)
- Job count change alerts (surges/declines)
- Historical data archival
- 6-hourly snapshot creation
- Market-wide snapshot creation
- Trend analysis and reporting
- Email notifications
"""

import json
import logging
import os
import smtplib
import ssl
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dataclasses import dataclass, field

from database import get_db, Database

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class LocationExpansion:
    """Represents a company expanding to a new location."""
    company_id: str
    company_name: str
    ats_type: str
    new_location: str
    detected_at: datetime
    job_count_at_detection: int = 0


@dataclass
class JobCountChange:
    """Represents a significant change in job count."""
    company_id: str
    company_name: str
    ats_type: str
    previous_count: int
    current_count: int
    change_percent: float
    change_type: str  # 'surge', 'decline', 'new'
    detected_at: datetime


@dataclass
class MarketIntelReport:
    """Container for market intelligence data."""
    generated_at: datetime
    period_start: datetime
    period_end: datetime
    
    # Summary stats
    total_companies: int = 0
    total_jobs: int = 0
    new_companies: int = 0
    
    # Work type breakdown
    remote_jobs: int = 0
    hybrid_jobs: int = 0
    onsite_jobs: int = 0
    
    # Intelligence
    location_expansions: List[LocationExpansion] = field(default_factory=list)
    job_surges: List[JobCountChange] = field(default_factory=list)
    job_declines: List[JobCountChange] = field(default_factory=list)
    new_entrants: List[Dict] = field(default_factory=list)
    
    # Trends
    month_over_month_change: float = 0.0
    top_growing_companies: List[Dict] = field(default_factory=list)
    top_shrinking_companies: List[Dict] = field(default_factory=list)


class MarketIntelligence:
    """Advanced market intelligence and analytics."""
    
    # Thresholds for alerts
    SURGE_THRESHOLD = 0.50  # 50% increase
    DECLINE_THRESHOLD = -0.30  # 30% decrease
    MIN_JOBS_FOR_ALERT = 5  # Minimum jobs to trigger alerts
    
    # Locations to ignore (too generic)
    GENERIC_LOCATIONS = {
        'remote', 'anywhere', 'global', 'worldwide', 'various',
        'multiple', 'tbd', 'flexible', 'distributed', 'virtual',
        'usa', 'us', 'united states', 'europe', 'asia', 'americas',
        'emea', 'apac', 'latam', 'north america', 'worldwide'
    }
    
    def __init__(self, db: Database = None):
        self.db = db or get_db()
    
    # ==================== LOCATION EXPANSION DETECTION ====================
    
    def detect_location_expansions(self) -> List[LocationExpansion]:
        """Detect companies expanding to new locations."""
        expansions = []
        
        try:
            with self.db.get_cursor() as cursor:
                cursor.execute("""
                    SELECT id, company_name, ats_type, locations, job_count
                    FROM companies
                    WHERE locations IS NOT NULL
                """)
                companies = list(cursor.fetchall())
            
            for row in companies:
                company_id = row['id']
                current_locations = set()
                
                try:
                    locations_list = row['locations'] if isinstance(row['locations'], list) else []
                    current_locations = {
                        loc.lower().strip() 
                        for loc in locations_list 
                        if self._is_meaningful_location(loc)
                    }
                except:
                    continue
                
                if not current_locations:
                    continue
                
                # Get previously known locations from archive
                with self.db.get_cursor() as cursor:
                    cursor.execute("""
                        SELECT locations_json FROM job_history_archive
                        WHERE company_id = %s
                        ORDER BY archive_date DESC
                        LIMIT 1
                    """, (company_id,))
                    prev_row = cursor.fetchone()

                previous_locations = set()
                if prev_row and prev_row['locations_json']:
                    try:
                        prev_list = prev_row['locations_json'] if isinstance(prev_row['locations_json'], list) else []
                        previous_locations = { 
                            loc.lower().strip() 
                            for loc in prev_list 
                            if self._is_meaningful_location(loc)
                        }
                    except:
                        pass
                
                # Find new locations
                new_locations = current_locations - previous_locations
                
                for new_loc in new_locations:
                    # Check if we already recorded this expansion recently
                    with self.db.get_cursor() as cursor:
                        cursor.execute("""
                            SELECT 1 FROM location_expansions
                            WHERE company_id = %s AND new_location = %s 
                            AND detected_at > NOW() - INTERVAL '7 days'
                        """, (company_id, new_loc.title()))
                        if cursor.fetchone():
                            continue
                            
                    expansion = LocationExpansion(
                        company_id=company_id,
                        company_name=row['company_name'],
                        ats_type=row['ats_type'],
                        new_location=new_loc.title(),
                        detected_at=datetime.utcnow(),
                        job_count_at_detection=row['job_count']
                    )
                    expansions.append(expansion)
                    self._record_location_expansion(expansion)
            
            return expansions
            
        except Exception as e:
            logger.error(f"Error detecting location expansions: {e}")
            return []
            
    def _is_meaningful_location(self, location: str) -> bool:
        """Check if a location string is not generic."""
        return location.lower().strip() not in self.GENERIC_LOCATIONS
        
    def _record_location_expansion(self, expansion: LocationExpansion):
        """Insert a location expansion record into the database."""
        try:
            with self.db.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO location_expansions 
                    (company_id, new_location, job_count_at_detection, detected_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (company_id, new_location) DO NOTHING
                """, (
                    expansion.company_id,
                    expansion.new_location,
                    expansion.job_count_at_detection,
                    expansion.detected_at
                ))
        except Exception as e:
            logger.error(f"Error recording location expansion: {e}")
            
    # ==================== JOB COUNT CHANGE DETECTION ====================
    
    def detect_job_count_changes(self) -> List[JobCountChange]:
        """Detect significant job count surges or declines."""
        changes = []
        
        try:
            with self.db.get_cursor() as cursor:
                cursor.execute("""
                    SELECT id, company_name, ats_type, job_count, last_job_count
                    FROM companies
                    WHERE job_count != last_job_count 
                      AND last_job_count >= %s
                      AND job_count >= %s
                """, (self.MIN_JOBS_FOR_ALERT, self.MIN_JOBS_FOR_ALERT))
                
                companies = list(cursor.fetchall())
                
                for row in companies:
                    company_id = row['id']
                    current = row['job_count']
                    previous = row['last_job_count']
                    
                    if previous == 0:
                        continue
                        
                    change_percent = (current - previous) / previous
                    change_type = None
                    
                    if change_percent >= self.SURGE_THRESHOLD:
                        change_type = 'surge'
                    elif change_percent <= self.DECLINE_THRESHOLD:
                        change_type = 'decline'
                        
                    if change_type:
                        change = JobCountChange(
                            company_id=company_id,
                            company_name=row['company_name'],
                            ats_type=row['ats_type'],
                            previous_count=previous,
                            current_count=current,
                            change_percent=round(change_percent, 4),
                            change_type=change_type,
                            detected_at=datetime.utcnow()
                        )
                        changes.append(change)
                        self._record_job_count_change(change)
            
            return changes
            
        except Exception as e:
            logger.error(f"Error detecting job count changes: {e}")
            return []
            
    def _record_job_count_change(self, change: JobCountChange):
        """Insert a job count change record into the database."""
        try:
            with self.db.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO job_count_changes 
                    (company_id, previous_count, current_count, change_percent, change_type, detected_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    change.company_id,
                    change.previous_count,
                    change.current_count,
                    change.change_percent,
                    change.change_type,
                    change.detected_at
                ))
        except Exception as e:
            logger.error(f"Error recording job count change: {e}")

    # ==================== ARCHIVAL AND PURGING ====================
    
    def archive_daily_snapshot(self):
        """Archive current job count, locations, and departments for all companies."""
        try:
            with self.db.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO job_history_archive (company_id, archive_date, job_count, locations_json, departments_json)
                    SELECT id, CURRENT_DATE, job_count, locations, departments FROM companies
                    ON CONFLICT (company_id, archive_date) DO UPDATE SET
                        job_count = EXCLUDED.job_count,
                        locations_json = EXCLUDED.locations_json,
                        departments_json = EXCLUDED.departments_json
                """)
                logger.info("Daily snapshot archived")
        except Exception as e:
            logger.error(f"Error archiving daily snapshot: {e}")

    def archive_job_history(self):
        """Alias for archive_daily_snapshot for backwards compatibility."""
        self.archive_daily_snapshot()
            
    def purge_old_job_details(self, days_to_keep: int = 30):
        """Delete old job detail records to save space."""
        try:
            cutoff = datetime.utcnow() - timedelta(days=days_to_keep)
            with self.db.get_cursor() as cursor:
                cursor.execute("DELETE FROM jobs WHERE last_seen < %s", (cutoff,))
                logger.info(f"Purged {cursor.rowcount} stale job records (older than {days_to_keep} days)")
        except Exception as e:
            logger.error(f"Error purging old job details: {e}")

    def purge_stale_companies(self, days_stale: int = 90):
        """Delete companies that haven't been seen recently and have 0 jobs."""
        try:
            cutoff = datetime.utcnow() - timedelta(days=days_stale)
            with self.db.get_cursor() as cursor:
                cursor.execute("""
                    DELETE FROM companies 
                    WHERE last_seen < %s AND job_count = 0
                """, (cutoff,))
                logger.info(f"Purged {cursor.rowcount} stale companies (not seen in {days_stale} days and 0 jobs)")
        except Exception as e:
            logger.error(f"Error purging stale companies: {e}")

    def purge_old_archives(self, days_to_keep: int = 90):
        """Purge old job history archives."""
        try:
            cutoff = datetime.utcnow() - timedelta(days=days_to_keep)
            with self.db.get_cursor() as cursor:
                cursor.execute("DELETE FROM job_history_archive WHERE archive_date < %s", (cutoff.date(),))
                logger.info(f"Purged {cursor.rowcount} old archive records")
        except Exception as e:
            logger.error(f"Error purging old archives: {e}")

    # ==================== REPORT GENERATION ====================
    
    def generate_report(self, days: int = 7) -> MarketIntelReport:
        """Generate a comprehensive market intelligence report."""
        report = MarketIntelReport(
            generated_at=datetime.utcnow(),
            period_start=datetime.utcnow() - timedelta(days=days),
            period_end=datetime.utcnow()
        )
        
        # Fetch current summary stats
        stats = self.db.get_stats()
        report.total_companies = stats.get('total_companies', 0)
        report.total_jobs = stats.get('total_jobs', 0)
        report.remote_jobs = stats.get('remote_jobs', 0)
        report.hybrid_jobs = stats.get('hybrid_jobs', 0)
        report.onsite_jobs = stats.get('onsite_jobs', 0)
        
        cutoff = report.period_start
        
        with self.db.get_cursor() as cursor:
            # Location Expansions
            cursor.execute("""
                SELECT le.company_id, le.new_location, le.detected_at, le.job_count_at_detection,
                       c.company_name, c.ats_type
                FROM location_expansions le
                JOIN companies c ON le.company_id = c.id
                WHERE le.detected_at >= %s
                ORDER BY le.detected_at DESC
            """, (cutoff,))
            for row in cursor.fetchall():
                report.location_expansions.append(LocationExpansion(
                    company_id=row['company_id'],
                    company_name=row['company_name'],
                    ats_type=row['ats_type'],
                    new_location=row['new_location'],
                    detected_at=row['detected_at'],
                    job_count_at_detection=row['job_count_at_detection'] or 0
                ))
            
            # Job Count Changes
            cursor.execute("""
                SELECT jcc.company_id, jcc.previous_count, jcc.current_count, 
                       jcc.change_percent, jcc.change_type, jcc.detected_at,
                       c.company_name, c.ats_type
                FROM job_count_changes jcc
                JOIN companies c ON jcc.company_id = c.id
                WHERE jcc.detected_at >= %s
                ORDER BY jcc.detected_at DESC
            """, (cutoff,))
            for row in cursor.fetchall():
                change = JobCountChange(
                    company_id=row['company_id'],
                    company_name=row['company_name'],
                    ats_type=row['ats_type'],
                    previous_count=row['previous_count'],
                    current_count=row['current_count'],
                    change_percent=row['change_percent'],
                    change_type=row['change_type'],
                    detected_at=row['detected_at']
                )
                if change.change_type == 'surge':
                    report.job_surges.append(change)
                elif change.change_type == 'decline':
                    report.job_declines.append(change)
                    
            # New Companies
            cursor.execute("""
                SELECT id, company_name, ats_type, job_count
                FROM companies
                WHERE first_seen >= %s
                ORDER BY first_seen DESC
            """, (cutoff,))
            report.new_entrants = [dict(r) for r in cursor.fetchall()]
            report.new_companies = len(report.new_entrants)
            
        return report

    # ==================== EMAIL REPORTER ====================
    
    def send_email_report(self, report: MarketIntelReport, recipient_email: str) -> bool:
        """Sends the market intelligence report via email."""
        sender_email = os.getenv('SMTP_USER')
        sender_password = os.getenv('SMTP_PASSWORD')
        smtp_server = os.getenv('SMTP_HOST', 'smtp.gmail.com')
        smtp_port = int(os.getenv('SMTP_PORT', 587))
        
        if not all([sender_email, sender_password]):
            logger.error("SMTP_USER or SMTP_PASSWORD not configured")
            return False

        message = MIMEMultipart("alternative")
        message["Subject"] = f"Job Intel Report: {report.period_start.strftime('%b %d')} - {report.period_end.strftime('%b %d')}"
        message["From"] = sender_email
        message["To"] = recipient_email
        
        html = self._format_report_to_html(report)
        part = MIMEText(html, "html")
        message.attach(part)

        try:
            context = ssl.create_default_context()
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls(context=context)
                server.login(sender_email, sender_password)
                server.sendmail(sender_email, recipient_email, message.as_string())
            logger.info(f"Successfully sent report to {recipient_email}")
            return True
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False

    def _format_report_to_html(self, report: MarketIntelReport) -> str:
        """Generates HTML body for the report."""
        surges_html = "".join(
            f"<li>{c.company_name} ({c.ats_type}): +{c.current_count - c.previous_count:,} jobs ({c.change_percent*100:.1f}%)</li>" 
            for c in report.job_surges[:10]
        ) or "<li>No surges detected</li>"
        
        declines_html = "".join(
            f"<li>{c.company_name} ({c.ats_type}): -{c.previous_count - c.current_count:,} jobs ({c.change_percent*100:.1f}%)</li>" 
            for c in report.job_declines[:10]
        ) or "<li>No declines detected</li>"
        
        expansions_html = "".join(
            f"<li>{e.company_name} expanded to <strong>{e.new_location}</strong></li>" 
            for e in report.location_expansions[:10]
        ) or "<li>No expansions detected</li>"
        
        return f"""
        <html>
        <head>
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; background: #0a0f1a; color: #f9fafb; padding: 20px; }}
                .container {{ max-width: 600px; margin: 0 auto; background: #111827; padding: 30px; border-radius: 12px; }}
                h1 {{ color: #3b82f6; margin-bottom: 5px; }}
                h2 {{ color: #06b6d4; border-bottom: 1px solid #374151; padding-bottom: 10px; margin-top: 30px; }}
                .stat-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin: 20px 0; }}
                .stat {{ background: #1f2937; padding: 15px; border-radius: 8px; text-align: center; }}
                .stat-value {{ font-size: 1.8em; font-weight: bold; color: #10b981; }}
                .stat-label {{ color: #9ca3af; font-size: 0.85em; }}
                ul {{ list-style-type: none; padding: 0; }}
                li {{ padding: 8px 0; border-bottom: 1px solid #374151; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Job Intelligence Report</h1>
                <p style="color:#9ca3af;">Period: {report.period_start.strftime('%b %d, %Y')} - {report.period_end.strftime('%b %d, %Y')}</p>
                
                <div class="stat-grid">
                    <div class="stat">
                        <div class="stat-value">{report.total_jobs:,}</div>
                        <div class="stat-label">Total Jobs</div>
                    </div>
                    <div class="stat">
                        <div class="stat-value">{report.total_companies:,}</div>
                        <div class="stat-label">Companies</div>
                    </div>
                    <div class="stat">
                        <div class="stat-value">{report.new_companies:,}</div>
                        <div class="stat-label">New This Week</div>
                    </div>
                </div>

                <h2>🚀 Job Surges ({len(report.job_surges)})</h2>
                <ul>{surges_html}</ul>

                <h2>📉 Job Declines ({len(report.job_declines)})</h2>
                <ul>{declines_html}</ul>
                
                <h2>🗺️ Location Expansions ({len(report.location_expansions)})</h2>
                <ul>{expansions_html}</ul>
            </div>
        </body>
        </html>
        """


# ==================== MAIN ENTRY POINT ====================

def run_daily_maintenance(db: Database = None) -> Dict[str, int]:
    """
    Run maintenance tasks (called every 6 hours despite the name):
    1. Detect location expansions
    2. Detect job count changes (surges/declines)
    3. Create 6h snapshots
    4. Create market snapshot
    5. Archive daily snapshot
    6. Create weekly aggregate
    7. Create monthly snapshot (on 1st of month)
    8. Purge old data
    """
    logger.info("Starting maintenance cycle...")
    db = db or get_db()
    intel = MarketIntelligence(db)
    
    # 1. Detect expansions and changes
    expansions = intel.detect_location_expansions()
    changes = intel.detect_job_count_changes()
    
    surges = [c for c in changes if c.change_type == 'surge']
    declines = [c for c in changes if c.change_type == 'decline']
    
    # 2. Create 6h snapshots
    db.create_6h_snapshots()
    
    # 3. Create market snapshot
    db.create_market_snapshot()
    
    # 4. Archive daily snapshot
    intel.archive_daily_snapshot()
    
    # 5. Create weekly aggregate
    db.create_weekly_aggregate()
    
    # 6. Create monthly snapshot on 1st of month (early morning)
    now = datetime.utcnow()
    if now.day == 1 and now.hour < 6:
        db.create_monthly_snapshot()
    
    # 7. Purge old data
    intel.purge_old_job_details(days_to_keep=30)
    intel.purge_stale_companies(days_stale=90)
    intel.purge_old_archives(days_to_keep=90)
    
    logger.info(f"✅ Maintenance complete: {len(expansions)} expansions, {len(surges)} surges, {len(declines)} declines")
    
    return {
        'expansions': len(expansions),
        'surges': len(surges),
        'declines': len(declines),
        'timestamp': datetime.utcnow().isoformat()
    }


def send_weekly_report(db: Database = None, recipient: str = None) -> bool:
    """Generate and send weekly intelligence report."""
    recipient = recipient or os.getenv('EMAIL_RECIPIENT')
    if not recipient:
        logger.warning("No email recipient configured")
        return False
    
    intel = MarketIntelligence(db)
    report = intel.generate_report(days=7)
    return intel.send_email_report(report, recipient)


if __name__ == "__main__":
    intel = MarketIntelligence()
    
    print("Running market intelligence analysis...")
    results = run_daily_maintenance()
    print(f"Maintenance results: {results}")
    
    report = intel.generate_report(days=7)
    print(f"\n📊 Report Summary:")
    print(f"  Companies: {report.total_companies}")
    print(f"  Jobs: {report.total_jobs}")
    print(f"  New Companies: {report.new_companies}")
    print(f"  Surges: {len(report.job_surges)}")
    print(f"  Expansions: {len(report.location_expansions)}")
