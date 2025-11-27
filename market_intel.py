"""
Market Intelligence Module
==========================
Advanced analytics and tracking for job market intelligence:

- Location expansion detection (new cities/regions)
- Job count change alerts (significant increases/decreases)
- Historical data archival and purging
- Trend analysis and reporting
- Email notifications

Uses PostgreSQL for Railway deployment.
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
    SURGE_THRESHOLD = 0.20  # 20% increase
    DECLINE_THRESHOLD = -0.20  # 20% decrease
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
                # Get current company data
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
                    # JSONB is already parsed in PostgreSQL
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
                            WHERE company_id = %s AND new_location = %s AND detected_at > NOW() - INTERVAL '7 days'
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
                    
                    # Record the new expansion
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
                # Select companies where job count has changed and last_job_count > 0 
                # and current job count is above the minimum threshold.
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
                    
                    # Prevent division by zero if last_job_count somehow remained 0 despite filter
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
    
    def archive_job_history(self):
        """Archive job count, locations, and departments for all companies."""
        try:
            with self.db.get_cursor() as cursor:
                # Select the latest state for all companies
                cursor.execute("""
                    INSERT INTO job_history_archive (company_id, job_count, locations_json, departments_json)
                    SELECT id, job_count, locations, departments FROM companies
                    ON CONFLICT (company_id, archive_date) DO NOTHING
                """)
        except Exception as e:
            logger.error(f"Error archiving job history: {e}")
            
    def purge_old_job_details(self, days_to_keep: int):
        """Delete old job detail records to save space."""
        try:
            cutoff = datetime.utcnow() - timedelta(days=days_to_keep)
            with self.db.get_cursor() as cursor:
                cursor.execute("""
                    DELETE FROM jobs WHERE last_seen < %s
                """, (cutoff,))
                logger.info(f"Purged {cursor.rowcount} stale job records (older than {days_to_keep} days).")
        except Exception as e:
            logger.error(f"Error purging old job details: {e}")

    def purge_stale_companies(self, days_stale: int):
        """Delete companies that haven't been seen/updated recently and have 0 jobs."""
        try:
            cutoff = datetime.utcnow() - timedelta(days=days_stale)
            with self.db.get_cursor() as cursor:
                # Companies not seen in N days AND have 0 jobs
                cursor.execute("""
                    DELETE FROM companies 
                    WHERE last_seen < %s AND job_count = 0
                """, (cutoff,))
                logger.info(f"Purged {cursor.rowcount} stale companies (not seen in {days_stale} days and 0 jobs).")
        except Exception as e:
            logger.error(f"Error purging stale companies: {e}")

    # ==================== REPORT GENERATION ====================
    
    def generate_report(self, days: int) -> MarketIntelReport:
        """Generate a comprehensive market intelligence report."""
        report = MarketIntelReport(
            generated_at=datetime.utcnow(),
            period_start=datetime.utcnow() - timedelta(days=days),
            period_end=datetime.utcnow()
        )
        
        # 1. Fetch current summary stats
        stats = self.db.get_stats()
        report.total_companies = stats.get('total_companies', 0)
        report.total_jobs = stats.get('total_jobs', 0)
        report.remote_jobs = stats.get('total_remote', 0)
        report.hybrid_jobs = stats.get('total_hybrid', 0)
        report.onsite_jobs = stats.get('total_onsite', 0)
        
        # 2. Fetch intelligence data (only changes that occurred in the report period)
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
            report.location_expansions = [
                LocationExpansion(**{**dict(row), 'detected_at': row['detected_at']})
                for row in cursor.fetchall()
            ]
            
            # Job Count Changes (Surges/Declines)
            cursor.execute("""
                SELECT jcc.company_id, jcc.previous_count, jcc.current_count, jcc.change_percent, jcc.change_type, jcc.detected_at,
                       c.company_name, c.ats_type
                FROM job_count_changes jcc
                JOIN companies c ON jcc.company_id = c.id
                WHERE jcc.detected_at >= %s
                ORDER BY jcc.detected_at DESC
            """, (cutoff,))
            for row in cursor.fetchall():
                change = JobCountChange(**{**dict(row), 'detected_at': row['detected_at']})
                if change.change_type == 'surge':
                    report.job_surges.append(change)
                elif change.change_type == 'decline':
                    report.job_declines.append(change)
                    
            # New Companies (first_seen in the period)
            cursor.execute("""
                SELECT id, company_name, ats_type, job_count
                FROM companies
                WHERE first_seen >= %s
                ORDER BY first_seen DESC
            """, (cutoff,))
            report.new_entrants = list(cursor.fetchall())
            report.new_companies = len(report.new_entrants)

            # Monthly trends (for top growing/shrinking)
            self._calculate_monthly_trends(report)
            
        return report

    def _calculate_monthly_trends(self, report: MarketIntelReport):
        """Calculate month-over-month change and find top movers."""
        
        # Find the month we are comparing against (2 months ago)
        two_months_ago = (datetime.utcnow().replace(day=1) - timedelta(days=1)).replace(day=1) - timedelta(days=1)
        two_months_ago = two_months_ago.replace(day=1).date()
        
        # Get all monthly snapshots for the last two relevant months
        with self.db.get_cursor() as cursor:
            # Query the change in job count between the latest snapshot and the snapshot two months ago
            cursor.execute("""
                WITH latest_snap AS (
                    SELECT company_id, job_count 
                    FROM monthly_snapshots 
                    WHERE snapshot_date >= NOW() - INTERVAL '30 days'
                ),
                previous_snap AS (
                    SELECT company_id, job_count 
                    FROM monthly_snapshots 
                    WHERE snapshot_date < NOW() - INTERVAL '30 days' 
                    ORDER BY snapshot_date DESC LIMIT 1
                )
                SELECT 
                    c.id, c.company_name, c.ats_type,
                    COALESCE(ls.job_count, 0) - COALESCE(ps.job_count, 0) AS job_change,
                    COALESCE(ls.job_count, 0) AS current_jobs,
                    COALESCE(ps.job_count, 0) AS previous_jobs
                FROM companies c
                LEFT JOIN latest_snap ls ON c.id = ls.company_id
                LEFT JOIN previous_snap ps ON c.id = ps.company_id
                ORDER BY job_change DESC
                LIMIT 50
            """)
            
            # This logic is simplified for top movers based on recent changes
            all_movers = [dict(row) for row in cursor.fetchall() if abs(row['job_change']) > 0]
            
            # Filter and sort
            report.top_growing_companies = sorted([
                m for m in all_movers if m['job_change'] > 0
            ], key=lambda x: x['job_change'], reverse=True)[:10]
            
            report.top_shrinking_companies = sorted([
                m for m in all_movers if m['job_change'] < 0
            ], key=lambda x: x['job_change'], reverse=False)[:10]
            
            
    # ==================== EMAIL REPORTER ====================
    
    def send_email_report(self, report: MarketIntelReport, recipient_email: str) -> bool:
        """Sends the market intelligence report via email."""
        
        # Email configuration from environment variables
        sender_email = os.getenv('SMTP_USER')
        sender_password = os.getenv('SMTP_PASSWORD')
        smtp_server = os.getenv('SMTP_HOST', 'smtp.gmail.com')
        smtp_port = int(os.getenv('SMTP_PORT', 587))
        
        if not all([sender_email, sender_password]):
            logger.error("SMTP_USER or SMTP_PASSWORD not configured. Cannot send email.")
            return False

        message = MIMEMultipart("alternative")
        message["Subject"] = f"Job Intel Weekly Report: {report.period_start.strftime('%b %d')} - {report.period_end.strftime('%b %d')}"
        message["From"] = sender_email
        message["To"] = recipient_email
        
        # Create HTML content (simplified for this example)
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
        """Generates a simple HTML body for the report."""
        # This is a very basic HTML template. In a real app, you'd use a template engine.
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: sans-serif; background-color: #f4f4f9; color: #333; }}
                .container {{ max-width: 600px; margin: 20px auto; background-color: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }}
                h2 {{ color: #007bff; border-bottom: 2px solid #eee; padding-bottom: 10px; }}
                .stat-box {{ background-color: #e9ecef; padding: 10px; border-radius: 4px; margin-bottom: 10px; }}
                ul {{ list-style-type: none; padding: 0; }}
                li {{ margin-bottom: 5px; border-bottom: 1px dotted #ccc; padding-bottom: 5px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Job Intel Weekly Report</h1>
                <p>Period: {report.period_start.strftime('%b %d, %Y')} - {report.period_end.strftime('%b %d, %Y')}</p>
                
                <h2>Summary Metrics</h2>
                <div class="stat-box">
                    <p><strong>Total Jobs:</strong> {report.total_jobs:,}</p>
                    <p><strong>Total Companies Tracked:</strong> {report.total_companies:,}</p>
                    <p><strong>New Companies Added:</strong> {report.new_companies:,}</p>
                    <p><strong>Remote Jobs:</strong> {report.remote_jobs:,} | <strong>Hybrid:</strong> {report.hybrid_jobs:,} | <strong>Onsite:</strong> {report.onsite_jobs:,}</p>
                </div>

                <h2>Key Intelligence</h2>
                
                <h3>🚀 Job Surges ({len(report.job_surges)})</h3>
                <ul>
                {"".join(f"<li>{c.company_name} ({c.ats_type.title()}): +{c.current_count - c.previous_count:,} jobs ({c.change_percent*100:.1f}%)</li>" for c in report.job_surges)}
                </ul>

                <h3>📉 Job Declines ({len(report.job_declines)})</h3>
                <ul>
                {"".join(f"<li>{c.company_name} ({c.ats_type.title()}): -{c.previous_count - c.current_count:,} jobs ({c.change_percent*100:.1f}%)</li>" for c in report.job_declines)}
                </ul>
                
                <h3>🗺️ Location Expansions ({len(report.location_expansions)})</h3>
                <ul>
                {"".join(f"<li>{e.company_name} expanded to <strong>{e.new_location}</strong> ({e.job_count_at_detection:,} jobs at detection)</li>" for e in report.location_expansions)}
                </ul>
                
                <h3>⭐ Top Growing Companies (Jobs M/M)</h3>
                <ul>
                {"".join(f"<li>{c['company_name']} ({c['ats_type'].title()}): +{c['job_change']:,} jobs</li>" for c in report.top_growing_companies)}
                </ul>
                
            </div>
        </body>
        </html>
        """
        return html


# ==================== MAIN ENTRY POINT (DAILY MAINTENANCE) ====================

def run_daily_maintenance(db: Database = None) -> Dict[str, int]:
    """
    Run all daily/weekly maintenance tasks:
    1. Archive job history (for diffing)
    2. Detect Location Expansions
    3. Detect Job Count Changes
    4. Purge old data
    """
    logger.info("Starting daily market intelligence maintenance...")
    intel = MarketIntelligence(db)
    
    # 1. Archive current state for future comparison
    intel.archive_job_history()
    
    # 2. Detect and record expansions/changes
    expansions = intel.detect_location_expansions()
    changes = intel.detect_job_count_changes()
    
    surges = [c for c in changes if c.change_type == 'surge']
    declines = [c for c in changes if c.change_type == 'decline']
    
    # 3. Create monthly snapshot (run only once per day, typically at a low-traffic time)
    # Check if the current hour is early morning (e.g., before 6 AM UTC)
    if datetime.utcnow().hour < 6:
        intel.db.create_monthly_snapshot()
    
    # 4. Purge old data
    intel.purge_old_job_details(days_to_keep=30)
    intel.purge_stale_companies(days_stale=90)
    
    logger.info(f"✅ Maintenance complete: {len(expansions)} expansions, {len(surges)} surges, {len(declines)} declines")
    
    return {
        'expansions': len(expansions),
        'surges': len(surges),
        'declines': len(declines)
    }


def send_weekly_report(db: Database = None, recipient: str = None):
    """Generate and send weekly intelligence report."""
    recipient = recipient or os.getenv('EMAIL_RECIPIENT')
    if not recipient:
        logger.warning("No email recipient configured")
        return False
    
    intel = MarketIntelligence(db)
    report = intel.generate_report(days=7)
    return intel.send_email_report(report, recipient)


if __name__ == "__main__":
    # Test the intelligence module
    intel = MarketIntelligence()
    
    print("Running market intelligence analysis...")
    
    # Run maintenance
    results = run_daily_maintenance()
    print(f"Maintenance results: {results}")
    
    # Generate report
    report = intel.generate_report(days=7)
    print(f"\n📊 Report Summary:")
    print(f"  Companies: {report.total_companies}")
    print(f"  Jobs: {report.total_jobs}")
    print(f"  New Companies: {report.new_companies}")
    print(f"  Surges: {len(report.job_surges)}")
    print(f"  Expansions: {len(report.location_expansions)}")
