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
                    
                    # Record the expansion
                    with self.db.get_cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO location_expansions 
                            (company_id, company_name, ats_type, new_location, 
                             previous_locations, job_count_at_detection)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                        """, (
                            company_id, row['company_name'], row['ats_type'],
                            new_loc.title(), json.dumps(list(previous_locations)),
                            row['job_count']
                        ))
                    
                    expansions.append(expansion)
                    logger.info(f"📍 Location expansion: {row['company_name']} → {new_loc.title()}")
                    
        except Exception as e:
            logger.error(f"Error detecting location expansions: {e}")
        
        return expansions
    
    def _is_meaningful_location(self, location: str) -> bool:
        """Check if location is specific enough to track."""
        if not location:
            return False
        
        loc_lower = location.lower().strip()
        
        if loc_lower in self.GENERIC_LOCATIONS:
            return False
        
        if len(loc_lower) < 3:
            return False
        
        # Must contain at least one letter
        if not any(c.isalpha() for c in loc_lower):
            return False
        
        return True
    
    # ==================== JOB COUNT CHANGE DETECTION ====================
    
    def detect_job_count_changes(self) -> Tuple[List[JobCountChange], List[JobCountChange]]:
        """Detect significant job count changes (surges and declines)."""
        surges = []
        declines = []
        
        try:
            with self.db.get_cursor() as cursor:
                # Get companies with their previous and current job counts
                cursor.execute("""
                    SELECT 
                        id, company_name, ats_type, 
                        job_count as current_count,
                        last_job_count as previous_count
                    FROM companies
                    WHERE job_count IS NOT NULL
                """)
                companies = list(cursor.fetchall())
            
            for row in companies:
                current = row['current_count'] or 0
                previous = row['previous_count'] or 0
                
                # Skip if counts are too small
                if current < self.MIN_JOBS_FOR_ALERT and previous < self.MIN_JOBS_FOR_ALERT:
                    continue
                
                # Calculate change
                if previous == 0:
                    if current >= self.MIN_JOBS_FOR_ALERT:
                        # New company with jobs
                        change = JobCountChange(
                            company_id=row['id'],
                            company_name=row['company_name'],
                            ats_type=row['ats_type'],
                            previous_count=0,
                            current_count=current,
                            change_percent=100.0,
                            change_type='new',
                            detected_at=datetime.utcnow()
                        )
                        surges.append(change)
                        self._record_change(change)
                    continue
                
                change_percent = (current - previous) / previous
                
                if change_percent >= self.SURGE_THRESHOLD:
                    change = JobCountChange(
                        company_id=row['id'],
                        company_name=row['company_name'],
                        ats_type=row['ats_type'],
                        previous_count=previous,
                        current_count=current,
                        change_percent=change_percent * 100,
                        change_type='surge',
                        detected_at=datetime.utcnow()
                    )
                    surges.append(change)
                    self._record_change(change)
                    logger.info(f"📈 Job surge: {row['company_name']} +{change_percent*100:.1f}% ({previous}→{current})")
                
                elif change_percent <= self.DECLINE_THRESHOLD:
                    change = JobCountChange(
                        company_id=row['id'],
                        company_name=row['company_name'],
                        ats_type=row['ats_type'],
                        previous_count=previous,
                        current_count=current,
                        change_percent=change_percent * 100,
                        change_type='decline',
                        detected_at=datetime.utcnow()
                    )
                    declines.append(change)
                    self._record_change(change)
                    logger.info(f"📉 Job decline: {row['company_name']} {change_percent*100:.1f}% ({previous}→{current})")
                    
        except Exception as e:
            logger.error(f"Error detecting job count changes: {e}")
        
        return surges, declines
    
    def _record_change(self, change: JobCountChange):
        """Record a job count change to the database."""
        try:
            with self.db.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO job_count_changes 
                    (company_id, company_name, ats_type, previous_count, 
                     current_count, change_percent, change_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    change.company_id, change.company_name, change.ats_type,
                    change.previous_count, change.current_count,
                    change.change_percent, change.change_type
                ))
        except Exception as e:
            logger.error(f"Error recording change: {e}")
    
    # ==================== DATA ARCHIVAL AND PURGING ====================
    
    def archive_daily_snapshot(self):
        """Create daily archive of job data for long-term storage."""
        today = datetime.utcnow().date()
        
        try:
            with self.db.get_cursor() as cursor:
                cursor.execute("""
                    SELECT id, job_count, remote_count, hybrid_count, onsite_count,
                           locations, departments
                    FROM companies
                """)
                companies = list(cursor.fetchall())
            
            archived = 0
            for row in companies:
                try:
                    with self.db.get_cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO job_history_archive
                            (company_id, archive_date, job_count, remote_count, 
                             hybrid_count, onsite_count, locations_json, departments_json)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (company_id, archive_date) DO UPDATE SET
                                job_count = EXCLUDED.job_count,
                                remote_count = EXCLUDED.remote_count,
                                hybrid_count = EXCLUDED.hybrid_count,
                                onsite_count = EXCLUDED.onsite_count,
                                locations_json = EXCLUDED.locations_json,
                                departments_json = EXCLUDED.departments_json
                        """, (
                            row['id'], today, row['job_count'],
                            row['remote_count'], row['hybrid_count'], row['onsite_count'],
                            json.dumps(row['locations']) if row['locations'] else '[]',
                            json.dumps(row['departments']) if row['departments'] else '[]'
                        ))
                        archived += 1
                except:
                    pass
            
            logger.info(f"📦 Archived daily snapshot for {archived} companies")
            
        except Exception as e:
            logger.error(f"Error archiving daily snapshot: {e}")
    
    def create_weekly_aggregate(self):
        """Create weekly aggregated statistics."""
        # Get start of current week (Monday)
        today = datetime.utcnow().date()
        week_start = today - timedelta(days=today.weekday())
        
        try:
            with self.db.get_cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_companies,
                        COALESCE(SUM(job_count), 0) as total_jobs,
                        COALESCE(SUM(remote_count), 0) as remote_jobs,
                        COALESCE(SUM(hybrid_count), 0) as hybrid_jobs,
                        COALESCE(SUM(onsite_count), 0) as onsite_jobs,
                        SUM(CASE WHEN ats_type = 'greenhouse' THEN 1 ELSE 0 END) as greenhouse,
                        SUM(CASE WHEN ats_type = 'lever' THEN 1 ELSE 0 END) as lever
                    FROM companies
                """)
                stats = cursor.fetchone()
                
                # Count new companies this week
                cursor.execute("""
                    SELECT COUNT(*) as count FROM companies
                    WHERE first_seen >= %s
                """, (week_start,))
                new_companies = cursor.fetchone()['count']
                
                cursor.execute("""
                    INSERT INTO weekly_stats
                    (week_start, total_companies, total_jobs, remote_jobs,
                     hybrid_jobs, onsite_jobs, greenhouse_companies, 
                     lever_companies, new_companies_this_week)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (week_start) DO UPDATE SET
                        total_companies = EXCLUDED.total_companies,
                        total_jobs = EXCLUDED.total_jobs,
                        remote_jobs = EXCLUDED.remote_jobs,
                        hybrid_jobs = EXCLUDED.hybrid_jobs,
                        onsite_jobs = EXCLUDED.onsite_jobs,
                        greenhouse_companies = EXCLUDED.greenhouse_companies,
                        lever_companies = EXCLUDED.lever_companies,
                        new_companies_this_week = EXCLUDED.new_companies_this_week
                """, (
                    week_start, stats['total_companies'], stats['total_jobs'],
                    stats['remote_jobs'], stats['hybrid_jobs'], stats['onsite_jobs'],
                    stats['greenhouse'], stats['lever'], new_companies
                ))
            
            logger.info(f"📊 Created weekly aggregate for week of {week_start}")
            
        except Exception as e:
            logger.error(f"Error creating weekly aggregate: {e}")
    
    def purge_old_job_details(self, days_to_keep: int = 30):
        """Purge individual job records older than threshold, keeping aggregates."""
        try:
            cutoff = datetime.utcnow() - timedelta(days=days_to_keep)
            
            with self.db.get_cursor() as cursor:
                # Delete old individual job records
                cursor.execute("""
                    DELETE FROM jobs 
                    WHERE last_seen < %s 
                    AND company_id IN (
                        SELECT id FROM companies WHERE last_seen >= %s
                    )
                """, (cutoff, cutoff))
                deleted_jobs = cursor.rowcount
                
                # Delete old daily archives (keep 90 days)
                archive_cutoff = datetime.utcnow() - timedelta(days=90)
                cursor.execute("""
                    DELETE FROM job_history_archive
                    WHERE archive_date < %s
                """, (archive_cutoff.date(),))
                deleted_archives = cursor.rowcount
                
                # Delete old change notifications (keep 60 days)
                notify_cutoff = datetime.utcnow() - timedelta(days=60)
                cursor.execute("DELETE FROM job_count_changes WHERE detected_at < %s", (notify_cutoff,))
                cursor.execute("DELETE FROM location_expansions WHERE detected_at < %s", (notify_cutoff,))
            
            logger.info(f"🗑️ Purged {deleted_jobs} old job records, {deleted_archives} old archives")
            
        except Exception as e:
            logger.error(f"Error purging old data: {e}")
    
    def purge_stale_companies(self, days_stale: int = 90):
        """Remove companies that haven't been seen in a while (likely closed boards)."""
        try:
            cutoff = datetime.utcnow() - timedelta(days=days_stale)
            
            with self.db.get_cursor() as cursor:
                # First, record them as 'closed' in changes
                cursor.execute("""
                    SELECT id, company_name, ats_type, job_count
                    FROM companies
                    WHERE last_seen < %s
                """, (cutoff,))
                stale_companies = list(cursor.fetchall())
                
                for row in stale_companies:
                    cursor.execute("""
                        INSERT INTO job_count_changes 
                        (company_id, company_name, ats_type, previous_count, 
                         current_count, change_percent, change_type)
                        VALUES (%s, %s, %s, %s, 0, -100, 'closed')
                    """, (row['id'], row['company_name'], row['ats_type'], row['job_count']))
                
                # Then delete
                cursor.execute("DELETE FROM companies WHERE last_seen < %s", (cutoff,))
                deleted = cursor.rowcount
            
            logger.info(f"🗑️ Removed {deleted} stale companies (not seen in {days_stale} days)")
            
        except Exception as e:
            logger.error(f"Error purging stale companies: {e}")
    
    # ==================== REPORT GENERATION ====================
    
    def generate_report(self, days: int = 7) -> MarketIntelReport:
        """Generate comprehensive market intelligence report."""
        now = datetime.utcnow()
        period_start = now - timedelta(days=days)
        
        report = MarketIntelReport(
            generated_at=now,
            period_start=period_start,
            period_end=now
        )
        
        try:
            with self.db.get_cursor() as cursor:
                # Overall stats
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_companies,
                        COALESCE(SUM(job_count), 0) as total_jobs,
                        COALESCE(SUM(remote_count), 0) as remote_jobs,
                        COALESCE(SUM(hybrid_count), 0) as hybrid_jobs,
                        COALESCE(SUM(onsite_count), 0) as onsite_jobs
                    FROM companies
                """)
                stats = cursor.fetchone()
                report.total_companies = stats['total_companies']
                report.total_jobs = int(stats['total_jobs'])
                report.remote_jobs = int(stats['remote_jobs'])
                report.hybrid_jobs = int(stats['hybrid_jobs'])
                report.onsite_jobs = int(stats['onsite_jobs'])
                
                # New companies
                cursor.execute("""
                    SELECT COUNT(*) as count FROM companies
                    WHERE first_seen >= %s
                """, (period_start,))
                report.new_companies = cursor.fetchone()['count']
                
                # Location expansions
                cursor.execute("""
                    SELECT * FROM location_expansions
                    WHERE detected_at >= %s
                    ORDER BY detected_at DESC
                """, (period_start,))
                for row in cursor:
                    report.location_expansions.append(LocationExpansion(
                        company_id=row['company_id'],
                        company_name=row['company_name'],
                        ats_type=row['ats_type'],
                        new_location=row['new_location'],
                        detected_at=row['detected_at'],
                        job_count_at_detection=row['job_count_at_detection']
                    ))
                
                # Job surges
                cursor.execute("""
                    SELECT * FROM job_count_changes
                    WHERE change_type = 'surge' AND detected_at >= %s
                    ORDER BY change_percent DESC
                """, (period_start,))
                for row in cursor:
                    report.job_surges.append(JobCountChange(
                        company_id=row['company_id'],
                        company_name=row['company_name'],
                        ats_type=row['ats_type'],
                        previous_count=row['previous_count'],
                        current_count=row['current_count'],
                        change_percent=row['change_percent'],
                        change_type=row['change_type'],
                        detected_at=row['detected_at']
                    ))
                
                # Job declines
                cursor.execute("""
                    SELECT * FROM job_count_changes
                    WHERE change_type = 'decline' AND detected_at >= %s
                    ORDER BY change_percent ASC
                """, (period_start,))
                for row in cursor:
                    report.job_declines.append(JobCountChange(
                        company_id=row['company_id'],
                        company_name=row['company_name'],
                        ats_type=row['ats_type'],
                        previous_count=row['previous_count'],
                        current_count=row['current_count'],
                        change_percent=row['change_percent'],
                        change_type=row['change_type'],
                        detected_at=row['detected_at']
                    ))
                
                # New entrants
                cursor.execute("""
                    SELECT company_name, ats_type, job_count, first_seen
                    FROM companies
                    WHERE first_seen >= %s
                    ORDER BY job_count DESC
                    LIMIT 20
                """, (period_start,))
                report.new_entrants = [dict(row) for row in cursor]
                
                # Month-over-month change
                cursor.execute("""
                    SELECT total_jobs FROM weekly_stats
                    ORDER BY week_start DESC LIMIT 2
                """)
                weeks = cursor.fetchall()
                if len(weeks) >= 2:
                    current = weeks[0]['total_jobs'] or 0
                    previous = weeks[1]['total_jobs'] or 1
                    report.month_over_month_change = ((current - previous) / previous) * 100
                
                # Top growing companies
                cursor.execute("""
                    SELECT company_name, ats_type, previous_count, current_count, change_percent
                    FROM job_count_changes
                    WHERE change_type = 'surge' AND detected_at >= %s
                    ORDER BY (current_count - previous_count) DESC
                    LIMIT 10
                """, (period_start,))
                report.top_growing_companies = [dict(row) for row in cursor]
                
                # Top shrinking companies
                cursor.execute("""
                    SELECT company_name, ats_type, previous_count, current_count, change_percent
                    FROM job_count_changes
                    WHERE change_type = 'decline' AND detected_at >= %s
                    ORDER BY (previous_count - current_count) DESC
                    LIMIT 10
                """, (period_start,))
                report.top_shrinking_companies = [dict(row) for row in cursor]
                
        except Exception as e:
            logger.error(f"Error generating report: {e}")
        
        return report
    
    # ==================== EMAIL REPORTING ====================
    
    def send_email_report(self, report: MarketIntelReport, recipient: str) -> bool:
        """Send market intelligence report via email."""
        
        # Get email config from environment
        smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        smtp_port = int(os.getenv('SMTP_PORT', '587'))
        smtp_user = os.getenv('SMTP_USER')
        smtp_pass = os.getenv('SMTP_PASS')
        
        if not smtp_user or not smtp_pass:
            logger.warning("Email not configured (SMTP_USER/SMTP_PASS missing)")
            return False
        
        # Build email content
        subject = f"📊 Job Market Intelligence Report - {report.generated_at.strftime('%Y-%m-%d')}"
        
        html_content = self._build_email_html(report)
        text_content = self._build_email_text(report)
        
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = smtp_user
        msg['To'] = recipient
        
        msg.attach(MIMEText(text_content, 'plain'))
        msg.attach(MIMEText(html_content, 'html'))
        
        try:
            context = ssl.create_default_context()
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls(context=context)
                server.login(smtp_user, smtp_pass)
                server.sendmail(smtp_user, recipient, msg.as_string())
            
            logger.info(f"📧 Email report sent to {recipient}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False
    
    def _build_email_html(self, report: MarketIntelReport) -> str:
        """Build HTML email content."""
        remote_pct = (report.remote_jobs / max(report.total_jobs, 1)) * 100
        hybrid_pct = (report.hybrid_jobs / max(report.total_jobs, 1)) * 100
        onsite_pct = (report.onsite_jobs / max(report.total_jobs, 1)) * 100
        
        expansions_html = ""
        for exp in report.location_expansions[:10]:
            expansions_html += f"<li><strong>{exp.company_name}</strong> → {exp.new_location}</li>"
        
        surges_html = ""
        for surge in report.job_surges[:10]:
            surges_html += f"<li><strong>{surge.company_name}</strong>: {surge.previous_count}→{surge.current_count} (+{surge.change_percent:.0f}%)</li>"
        
        declines_html = ""
        for decline in report.job_declines[:10]:
            declines_html += f"<li><strong>{decline.company_name}</strong>: {decline.previous_count}→{decline.current_count} ({decline.change_percent:.0f}%)</li>"
        
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; }}
                .header {{ background: linear-gradient(135deg, #667eea, #764ba2); color: white; padding: 20px; text-align: center; }}
                .section {{ padding: 15px; border-bottom: 1px solid #eee; }}
                .stat {{ display: inline-block; text-align: center; margin: 10px 20px; }}
                .stat-value {{ font-size: 2em; font-weight: bold; color: #667eea; }}
                .stat-label {{ color: #666; font-size: 0.9em; }}
                ul {{ padding-left: 20px; }}
                li {{ margin: 5px 0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>📊 Job Market Intelligence</h1>
                <p>{report.period_start.strftime('%b %d')} - {report.period_end.strftime('%b %d, %Y')}</p>
            </div>
            
            <div class="section">
                <h2>📈 Overview</h2>
                <div class="stat">
                    <div class="stat-value">{report.total_companies:,}</div>
                    <div class="stat-label">Companies</div>
                </div>
                <div class="stat">
                    <div class="stat-value">{report.total_jobs:,}</div>
                    <div class="stat-label">Total Jobs</div>
                </div>
                <div class="stat">
                    <div class="stat-value">{report.new_companies}</div>
                    <div class="stat-label">New Companies</div>
                </div>
            </div>
            
            <div class="section">
                <h2>🏠 Work Type Breakdown</h2>
                <p>🏠 Remote: {report.remote_jobs:,} ({remote_pct:.1f}%)</p>
                <p>🔄 Hybrid: {report.hybrid_jobs:,} ({hybrid_pct:.1f}%)</p>
                <p>🏢 On-site: {report.onsite_jobs:,} ({onsite_pct:.1f}%)</p>
            </div>
            
            <div class="section">
                <h2>📍 Location Expansions ({len(report.location_expansions)})</h2>
                <ul>{expansions_html if expansions_html else '<li>No expansions detected</li>'}</ul>
            </div>
            
            <div class="section">
                <h2>🚀 Hiring Surges ({len(report.job_surges)})</h2>
                <ul>{surges_html if surges_html else '<li>No significant surges</li>'}</ul>
            </div>
            
            <div class="section">
                <h2>📉 Hiring Slowdowns ({len(report.job_declines)})</h2>
                <ul>{declines_html if declines_html else '<li>No significant declines</li>'}</ul>
            </div>
            
            <div class="section" style="text-align: center; color: #666; font-size: 0.9em;">
                <p>Generated by Job Intelligence Collector</p>
            </div>
        </body>
        </html>
        """
    
    def _build_email_text(self, report: MarketIntelReport) -> str:
        """Build plain text email content."""
        lines = [
            "JOB MARKET INTELLIGENCE REPORT",
            f"Period: {report.period_start.strftime('%b %d')} - {report.period_end.strftime('%b %d, %Y')}",
            "",
            "=== OVERVIEW ===",
            f"Total Companies: {report.total_companies:,}",
            f"Total Jobs: {report.total_jobs:,}",
            f"New Companies: {report.new_companies}",
            "",
            "=== WORK TYPE BREAKDOWN ===",
            f"Remote: {report.remote_jobs:,}",
            f"Hybrid: {report.hybrid_jobs:,}",
            f"On-site: {report.onsite_jobs:,}",
            "",
        ]
        
        if report.location_expansions:
            lines.append(f"=== LOCATION EXPANSIONS ({len(report.location_expansions)}) ===")
            for exp in report.location_expansions[:10]:
                lines.append(f"  • {exp.company_name} → {exp.new_location}")
            lines.append("")
        
        if report.job_surges:
            lines.append(f"=== HIRING SURGES ({len(report.job_surges)}) ===")
            for surge in report.job_surges[:10]:
                lines.append(f"  • {surge.company_name}: {surge.previous_count}→{surge.current_count} (+{surge.change_percent:.0f}%)")
            lines.append("")
        
        if report.job_declines:
            lines.append(f"=== HIRING SLOWDOWNS ({len(report.job_declines)}) ===")
            for decline in report.job_declines[:10]:
                lines.append(f"  • {decline.company_name}: {decline.previous_count}→{decline.current_count} ({decline.change_percent:.0f}%)")
        
        return "\n".join(lines)


# ==================== MAINTENANCE TASKS ====================

def run_daily_maintenance(db: Database = None):
    """Run all maintenance tasks (called every 6 hours despite the name)."""
    intel = MarketIntelligence(db)
    
    logger.info("🔧 Running maintenance tasks...")
    
    # Detect changes
    expansions = intel.detect_location_expansions()
    surges, declines = intel.detect_job_count_changes()
    
    # Create granular snapshots (every 6 hours)
    intel.db.create_6h_snapshots()
    intel.db.create_market_snapshot()
    
    # Create daily archive (will update if already exists for today)
    intel.archive_daily_snapshot()
    
    # Create weekly aggregate
    intel.create_weekly_aggregate()
    
    # Create monthly snapshot on first run of the month
    if datetime.utcnow().day == 1 and datetime.utcnow().hour < 6:
        intel.db.create_monthly_snapshot()
    
    # Purge old data
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
    print(f"  Location Expansions: {len(report.location_expansions)}")
    print(f"  Job Surges: {len(report.job_surges)}")
    print(f"  Job Declines: {len(report.job_declines)}")
