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
    total_companies_tracked: int
    total_open_jobs: int
    time_to_fill_metrics: Dict[str, Any] = field(default_factory=dict) # NEW (E)
    top_skills: Dict[str, int] = field(default_factory=dict) # NEW (D)
    top_hiring_regions: Dict[str, int] = field(default_factory=dict) # NEW (F)
    expansions: List[LocationExpansion] = field(default_factory=list)
    surges: List[JobCountChange] = field(default_factory=list)
    declines: List[JobCountChange] = field(default_factory=list)


class MarketIntelligence:
    """Advanced analytics and reporting for the job market."""

    def __init__(self, db: Optional[Database] = None):
        self.db = db or get_db()

    # --- NEW INTELLIGENCE METRICS (E, D, F) ---

    def get_time_to_fill_metrics(self) -> Dict[str, Any]:
        """Calculates and returns time-to-fill metrics from the database (E)."""
        try:
            return self.db.get_time_to_fill_metrics()
        except Exception as e:
            logger.error(f"Error fetching time-to-fill metrics: {e}")
            return {}
            
    def get_top_skills(self, limit: int = 10) -> Dict[str, int]:
        """
        Calculates the top N demanded skills across all active companies (D).
        """
        with self.db.get_cursor() as cursor:
            # PostgreSQL command to unnest and sum JSONB data
            cursor.execute("""
                SELECT 
                    key, 
                    SUM(value::int) AS total_count
                FROM companies, jsonb_each_text(extracted_skills)
                GROUP BY key
                ORDER BY total_count DESC
                LIMIT %s;
            """, (limit,))
            
            return {row['key']: row['total_count'] for row in cursor.fetchall()}

    def get_top_hiring_regions(self, limit: int = 5) -> Dict[str, int]:
        """
        Calculates the top N hiring regions (countries) across all active companies (F).
        """
        with self.db.get_cursor() as cursor:
            # PostgreSQL command to unnest and sum JSONB data (targeting 'country')
            cursor.execute("""
                SELECT 
                    key, 
                    SUM(value::int) AS total_count
                FROM companies, jsonb_each_text(normalized_locations -> 'country')
                GROUP BY key
                ORDER BY total_count DESC
                LIMIT %s;
            """, (limit,))
            
            return {row['key']: row['total_count'] for row in cursor.fetchall()}

    # --- EXISTING/MODIFIED METHODS ---

    def check_for_location_expansion(self) -> List[LocationExpansion]:
        """Checks for new locations appearing in the past 6 hours."""
        # This function requires snapshot comparison logic which is complex
        # and has been kept as a placeholder.
        return []

    def check_for_job_count_change(self, min_percent: float = 0.10) -> List[JobCountChange]:
        """Compares current job count to the 6-hour snapshot for surges/declines."""
        # This function requires snapshot comparison logic which is complex
        # and has been kept as a placeholder.
        return []

    def purge_old_archives(self, days_to_keep: int):
        """Removes closed job archives older than X days (B)."""
        with self.db.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("""
                DELETE FROM job_archive 
                WHERE status = 'closed' 
                AND last_seen < NOW() - INTERVAL '%s days'
            """, (days_to_keep,))
            logger.info(f"Purged {cursor.rowcount} old closed job archives.")
        
    def purge_old_job_details(self, days_to_keep: int):
        """Removes job details from the main tables that might not be archived."""
        # Keeping this as a cleanup placeholder
        pass 

    def purge_stale_companies(self, days_stale: int):
        """Removes companies not updated in X days."""
        # Keeping this as a cleanup placeholder
        pass 

    def generate_report(self, days: int = 7) -> MarketIntelReport:
        """Generates a comprehensive market intelligence report."""
        now = datetime.utcnow()
        period_start = now - timedelta(days=days)
        
        stats = self.db.get_stats()
        
        report = MarketIntelReport(
            generated_at=now,
            period_start=period_start,
            period_end=now,
            total_companies_tracked=stats.get('total_companies', 0),
            total_open_jobs=stats.get('total_jobs', 0),
            
            # E: Time-to-Fill Metrics
            time_to_fill_metrics=self.get_time_to_fill_metrics(), 
            
            # D: Top Skills
            top_skills=self.get_top_skills(limit=10),
            
            # F: Top Regions
            top_hiring_regions=self.get_top_hiring_regions(limit=5),
            
            # Placeholder for actual analysis logic
            expansions=self.check_for_location_expansion(),
            surges=self.check_for_job_count_change(min_percent=0.10),
            declines=self.check_for_job_count_change(min_percent=0.10),
        )
        return report

    def send_email_report(self, report: MarketIntelReport, recipient: str) -> bool:
        """Sends the market intelligence report via email."""
        # Placeholder for email logic
        return True 


def run_daily_maintenance() -> Dict[str, Any]:
    """
    Main background task to run all market intelligence and cleanup operations.
    Runs every 6 hours by the scheduler.
    """
    db = get_db()
    intel = MarketIntelligence(db)
    now = datetime.utcnow()
    
    logger.info("Starting daily market intelligence maintenance...")

    # 1. Check for expansions/surges/declines
    expansions = intel.check_for_location_expansion()
    surges = intel.check_for_job_count_change(min_percent=0.10)
    declines = intel.check_for_job_count_change(min_percent=0.10)
    
    # 2. Update 6-hourly snapshot
    db.create_6h_snapshots()
    
    # 3. Create monthly snapshot (only once a day, early in the morning)
    if now.hour < 6:
        db.create_monthly_snapshot()
    
    # 4. Generate report
    report = intel.generate_report(days=7)
    
    # 5. Log Key Intelligence Metrics (NEW)
    ttf = report.time_to_fill_metrics.get('overall_avg_ttf_days')
    top_skill = next(iter(report.top_skills), 'N/A')
    top_region = next(iter(report.top_hiring_regions), 'N/A')
    
    logger.info(f"📊 Market Intelligence: TTF: {ttf:.1f} days | Top Skill: {top_skill} | Top Region: {top_region}")

    # 6. Purge old data (B)
    intel.purge_old_job_details(days_to_keep=30)
    intel.purge_stale_companies(days_stale=90)
    intel.purge_old_archives(days_to_keep=90)
    
    logger.info(f"✅ Maintenance complete: {len(expansions)} expansions, {len(surges)} surges, {len(declines)} declines")
    
    return {
        'expansions': len(expansions),
        'surges': len(surges),
        'declines': len(declines),
        'time_to_fill': ttf,
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
    print(f"  Companies: {report.total_companies_tracked}")
    print(f"  Open Jobs: {report.total_open_jobs}")
    print(f"  Time-to-Fill (Overall): {report.time_to_fill_metrics.get('overall_avg_ttf_days'):.1f} days")
    print(f"  Top Skills: {report.top_skills}")
    print(f"  Top Regions: {report.top_hiring_regions}")
    print(f"  Surges: {len(report.surges)}")
    print(f"  Declines: {len(report.declines)}")
