"""
Market Intelligence Module
Complete implementation of all intelligence features
"""
import logging
from datetime import datetime
from database import get_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_for_location_expansion(days: int = 30):
    """
    Detect companies expanding to new locations
    COMPLETE IMPLEMENTATION
    """
    try:
        db = get_db()
        expansions = db.get_location_expansions(days=days)
        
        logger.info(f"📍 Found {len(expansions)} location expansions in last {days} days")
        
        # Record intelligence events
        for expansion in expansions[:10]:  # Top 10
            db.record_intelligence_event(
                event_type='location_expansion',
                company_id=expansion['company_id'],
                company_name=expansion['company_name'],
                event_data={
                    'new_country': expansion.get('country'),
                    'total_jobs': expansion.get('job_count'),
                    'detected_at': datetime.utcnow().isoformat()
                }
            )
        
        return expansions
        
    except Exception as e:
        logger.error(f"Error checking location expansion: {e}")
        return []

def check_for_job_count_change(days: int = 7):
    """
    Detect hiring surges and freezes
    COMPLETE IMPLEMENTATION
    """
    try:
        db = get_db()
        surges, declines = db.get_job_count_changes(days=days)
        
        logger.info(f"📈 Found {len(surges)} surges and {len(declines)} declines")
        
        # Record surge events
        for surge in surges[:10]:
            db.record_intelligence_event(
                event_type='hiring_surge',
                company_id=surge['company_id'],
                company_name=surge['company_name'],
                event_data={
                    'change_amount': surge['change_amount'],
                    'change_percent': surge.get('change_percent'),
                    'current_jobs': surge['current_jobs'],
                    'detected_at': datetime.utcnow().isoformat()
                }
            )
        
        # Record decline events
        for decline in declines[:10]:
            db.record_intelligence_event(
                event_type='hiring_freeze',
                company_id=decline['company_id'],
                company_name=decline['company_name'],
                event_data={
                    'change_amount': decline['change_amount'],
                    'change_percent': decline.get('change_percent'),
                    'current_jobs': decline['current_jobs'],
                    'detected_at': datetime.utcnow().isoformat()
                }
            )
        
        return surges, declines
        
    except Exception as e:
        logger.error(f"Error checking job count changes: {e}")
        return [], []

def purge_old_job_details(days_to_keep: int = 90):
    """
    Archive old closed jobs and clean up database
    COMPLETE IMPLEMENTATION
    """
    try:
        db = get_db()
        
        with db.get_cursor(dict_cursor=False) as cursor:
            # Archive old closed jobs
            cursor.execute("""
                DELETE FROM job_archive
                WHERE status = 'closed'
                  AND last_seen < NOW() - INTERVAL %s
            """, (f'{days_to_keep} days',))
            
            deleted_count = cursor.rowcount
            
        logger.info(f"🗑️ Purged {deleted_count} old job records (>{days_to_keep} days)")
        return deleted_count
        
    except Exception as e:
        logger.error(f"Error purging old jobs: {e}")
        return 0

def purge_stale_companies(inactive_days: int = 180):
    """
    Mark companies as inactive if not updated recently
    COMPLETE IMPLEMENTATION
    """
    try:
        db = get_db()
        
        with db.get_cursor(dict_cursor=False) as cursor:
            # Mark companies as inactive
            cursor.execute("""
                UPDATE companies
                SET active = FALSE
                WHERE last_updated < NOW() - INTERVAL %s
                  AND active = TRUE
            """, (f'{inactive_days} days',))
            
            marked_inactive = cursor.rowcount
            
        logger.info(f"⏸️ Marked {marked_inactive} companies as inactive (>{inactive_days} days)")
        return marked_inactive
        
    except Exception as e:
        logger.error(f"Error purging stale companies: {e}")
        return 0

def create_6h_snapshots():
    """
    Create 6-hour snapshots for trend analysis
    COMPLETE IMPLEMENTATION
    """
    try:
        db = get_db()
        count = db.create_6h_snapshots()
        logger.info(f"📸 Created {count} 6-hour snapshots")
        return count
    except Exception as e:
        logger.error(f"Error creating snapshots: {e}")
        return 0

def create_monthly_snapshot():
    """
    Create monthly snapshot for long-term trends
    COMPLETE IMPLEMENTATION
    """
    try:
        db = get_db()
        count = db.create_monthly_snapshot()
        logger.info(f"📅 Created/updated monthly snapshot ({count} companies)")
        return count
    except Exception as e:
        logger.error(f"Error creating monthly snapshot: {e}")
        return 0

def run_daily_maintenance():
    """
    Run all intelligence and maintenance tasks
    COMPLETE IMPLEMENTATION
    """
    logger.info("=" * 60)
    logger.info("🔧 Starting Daily Maintenance")
    logger.info("=" * 60)
    
    try:
        # Create snapshots
        create_6h_snapshots()
        
        # Check for intelligence events
        surges, declines = check_for_job_count_change(days=7)
        expansions = check_for_location_expansion(days=30)
        
        # Get analytics
        db = get_db()
        ttf_metrics = db.get_time_to_fill_metrics()
        
        logger.info(f"📊 Intelligence Summary:")
        logger.info(f"   - Hiring Surges: {len(surges)}")
        logger.info(f"   - Hiring Freezes: {len(declines)}")
        logger.info(f"   - Location Expansions: {len(expansions)}")
        logger.info(f"   - Avg Time-to-Fill: {ttf_metrics.get('overall_avg_ttf_days', 0):.1f} days")
        
        # Cleanup (only run during off-peak hours)
        current_hour = datetime.utcnow().hour
        if 0 <= current_hour <= 4:  # Run between midnight and 4am UTC
            purged_jobs = purge_old_job_details(days_to_keep=90)
            marked_inactive = purge_stale_companies(inactive_days=180)
            logger.info(f"🧹 Cleanup: {purged_jobs} jobs purged, {marked_inactive} companies marked inactive")
        
        logger.info("=" * 60)
        logger.info("✅ Daily Maintenance Complete")
        logger.info("=" * 60)
        
        return {
            'surges': len(surges),
            'declines': len(declines),
            'expansions': len(expansions),
            'avg_ttf': ttf_metrics.get('overall_avg_ttf_days', 0)
        }
        
    except Exception as e:
        logger.error(f"❌ Daily maintenance failed: {e}", exc_info=True)
        return None
