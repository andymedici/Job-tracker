"""One-time database initialization script"""

import logging
from database import get_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_database():
    """Run one-time database initialization"""
    logger.info("=" * 80)
    logger.info("🔧 DATABASE INITIALIZATION")
    logger.info("=" * 80)
    
    db = get_db()
    
    # Add performance indexes
    logger.info("Adding performance indexes...")
    db.add_performance_indexes()
    logger.info("✅ Performance indexes added")
    
    # Cleanup old snapshots
    logger.info("Cleaning up snapshots older than 90 days...")
    deleted = db.cleanup_old_snapshots(90)
    logger.info(f"✅ Deleted {deleted} old snapshots")
    
    logger.info("=" * 80)
    logger.info("✅ DATABASE INITIALIZATION COMPLETE")
    logger.info("=" * 80)

if __name__ == '__main__':
    initialize_database()
