"""
Integration Module - Connect Upgrades to Existing Application
==============================================================
Provides:
1. New API endpoints for upgraded functionality
2. Scheduled task registration
3. Database schema additions
4. Stats and monitoring endpoints
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from functools import wraps
from typing import Dict, List, Optional

from flask import Blueprint, jsonify, request, current_app
from flask_limiter import Limiter

# Import upgrade modules
from collector_v7 import JobIntelCollectorV7, DiscoveryStats
from mega_seed_expander import SeedExpander, SeedCompany
from self_growth_intelligence import SelfGrowthEngine, GrowthStats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# FLASK BLUEPRINT
# =============================================================================

upgrade_bp = Blueprint('upgrade', __name__, url_prefix='/api')


def async_route(f):
    """Decorator to run async functions in Flask routes"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(f(*args, **kwargs))
        finally:
            loop.close()
    return wrapper


# =============================================================================
# SEED EXPANSION ENDPOINTS
# =============================================================================

@upgrade_bp.route('/seeds/expand-mega', methods=['POST'])
@async_route
async def expand_seeds_mega():
    """Run full mega seed expansion (20+ sources)"""
    try:
        db_path = current_app.config.get('DATABASE_PATH', 'job_intel.db')
        expander = SeedExpander(db_path=db_path)
        
        # Get tier filter from request
        tiers = request.json.get('tiers', [1, 2, 3]) if request.json else [1, 2, 3]
        
        logger.info(f"Starting mega expansion for tiers: {tiers}")
        results = await expander.expand_all(tiers=tiers)
        
        # Save to database
        saved = expander.save_to_database(results)
        
        # Build response
        summary = {source: len(seeds) for source, seeds in results.items()}
        total = sum(summary.values())
        
        return jsonify({
            'success': True,
            'total_found': total,
            'saved_to_db': saved,
            'by_source': summary,
            'tiers_processed': tiers,
        })
        
    except Exception as e:
        logger.error(f"Mega expansion error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@upgrade_bp.route('/seeds/expand-tier1', methods=['POST'])
@async_route
async def expand_seeds_tier1():
    """Run premium-only seed expansion (Tier 1 sources)"""
    try:
        db_path = current_app.config.get('DATABASE_PATH', 'job_intel.db')
        expander = SeedExpander(db_path=db_path)
        
        logger.info("Starting Tier 1 expansion...")
        results = await expander.expand_all(tiers=[1])
        
        saved = expander.save_to_database(results)
        
        summary = {source: len(seeds) for source, seeds in results.items()}
        
        return jsonify({
            'success': True,
            'total_found': sum(summary.values()),
            'saved_to_db': saved,
            'by_source': summary,
        })
        
    except Exception as e:
        logger.error(f"Tier 1 expansion error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@upgrade_bp.route('/seeds/stats', methods=['GET'])
def get_seed_stats():
    """Get seed company statistics"""
    try:
        import sqlite3
        db_path = current_app.config.get('DATABASE_PATH', 'job_intel.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Total seeds
        cursor.execute('SELECT COUNT(*) FROM seed_companies')
        total = cursor.fetchone()[0]
        
        # By tier
        cursor.execute('SELECT tier, COUNT(*) FROM seed_companies GROUP BY tier')
        by_tier = {f"tier_{row[0]}": row[1] for row in cursor.fetchall()}
        
        # By source
        cursor.execute('SELECT source, COUNT(*) FROM seed_companies GROUP BY source ORDER BY COUNT(*) DESC LIMIT 20')
        by_source = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Tested vs untested
        cursor.execute('SELECT found, COUNT(*) FROM seed_companies GROUP BY found')
        by_status = {}
        for row in cursor.fetchall():
            status = 'found' if row[0] else 'not_found'
            by_status[status] = row[1]
        
        cursor.execute('SELECT COUNT(*) FROM seed_companies WHERE tested_at IS NULL')
        by_status['untested'] = cursor.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'total_seeds': total,
            'by_tier': by_tier,
            'by_source': by_source,
            'by_status': by_status,
        })
        
    except Exception as e:
        logger.error(f"Seed stats error: {e}")
        return jsonify({'error': str(e)}), 500


# =============================================================================
# COLLECTOR V7 ENDPOINTS
# =============================================================================

@upgrade_bp.route('/collect/v7', methods=['POST'])
@async_route
async def run_collector_v7():
    """Run V7 collector with 15 ATS types and parallel testing"""
    try:
        db_path = current_app.config.get('DATABASE_PATH', 'job_intel.db')
        collector = JobIntelCollectorV7(db_path=db_path)
        collector.init_database()
        
        # Get options from request
        options = request.json or {}
        batch_size = options.get('batch_size', 10)
        limit = options.get('limit', 1000)  # Max seeds to process
        tier = options.get('tier')  # Optional tier filter
        
        # Load seeds from database
        import sqlite3
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        query = '''
            SELECT name FROM seed_companies 
            WHERE tested_at IS NULL OR tested_at < datetime('now', '-7 days')
        '''
        params = []
        
        if tier:
            query += ' AND tier = ?'
            params.append(tier)
        
        query += ' ORDER BY tier ASC, confidence DESC LIMIT ?'
        params.append(limit)
        
        cursor.execute(query, params)
        seeds = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        if not seeds:
            return jsonify({
                'success': True,
                'message': 'No untested seeds found',
                'stats': {}
            })
        
        logger.info(f"Running V7 collector on {len(seeds)} seeds...")
        stats = await collector.discover_from_seeds(seeds, batch_size=batch_size)
        
        # Update tested_at for processed seeds
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.executemany(
            'UPDATE seed_companies SET tested_at = ? WHERE name = ?',
            [(datetime.now().isoformat(), seed) for seed in seeds]
        )
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'stats': {
                'seeds_tested': stats.seeds_tested,
                'companies_found': stats.companies_found,
                'jobs_found': stats.jobs_found,
                'errors': stats.errors,
                'duration_seconds': stats.duration_seconds,
                'ats_breakdown': stats.ats_breakdown,
                'self_discoveries': stats.new_discoveries,
            }
        })
        
    except Exception as e:
        logger.error(f"V7 collector error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@upgrade_bp.route('/collect/v7/test', methods=['POST'])
@async_route
async def test_companies_v7():
    """Test specific companies with V7 collector"""
    try:
        if not request.json or 'companies' not in request.json:
            return jsonify({'error': 'Missing companies array'}), 400
        
        companies = request.json['companies']
        if not isinstance(companies, list) or len(companies) > 50:
            return jsonify({'error': 'Provide 1-50 company names'}), 400
        
        db_path = current_app.config.get('DATABASE_PATH', 'job_intel.db')
        collector = JobIntelCollectorV7(db_path=db_path)
        collector.init_database()
        
        stats = await collector.discover_from_seeds(companies, batch_size=5)
        
        return jsonify({
            'success': True,
            'stats': {
                'companies_found': stats.companies_found,
                'jobs_found': stats.jobs_found,
                'ats_breakdown': stats.ats_breakdown,
            }
        })
        
    except Exception as e:
        logger.error(f"V7 test error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


# =============================================================================
# SELF-GROWTH ENDPOINTS
# =============================================================================

@upgrade_bp.route('/self-growth/run', methods=['POST'])
@async_route
async def run_self_growth():
    """Run self-growth intelligence analysis"""
    try:
        db_path = current_app.config.get('DATABASE_PATH', 'job_intel.db')
        engine = SelfGrowthEngine(db_path=db_path)
        
        logger.info("Starting self-growth analysis...")
        stats = await engine.run_analysis()
        
        return jsonify({
            'success': True,
            'stats': {
                'companies_analyzed': stats.companies_analyzed,
                'discoveries_from_jobs': stats.discoveries_from_jobs,
                'discoveries_from_websites': stats.discoveries_from_websites,
                'discoveries_from_news': stats.discoveries_from_news,
                'total_discoveries': stats.total_discoveries,
                'high_confidence': stats.high_confidence,
                'promoted_to_seeds': stats.promoted_to_seeds,
                'duration_seconds': stats.duration_seconds,
            },
            'by_type': engine.get_discovery_summary(),
        })
        
    except Exception as e:
        logger.error(f"Self-growth error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@upgrade_bp.route('/self-growth/discoveries', methods=['GET'])
def get_discoveries():
    """Get self-growth discoveries"""
    try:
        import sqlite3
        db_path = current_app.config.get('DATABASE_PATH', 'job_intel.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get query params
        min_confidence = float(request.args.get('min_confidence', 0.5))
        limit = int(request.args.get('limit', 100))
        discovery_type = request.args.get('type')
        
        query = '''
            SELECT name, source_company, discovery_type, confidence, context, 
                   promoted_to_seed, discovered_at
            FROM self_growth_discoveries
            WHERE confidence >= ?
        '''
        params = [min_confidence]
        
        if discovery_type:
            query += ' AND discovery_type = ?'
            params.append(discovery_type)
        
        query += ' ORDER BY confidence DESC, discovered_at DESC LIMIT ?'
        params.append(limit)
        
        cursor.execute(query, params)
        
        discoveries = []
        for row in cursor.fetchall():
            discoveries.append({
                'name': row[0],
                'source_company': row[1],
                'discovery_type': row[2],
                'confidence': row[3],
                'context': row[4],
                'promoted': bool(row[5]),
                'discovered_at': row[6],
            })
        
        conn.close()
        
        return jsonify({
            'count': len(discoveries),
            'discoveries': discoveries,
        })
        
    except Exception as e:
        logger.error(f"Get discoveries error: {e}")
        return jsonify({'error': str(e)}), 500


# =============================================================================
# ENHANCED STATS ENDPOINT
# =============================================================================

@upgrade_bp.route('/stats/enhanced', methods=['GET'])
def get_enhanced_stats():
    """Get enhanced statistics with ATS breakdown"""
    try:
        import sqlite3
        db_path = current_app.config.get('DATABASE_PATH', 'job_intel.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        stats = {}
        
        # Total tracked companies
        cursor.execute('SELECT COUNT(*) FROM tracked_companies')
        stats['total_companies'] = cursor.fetchone()[0]
        
        # Total jobs
        cursor.execute('SELECT SUM(job_count) FROM tracked_companies')
        result = cursor.fetchone()[0]
        stats['total_jobs'] = result or 0
        
        # Remote jobs
        cursor.execute('SELECT SUM(remote_count) FROM tracked_companies')
        result = cursor.fetchone()[0]
        stats['remote_jobs'] = result or 0
        
        # By ATS type
        cursor.execute('''
            SELECT ats_type, COUNT(*), SUM(job_count)
            FROM tracked_companies
            GROUP BY ats_type
            ORDER BY COUNT(*) DESC
        ''')
        stats['by_ats'] = {
            row[0]: {'companies': row[1], 'jobs': row[2] or 0}
            for row in cursor.fetchall()
        }
        
        # Total seeds
        cursor.execute('SELECT COUNT(*) FROM seed_companies')
        stats['total_seeds'] = cursor.fetchone()[0]
        
        # Conversion rate
        cursor.execute('SELECT COUNT(*) FROM seed_companies WHERE found = TRUE')
        found = cursor.fetchone()[0]
        if stats['total_seeds'] > 0:
            stats['conversion_rate'] = round(found / stats['total_seeds'] * 100, 2)
        else:
            stats['conversion_rate'] = 0
        
        # Self-growth discoveries
        cursor.execute('SELECT COUNT(*) FROM self_growth_discoveries')
        stats['total_discoveries'] = cursor.fetchone()[0] if cursor.fetchone() else 0
        
        cursor.execute('SELECT COUNT(*) FROM self_growth_discoveries WHERE promoted_to_seed = TRUE')
        stats['promoted_discoveries'] = cursor.fetchone()[0] if cursor.fetchone() else 0
        
        # Recent activity
        cursor.execute('''
            SELECT COUNT(*) FROM tracked_companies 
            WHERE last_updated > datetime('now', '-24 hours')
        ''')
        stats['updated_last_24h'] = cursor.fetchone()[0]
        
        conn.close()
        
        return jsonify(stats)
        
    except Exception as e:
        logger.error(f"Enhanced stats error: {e}")
        return jsonify({'error': str(e)}), 500


# =============================================================================
# SCHEDULED TASKS
# =============================================================================

def register_scheduled_tasks(scheduler, db_path: str):
    """Register scheduled tasks with APScheduler"""
    
    @scheduler.task('cron', id='self_growth_daily', hour=4, minute=0)
    def daily_self_growth():
        """Run self-growth analysis daily at 4:00 AM UTC"""
        logger.info("Running scheduled self-growth analysis...")
        
        async def run():
            engine = SelfGrowthEngine(db_path=db_path)
            stats = await engine.run_analysis()
            logger.info(f"Self-growth complete: {stats.total_discoveries} discoveries, {stats.promoted_to_seeds} promoted")
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run())
        finally:
            loop.close()
    
    @scheduler.task('cron', id='mega_expansion_weekly', day_of_week='sun', hour=5, minute=0)
    def weekly_mega_expansion():
        """Run mega seed expansion weekly on Sunday at 5:00 AM UTC"""
        logger.info("Running scheduled mega expansion...")
        
        async def run():
            expander = SeedExpander(db_path=db_path)
            results = await expander.expand_all(tiers=[1, 2])
            saved = expander.save_to_database(results)
            logger.info(f"Mega expansion complete: {saved} new seeds")
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run())
        finally:
            loop.close()
    
    @scheduler.task('interval', id='v7_collection', hours=6)
    def periodic_collection():
        """Run V7 collector every 6 hours"""
        logger.info("Running scheduled V7 collection...")
        
        async def run():
            collector = JobIntelCollectorV7(db_path=db_path)
            collector.init_database()
            
            # Load untested seeds
            import sqlite3
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute('''
                SELECT name FROM seed_companies 
                WHERE tested_at IS NULL OR tested_at < datetime('now', '-7 days')
                ORDER BY tier ASC, confidence DESC 
                LIMIT 500
            ''')
            seeds = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            if seeds:
                stats = await collector.discover_from_seeds(seeds, batch_size=10)
                logger.info(f"Collection complete: {stats.companies_found} companies, {stats.jobs_found} jobs")
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run())
        finally:
            loop.close()


# =============================================================================
# DATABASE SCHEMA ADDITIONS
# =============================================================================

def apply_schema_additions(db_path: str):
    """Apply additional database schema for upgrades"""
    import sqlite3
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Add columns to seed_companies if not exist
    try:
        cursor.execute('ALTER TABLE seed_companies ADD COLUMN discovery_source VARCHAR(100)')
    except sqlite3.OperationalError:
        pass
    
    try:
        cursor.execute('ALTER TABLE seed_companies ADD COLUMN discovery_confidence DECIMAL(3,2)')
    except sqlite3.OperationalError:
        pass
    
    try:
        cursor.execute('ALTER TABLE seed_companies ADD COLUMN discovered_from VARCHAR(255)')
    except sqlite3.OperationalError:
        pass
    
    # Create ATS predictions table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ats_predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name VARCHAR(255),
            predicted_ats VARCHAR(50),
            actual_ats VARCHAR(50),
            confidence DECIMAL(3,2),
            correct BOOLEAN,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create self-growth discoveries table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS self_growth_discoveries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(255),
            source_company VARCHAR(255),
            discovery_type VARCHAR(50),
            confidence DECIMAL(3,2),
            context TEXT,
            url TEXT,
            promoted_to_seed BOOLEAN DEFAULT FALSE,
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create discovery runs log
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS discovery_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_type VARCHAR(50),
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            seeds_tested INTEGER DEFAULT 0,
            companies_found INTEGER DEFAULT 0,
            jobs_found INTEGER DEFAULT 0,
            errors INTEGER DEFAULT 0,
            ats_breakdown TEXT,
            notes TEXT
        )
    ''')
    
    # Create indexes
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_seeds_tier ON seed_companies(tier)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_seeds_source ON seed_companies(source)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_seeds_tested ON seed_companies(tested_at)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_companies_ats ON tracked_companies(ats_type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_discoveries_type ON self_growth_discoveries(discovery_type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_discoveries_confidence ON self_growth_discoveries(confidence)')
    
    conn.commit()
    conn.close()
    
    logger.info("Database schema additions applied")


# =============================================================================
# REGISTRATION FUNCTIONS
# =============================================================================

def register_upgrade_routes(app, limiter: Optional[Limiter] = None):
    """Register upgrade routes with Flask app"""
    app.register_blueprint(upgrade_bp)
    
    # Apply rate limits if limiter provided
    if limiter:
        limiter.limit("10 per minute")(upgrade_bp)
    
    logger.info("Upgrade routes registered")


def register_upgrade_scheduled_tasks(scheduler, db_path: str):
    """Register scheduled tasks"""
    register_scheduled_tasks(scheduler, db_path)
    logger.info("Upgrade scheduled tasks registered")


# =============================================================================
# STANDALONE TESTING
# =============================================================================

if __name__ == '__main__':
    # Test database schema
    import tempfile
    import os
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        test_db = f.name
    
    try:
        apply_schema_additions(test_db)
        print("Schema additions applied successfully!")
        
        # Verify tables
        import sqlite3
        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"Tables created: {tables}")
        
        conn.close()
        
    finally:
        os.unlink(test_db)
