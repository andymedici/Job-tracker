"""Job Intelligence Platform - Main Application"""

import os
import logging
import asyncio
import threading
from datetime import datetime, timezone, timedelta
from flask import Flask, request, jsonify, render_template
from waitress import serve
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.jobstores.base import JobLookupError

from database import get_db, TRENDS_CUTOFF_DATE
from collector import run_collection, run_refresh
from market_intel import run_daily_maintenance
from middleware.auth import AuthManager, require_api_key, require_admin_key, optional_auth
from middleware.rate_limit import setup_rate_limiter

# =============================================================================
# UPGRADE MODULE IMPORTS (V7 Collector, Mega Expander, Self-Growth)
# =============================================================================
try:
    from config import config, ATS_CONFIGS, COMPANY_TOKEN_MAPPINGS
    UPGRADE_CONFIG_LOADED = True
except ImportError:
    UPGRADE_CONFIG_LOADED = False
    logging.warning("⚠️ Upgrade config not found - using defaults")

try:
    import collector_v7
    COLLECTOR_V7_AVAILABLE = True
except ImportError:
    COLLECTOR_V7_AVAILABLE = False
    logging.warning("⚠️ collector_v7.py not found - V7 features disabled")

try:
    import mega_seed_expander
    MEGA_EXPANDER_AVAILABLE = True
except ImportError:
    MEGA_EXPANDER_AVAILABLE = False
    logging.warning("⚠️ mega_seed_expander.py not found - mega expansion disabled")

try:
    import self_growth_intelligence
    SELF_GROWTH_AVAILABLE = True
except ImportError:
    SELF_GROWTH_AVAILABLE = False
    logging.warning("⚠️ self_growth_intelligence.py not found - self-growth disabled")

# NOTE: integration.py not used - upgrade endpoints are built directly into app.py
INTEGRATION_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

auth_manager = AuthManager()
limiter = setup_rate_limiter(app)

RATE_LIMITS = {
    'default': '1000 per day',
    'authenticated_read': '100 per hour',
    'write': '50 per hour',
    'admin': '200 per hour'
}

collection_state = {
    'is_running': False,
    'mode': None,
    'current_progress': 0,
    'last_stats': None,
    'last_run': None
}

# V7 Collection State (separate from legacy collector)
v7_collection_state = {
    'is_running': False,
    'started_at': None,
    'last_run': None,
    'last_stats': None
}

def template_check():
    import os
    logger.info("=" * 80)
    logger.info("🔍 TEMPLATE CHECK ON STARTUP")
    logger.info("=" * 80)
    logger.info(f"Working dir: {os.getcwd()}")
    logger.info(f"Root path: {app.root_path}")
    logger.info(f"Template folder: {app.template_folder}")
    template_dir = os.path.join(app.root_path, 'templates')
    if os.path.exists(template_dir):
        files = os.listdir(template_dir)
        logger.info(f"✅ Templates found: {files}")
    else:
        logger.error(f"❌ Template directory not found: {template_dir}")
    logger.info("=" * 80)

template_check()

scheduler = BackgroundScheduler()

def scheduled_refresh():
    if not get_db().acquire_advisory_lock('scheduled_refresh'):
        logger.info("Refresh already running on another instance")
        return
    try:
        logger.info("Starting scheduled refresh")
        stats = asyncio.run(run_refresh(hours_since_update=24, max_companies=500))
        run_daily_maintenance()
        collection_state['last_stats'] = {
            'total_tested': 0,
            'total_discovered': 0,
            'total_jobs_collected': stats.total_jobs_collected,
            'total_new_jobs': stats.total_new_jobs,
            'total_updated_jobs': stats.total_updated_jobs,
            'total_closed_jobs': stats.total_closed_jobs
        }
        collection_state['last_run'] = datetime.now(timezone.utc).isoformat()
        logger.info(f"Scheduled refresh complete: {stats.total_jobs_collected} jobs")
    finally:
        get_db().release_advisory_lock('scheduled_refresh')

def scheduled_discovery():
    if not get_db().acquire_advisory_lock('scheduled_discovery'):
        logger.info("Discovery already running on another instance")
        return
    try:
        logger.info("Starting scheduled discovery")
        stats = asyncio.run(run_collection(max_companies=2000))
        run_daily_maintenance()
        collection_state['last_stats'] = {
            'total_tested': stats.total_tested,
            'total_discovered': stats.total_discovered,
            'total_jobs_collected': stats.total_jobs_collected,
            'total_new_jobs': stats.total_new_jobs,
            'total_updated_jobs': stats.total_updated_jobs,
            'total_closed_jobs': stats.total_closed_jobs
        }
        collection_state['last_run'] = datetime.now(timezone.utc).isoformat()
        logger.info(f"Scheduled discovery complete: {stats.total_discovered} companies")
    finally:
        get_db().release_advisory_lock('scheduled_discovery')

def scheduled_tier1_expansion():
    if not get_db().acquire_advisory_lock('tier1_expansion'):
        return
    try:
        logger.info("Starting Tier 1 seed expansion")
        from seed_expander import run_tier1_expansion
        added = asyncio.run(run_tier1_expansion())
        logger.info(f"Tier 1 expansion complete: {added} seeds added")
    finally:
        get_db().release_advisory_lock('tier1_expansion')

def scheduled_tier2_expansion():
    if not get_db().acquire_advisory_lock('tier2_expansion'):
        return
    try:
        logger.info("Starting Tier 2 seed expansion")
        from seed_expander import run_tier2_expansion
        added = asyncio.run(run_tier2_expansion())
        logger.info(f"Tier 2 expansion complete: {added} seeds added")
    finally:
        get_db().release_advisory_lock('tier2_expansion')

# =============================================================================
# UPGRADE MODULE SCHEDULED TASKS
# =============================================================================

def scheduled_v7_discovery():
    """Run V7 collector for new company discovery"""
    if not COLLECTOR_V7_AVAILABLE:
        logger.warning("V7 collector not available, skipping")
        return
    
    if not get_db().acquire_advisory_lock('v7_discovery'):
        logger.info("V7 discovery already running on another instance")
        return
    
    try:
        logger.info("🚀 Starting V7 scheduled discovery")
        v7_collection_state['is_running'] = True
        v7_collection_state['started_at'] = datetime.now(timezone.utc).isoformat()
        
        # Run V7 collector
        db = get_db()  # <-- ADD THIS
        stats = asyncio.run(collector_v7.run_discovery(db=db, max_seeds=500))
        
        v7_collection_state['last_stats'] = stats
        v7_collection_state['last_run'] = datetime.now(timezone.utc).isoformat()
        
        logger.info(f"✅ V7 discovery complete: {stats.get('companies_found', 0)} companies, {stats.get('jobs_found', 0)} jobs")
    except Exception as e:
        logger.error(f"❌ V7 discovery failed: {e}", exc_info=True)
    finally:
        v7_collection_state['is_running'] = False
        get_db().release_advisory_lock('v7_discovery')


def scheduled_mega_expansion():
    """Run mega seed expansion (weekly)"""
    if not MEGA_EXPANDER_AVAILABLE:
        logger.warning("Mega expander not available, skipping")
        return
    
    if not get_db().acquire_advisory_lock('mega_expansion'):
        logger.info("Mega expansion already running on another instance")
        return
    
    try:
        logger.info("🌱 Starting scheduled mega seed expansion")
        db = get_db()
        stats = asyncio.run(mega_seed_expander.run_expansion(db=db, tiers=[1, 2]))
        logger.info(f"✅ Mega expansion complete: {stats.get('total_saved', 0)} seeds added")
    except Exception as e:
        logger.error(f"❌ Mega expansion failed: {e}", exc_info=True)
    finally:
        get_db().release_advisory_lock('mega_expansion')


def scheduled_self_growth():
    """Run self-growth intelligence (daily)"""
    if not SELF_GROWTH_AVAILABLE:
        logger.warning("Self-growth not available, skipping")
        return
    
    if not get_db().acquire_advisory_lock('self_growth'):
        logger.info("Self-growth already running on another instance")
        return
    
    try:
        logger.info("🧠 Starting scheduled self-growth analysis")
        
        db = get_db()
        stats = asyncio.run(self_growth_intelligence.run_self_growth(db, limit=200))
        
        logger.info(f"✅ Self-growth complete: {stats.get('discoveries', 0)} new companies discovered")
    except Exception as e:
        logger.error(f"❌ Self-growth failed: {e}", exc_info=True)
    finally:
        get_db().release_advisory_lock('self_growth')


def scheduled_snapshot_cleanup():
    """Monthly cleanup of old snapshots"""
    if not get_db().acquire_advisory_lock('snapshot_cleanup'):
        return
    try:
        logger.info("Starting snapshot cleanup")
        db = get_db()
        deleted = db.cleanup_old_snapshots(90)  # Keep 90 days
        logger.info(f"Snapshot cleanup complete: deleted {deleted} old snapshots")
    finally:
        get_db().release_advisory_lock('snapshot_cleanup')


# =============================================================================
# SCHEDULER CONFIGURATION
# =============================================================================

# Legacy scheduled jobs
scheduler.add_job(scheduled_refresh, CronTrigger(hour=6), id='refresh', replace_existing=True)
scheduler.add_job(scheduled_discovery, CronTrigger(hour=7), id='discovery', replace_existing=True)
scheduler.add_job(scheduled_tier1_expansion, CronTrigger(day_of_week='sun', hour=3), id='tier1_expansion', replace_existing=True)
scheduler.add_job(scheduled_tier2_expansion, CronTrigger(day=1, hour=4), id='tier2_expansion', replace_existing=True)
scheduler.add_job(scheduled_snapshot_cleanup, CronTrigger(day=1, hour=2), id='snapshot_cleanup', replace_existing=True)

# Upgrade module scheduled jobs
if COLLECTOR_V7_AVAILABLE:
    # V7 discovery every 6 hours (offset from legacy discovery)
    scheduler.add_job(
        scheduled_v7_discovery, 
        CronTrigger(hour='0,6,12,18', minute=30),  # 30 min offset
        id='v7_discovery', 
        replace_existing=True
    )

if MEGA_EXPANDER_AVAILABLE:
    # Mega expansion weekly on Saturday (different from Tier 1/2)
    scheduler.add_job(
        scheduled_mega_expansion, 
        CronTrigger(day_of_week='sat', hour=5),
        id='mega_expansion', 
        replace_existing=True
    )

if SELF_GROWTH_AVAILABLE:
    # Self-growth daily at 4 AM
    scheduler.add_job(
        scheduled_self_growth, 
        CronTrigger(hour=4),
        id='self_growth', 
        replace_existing=True
    )

logger.info("📅 Scheduler configured:")
logger.info("   - Refresh: Daily at 6:00 AM UTC")
logger.info("   - Discovery: Daily at 7:00 AM UTC")
logger.info("   - Tier 1 Expansion: Weekly (Sunday 3:00 AM UTC)")
logger.info("   - Tier 2 Expansion: Monthly (1st at 4:00 AM UTC)")
logger.info("   - Snapshot Cleanup: Monthly (1st at 2:00 AM UTC)")
if COLLECTOR_V7_AVAILABLE:
    logger.info("   - V7 Discovery: Every 6 hours at :30")
if MEGA_EXPANDER_AVAILABLE:
    logger.info("   - Mega Expansion: Weekly (Saturday 5:00 AM UTC)")
if SELF_GROWTH_AVAILABLE:
    logger.info("   - Self-Growth: Daily at 4:00 AM UTC")

# ============================================================================
# ERROR HANDLERS
# ============================================================================
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found', 'message': str(error)}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal error: {error}", exc_info=True)
    return jsonify({'error': 'Internal server error', 'message': str(error)}), 500

@app.errorhandler(Exception)
def handle_exception(error):
    logger.error(f"Unhandled exception: {error}", exc_info=True)
    return jsonify({'error': 'Server error', 'message': str(error)}), 500

# ============================================================================
# HEALTH & STATUS
# ============================================================================
@app.route('/health')
@limiter.exempt
def health():
    return jsonify({'status': 'healthy', 'timestamp': datetime.now(timezone.utc).isoformat()}), 200

@app.route('/')
@limiter.exempt
def index():
    return jsonify({
        'service': 'Job Intelligence Platform',
        'status': 'running',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'upgrade_modules': {
            'collector_v7': COLLECTOR_V7_AVAILABLE,
            'mega_expander': MEGA_EXPANDER_AVAILABLE,
            'self_growth': SELF_GROWTH_AVAILABLE,
            'config': UPGRADE_CONFIG_LOADED
        },
        'endpoints': {
            'dashboard': '/dashboard',
            'analytics': '/analytics',
            'trends': '/trends',
            'intelligence': '/intelligence',
            'companies': '/companies',
            'jobs': '/jobs',
            'submit_seed': '/submit-seed',
            'seed_admin': '/seed-admin',
            'api_stats': '/api/stats',
            'api_intel': '/api/intel',
            'api_companies': '/api/companies',
            'api_jobs': '/api/jobs',
            # Upgrade endpoints
            'api_v7_collect': '/api/collect/v7',
            'api_mega_expand': '/api/seeds/expand-mega',
            'api_self_growth': '/api/self-growth/run'
        }
    }), 200

@app.route('/api/seeds/expand-advanced', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
def expand_advanced_seeds():
    """Expand seeds using advanced multi-source collection"""
    try:
        def run_expansion():
            try:
                logger.info("Starting advanced seed expansion...")
                import seed_sources
                added = asyncio.run(seed_sources.run_advanced_seed_collection())
                logger.info(f"✅ Advanced expansion complete: {added} seeds added")
            except Exception as e:
                logger.error(f"❌ Advanced seed expansion failed: {e}", exc_info=True)
        
        # Start in background
        thread = threading.Thread(target=run_expansion, daemon=True)
        thread.start()
        
        return jsonify({
            'success': True,
            'message': 'Advanced seed expansion started in background',
            'note': 'This will collect from 7+ premium sources. Check logs for progress.'
        }), 202
        
    except Exception as e:
        logger.error(f"Error in advanced seed expansion: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# PAGE ROUTES
# ============================================================================
@app.route('/dashboard')
@optional_auth
def dashboard():
    return render_template('dashboard.html')

@app.route('/analytics')
@optional_auth
def analytics_page():
    """Advanced analytics page"""
    return render_template('analytics.html')

@app.route('/companies')
@optional_auth
def companies_page():
    return render_template('companies.html')

@app.route('/company/<int:company_id>')
@optional_auth
def company_page(company_id):
    return render_template('company.html')

@app.route('/jobs')
@optional_auth
def jobs_page():
    return render_template('jobs.html')

@app.route('/trends')
def trends_page():
    """Trends analysis page"""
    return render_template('trends.html')

@app.route('/submit-seed')
@optional_auth
def submit_seed_page():
    return render_template('submit-seed.html')

@app.route('/seed-admin')
@optional_auth
def seed_admin_page():
    """Seed management admin page"""
    return render_template('seed-admin.html')

@app.route('/salary-insights')
@limiter.exempt
def salary_insights_page():
    """Salary insights page"""
    return render_template('salary-insights.html')

@app.route('/intelligence')
def intelligence_page():
    """Intelligence alerts page"""
    return render_template('intelligence.html')

# ============================================================================
# ANALYTICS ENDPOINTS - FIXED
# ============================================================================
@app.route('/api/debug/analytics')
@limiter.limit("30 per minute")
def debug_analytics():
    """Debug endpoint to check analytics data"""
    try:
        db = get_db()
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                # Check if we have any active jobs
                cur.execute("SELECT COUNT(*) FROM job_archive WHERE status = 'active'")
                active_jobs = cur.fetchone()[0]
                
                # Check if we have companies
                cur.execute("SELECT COUNT(*) FROM companies")
                total_companies = cur.fetchone()[0]
                
                # Check if we have any skills data (sample a few jobs)
                cur.execute("SELECT title FROM job_archive WHERE status = 'active' LIMIT 100")
                job_titles = [row[0] for row in cur.fetchall()]
                
                # Check locations
                cur.execute("SELECT DISTINCT location FROM job_archive WHERE status = 'active' AND location IS NOT NULL LIMIT 100")
                locations = [row[0] for row in cur.fetchall()]
                
                # Check departments
                cur.execute("SELECT DISTINCT department FROM job_archive WHERE status = 'active' AND department IS NOT NULL LIMIT 100")
                departments = [row[0] for row in cur.fetchall()]
                
                return jsonify({
                    'active_jobs': active_jobs,
                    'total_companies': total_companies,
                    'sample_job_titles': job_titles,
                    'sample_locations': locations,
                    'sample_departments': departments,
                    'has_data': active_jobs > 0
                }), 200
    except Exception as e:
        logger.error(f"Debug error: {e}", exc_info=True)
        return jsonify({'error': str(e), 'has_data': False}), 200

@app.route('/api/advanced-analytics')
@limiter.limit("30 per minute")
@optional_auth
def get_advanced_analytics_simple():
    """Get advanced analytics (simple endpoint)"""
    try:
        db = get_db()
        analytics = db.get_advanced_analytics()
        return jsonify(analytics), 200
    except Exception as e:
        logger.error(f"Error getting advanced analytics: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/advanced')
@limiter.limit("30 per minute")
@optional_auth
def get_advanced_analytics_api():
    """Get advanced analytics with optional cutoff date"""
    try:
        db = get_db()
        analytics = db.get_advanced_analytics()
        return jsonify(analytics), 200
    except Exception as e:
        logger.error(f"Error getting advanced analytics: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

# ============================================================================
# STATS & INTELLIGENCE
# ============================================================================
@app.route('/api/intelligence/location-expansions')
@limiter.limit("30 per minute")
@optional_auth
def get_location_expansions_api():
    """Get recent location expansion events with optional cutoff date"""
    try:
        days = request.args.get('days', 30, type=int)
        days = min(days, 365)
        cutoff_date = request.args.get('cutoff_date', TRENDS_CUTOFF_DATE)
        
        db = get_db()
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        c.company_name,
                        ie.metadata->>'location' as new_location,
                        ie.metadata->>'job_count' as job_count,
                        ie.detected_at
                    FROM intelligence_events ie
                    JOIN companies c ON ie.company_id = c.id
                    WHERE ie.event_type = 'location_expansion'
                    AND ie.detected_at >= NOW() - INTERVAL %s
                    AND ie.detected_at >= %s::timestamp
                    ORDER BY ie.detected_at DESC
                    LIMIT 50
                """, (f'{days} days', cutoff_date))
                columns = [desc[0] for desc in cur.description]
                expansions = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        return jsonify({
            'days': days,
            'total_expansions': len(expansions),
            'expansions': expansions,
            'cutoff_date': cutoff_date
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting location expansions: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/intelligence/events')
@limiter.limit("30 per minute")
@optional_auth
def get_intelligence_events_api():
    """Get all intelligence events"""
    try:
        days = request.args.get('days', 30, type=int)
        event_type = request.args.get('type', None)
        
        db = get_db()
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                if event_type:
                    cur.execute("""
                        SELECT 
                            ie.id,
                            ie.event_type,
                            ie.severity,
                            ie.metadata,
                            ie.detected_at,
                            c.company_name,
                            c.id as company_id
                        FROM intelligence_events ie
                        JOIN companies c ON ie.company_id = c.id
                        WHERE ie.detected_at >= NOW() - INTERVAL %s
                        AND ie.event_type = %s
                        ORDER BY ie.detected_at DESC
                        LIMIT 100
                    """, (f'{days} days', event_type))
                else:
                    cur.execute("""
                        SELECT 
                            ie.id,
                            ie.event_type,
                            ie.severity,
                            ie.metadata,
                            ie.detected_at,
                            c.company_name,
                            c.id as company_id
                        FROM intelligence_events ie
                        JOIN companies c ON ie.company_id = c.id
                        WHERE ie.detected_at >= NOW() - INTERVAL %s
                        ORDER BY ie.detected_at DESC
                        LIMIT 100
                    """, (f'{days} days',))
                
                columns = [desc[0] for desc in cur.description]
                events = [dict(zip(columns, row)) for row in cur.fetchall()]
                
                return jsonify({
                    'days': days,
                    'event_type': event_type,
                    'total_events': len(events),
                    'events': events
                }), 200
                
    except Exception as e:
        logger.error(f"Error getting intelligence events: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats')
@limiter.limit("60 per minute")
def get_stats():
    """Get platform statistics with optional cutoff date"""
    try:
        cutoff_date = request.args.get('cutoff_date', TRENDS_CUTOFF_DATE)
        db = get_db()
        stats = db.get_stats()
        
        # Add cutoff date info
        stats['cutoff_date'] = cutoff_date
        
        # Add upgrade module status
        stats['upgrade_modules'] = {
            'collector_v7': COLLECTOR_V7_AVAILABLE,
            'mega_expander': MEGA_EXPANDER_AVAILABLE,
            'self_growth': SELF_GROWTH_AVAILABLE
        }
        
        return jsonify(stats), 200
    except Exception as e:
        logger.error(f"Error getting stats: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/intel')
@limiter.limit(RATE_LIMITS['authenticated_read'])
@require_api_key
def api_intel():
    try:
        days = request.args.get('days', 14, type=int)
        db = get_db()
        surges, declines = db.get_job_count_changes(days=days)
        expansions = db.get_location_expansions(days=days)
        return jsonify({
            'surges': surges,
            'declines': declines,
            'expansions': expansions
        }), 200
    except Exception as e:
        logger.error(f"Error getting intelligence: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/trends')
@limiter.limit(RATE_LIMITS['authenticated_read'])
@require_api_key
def api_trends():
    try:
        days = request.args.get('days', 7, type=int)
        db = get_db()
        trends = db.get_market_trends(days=days)
        return jsonify({'granular': trends}), 200
    except Exception as e:
        logger.error(f"Error getting trends: {e}")
        return jsonify({'error': str(e)}), 500

# ============================================================================
# SALARY INSIGHTS
# ============================================================================
@app.route('/api/salary-insights', methods=['GET'])
@limiter.limit("30 per minute")
@require_api_key
def get_salary_insights():
    """Get comprehensive salary insights"""
    try:
        from psycopg2.extras import RealDictCursor
        db = get_db()
        
        with db.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Overview metrics
                cur.execute("""
                    SELECT 
                        COUNT(*) as jobs_with_salary,
                        MIN(salary_min) as min_salary,
                        MAX(salary_max) as max_salary,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (salary_min + salary_max) / 2) as median_salary,
                        (SELECT COUNT(*) FROM job_archive WHERE status = 'active') as total_jobs
                    FROM job_archive
                    WHERE status = 'active' AND salary_min IS NOT NULL AND salary_max IS NOT NULL
                """)
                overview_row = cur.fetchone()
                overview = dict(overview_row) if overview_row else {
                    'jobs_with_salary': 0,
                    'min_salary': 0,
                    'max_salary': 0,
                    'median_salary': 0,
                    'total_jobs': 0
                }
                
                # By role
                cur.execute("""
                    SELECT 
                        title as role,
                        AVG((salary_min + salary_max) / 2) as avg_salary,
                        COUNT(*) as count
                    FROM job_archive
                    WHERE status = 'active' AND salary_min IS NOT NULL
                    GROUP BY title
                    HAVING COUNT(*) >= 1
                    ORDER BY avg_salary DESC
                    LIMIT 20
                """)
                by_role = [dict(row) for row in cur.fetchall()]
                
                # By location
                cur.execute("""
                    SELECT 
                        location,
                        AVG((salary_min + salary_max) / 2) as avg_salary,
                        COUNT(*) as count
                    FROM job_archive
                    WHERE status = 'active' AND salary_min IS NOT NULL AND location IS NOT NULL
                    GROUP BY location
                    HAVING COUNT(*) >= 1
                    ORDER BY avg_salary DESC
                    LIMIT 15
                """)
                by_location = [dict(row) for row in cur.fetchall()]
                
                # By company
                cur.execute("""
                    SELECT 
                        c.company_name as company,
                        AVG((j.salary_min + j.salary_max) / 2) as avg_salary,
                        COUNT(*) as count
                    FROM job_archive j
                    JOIN companies c ON j.company_id = c.id
                    WHERE j.status = 'active' AND j.salary_min IS NOT NULL
                    GROUP BY c.company_name
                    HAVING COUNT(*) >= 1
                    ORDER BY avg_salary DESC
                    LIMIT 15
                """)
                by_company = [dict(row) for row in cur.fetchall()]
                
                # Distribution
                cur.execute("""
                    SELECT 
                        CASE 
                            WHEN (salary_min + salary_max) / 2 < 75000 THEN '<$75k'
                            WHEN (salary_min + salary_max) / 2 < 100000 THEN '$75k-$100k'
                            WHEN (salary_min + salary_max) / 2 < 150000 THEN '$100k-$150k'
                            WHEN (salary_min + salary_max) / 2 < 200000 THEN '$150k-$200k'
                            WHEN (salary_min + salary_max) / 2 < 250000 THEN '$200k-$250k'
                            ELSE '$250k+'
                        END as range,
                        COUNT(*) as count
                    FROM job_archive
                    WHERE status = 'active' AND salary_min IS NOT NULL
                    GROUP BY range
                    ORDER BY MIN((salary_min + salary_max) / 2)
                """)
                distribution = [dict(row) for row in cur.fetchall()]
                
                # Detailed breakdown
                cur.execute("""
                    SELECT 
                        j.title as role,
                        c.company_name as company,
                        j.location,
                        j.salary_min,
                        j.salary_max,
                        COALESCE(j.salary_currency, 'USD') as currency,
                        COUNT(*) as count
                    FROM job_archive j
                    JOIN companies c ON j.company_id = c.id
                    WHERE j.status = 'active' AND j.salary_min IS NOT NULL
                    GROUP BY j.title, c.company_name, j.location, j.salary_min, j.salary_max, j.salary_currency
                    ORDER BY j.salary_max DESC
                    LIMIT 100
                """)
                detailed = [dict(row) for row in cur.fetchall()]
                
                # Percentiles
                cur.execute("""
                    SELECT 
                        PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY (salary_min + salary_max) / 2) as p10,
                        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY (salary_min + salary_max) / 2) as p25,
                        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY (salary_min + salary_max) / 2) as p50,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY (salary_min + salary_max) / 2) as p75,
                        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY (salary_min + salary_max) / 2) as p90
                    FROM job_archive
                    WHERE status = 'active' AND salary_min IS NOT NULL
                """)
                percentile_row = cur.fetchone()
                percentiles = dict(percentile_row) if percentile_row else {
                    'p10': 0, 'p25': 0, 'p50': 0, 'p75': 0, 'p90': 0
                }
                
                return jsonify({
                    'overview': overview,
                    'by_role': by_role,
                    'by_location': by_location,
                    'by_company': by_company,
                    'distribution': distribution,
                    'detailed': detailed,
                    'percentiles': percentiles
                }), 200
    except Exception as e:
        logger.error(f"Error in salary insights: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

# ============================================================================
# COMPANIES API
# ============================================================================
@app.route('/api/companies')
@limiter.limit(RATE_LIMITS['authenticated_read'])
@require_api_key
def api_companies():
    try:
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 50, type=int), 100)
        search = request.args.get('search', '').strip()
        ats_type = request.args.get('ats_type', '').strip()
        
        db = get_db()
        query = """
            SELECT 
                c.id, c.company_name, c.ats_type, c.board_url,
                c.job_count, c.last_scraped, c.created_at,
                COALESCE(c.job_count, 0) as active_jobs
            FROM companies c
            WHERE 1=1
        """
        params = []
        
        if search:
            query += " AND c.company_name ILIKE %s"
            params.append(f'%{search}%')
        
        if ats_type:
            query += " AND c.ats_type = %s"
            params.append(ats_type)
        
        query += " ORDER BY c.job_count DESC NULLS LAST, c.company_name LIMIT %s OFFSET %s"
        params.extend([per_page, (page - 1) * per_page])
        
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                companies = [dict(zip(columns, row)) for row in cur.fetchall()]
                
                count_query = "SELECT COUNT(*) FROM companies WHERE 1=1"
                count_params = []
                if search:
                    count_query += " AND company_name ILIKE %s"
                    count_params.append(f'%{search}%')
                if ats_type:
                    count_query += " AND ats_type = %s"
                    count_params.append(ats_type)
                
                cur.execute(count_query, count_params)
                total = cur.fetchone()[0]
        
        return jsonify({
            'companies': companies,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'pages': (total + per_page - 1) // per_page if total > 0 else 1
            }
        }), 200
    except Exception as e:
        logger.error(f"Error getting companies: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/companies/<int:company_id>')
@limiter.limit("30 per minute")
@optional_auth
def api_company_detail(company_id):
    try:
        db = get_db()
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM companies WHERE id = %s", (company_id,))
                company = cur.fetchone()
                if not company:
                    return jsonify({'error': 'Company not found'}), 404
                
                columns = [desc[0] for desc in cur.description]
                company_data = dict(zip(columns, company))
                
                # Get ALL jobs for this company (NO LIMIT)
                cur.execute("""
                    SELECT job_id, title, location, department, work_type, job_url, 
                           posted_date, salary_min, salary_max, salary_currency, status, first_seen, last_seen
                    FROM job_archive
                    WHERE company_id = %s
                    ORDER BY status DESC, first_seen DESC
                """, (company_id,))
                
                jobs_columns = [desc[0] for desc in cur.description]
                jobs = [dict(zip(jobs_columns, row)) for row in cur.fetchall()]
                
                company_data['jobs'] = jobs
                
                logger.info(f"Company {company_id} ({company_data.get('company_name')}): Returning {len(jobs)} jobs")
                
                return jsonify(company_data), 200
    except Exception as e:
        logger.error(f"Error getting company detail: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

# ============================================================================
# TRENDS & ANALYTICS ENDPOINTS - NEW
# ============================================================================

@app.route('/api/trends/company/<int:company_id>')
@limiter.limit("30 per minute")
@optional_auth
def get_company_trend(company_id):
    """Get historical job count trends for a company"""
    try:
        days = request.args.get('days', 90, type=int)
        days = min(days, 365)  # Max 1 year
        
        db = get_db()
        trends = db.get_company_growth_trend(company_id, days)
        
        return jsonify({
            'company_id': company_id,
            'days': days,
            'data_points': len(trends),
            'trends': trends
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting company trend: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/trends/market')
@limiter.limit("30 per minute")
@optional_auth
def get_market_trend():
    """Get overall market hiring trends"""
    try:
        days = request.args.get('days', 90, type=int)
        days = min(days, 365)
        
        db = get_db()
        trends = db.get_market_trends(days)
        
        # Calculate growth rate
        if len(trends) >= 2:
            first_total = trends[0]['total_jobs']
            last_total = trends[-1]['total_jobs']
            growth_rate = ((last_total - first_total) / first_total * 100) if first_total > 0 else 0
        else:
            growth_rate = 0
        
        return jsonify({
            'days': days,
            'data_points': len(trends),
            'growth_rate': round(growth_rate, 2),
            'trends': trends,
            'summary': {
                'current_jobs': trends[-1]['total_jobs'] if trends else 0,
                'current_companies': trends[-1]['active_companies'] if trends else 0,
                'avg_jobs_per_company': float(trends[-1]['avg_jobs_per_company']) if trends else 0
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting market trends: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/trends/skills')
@limiter.limit("30 per minute")
@optional_auth
def get_skills_trend():
    """Get skills demand trends over time"""
    try:
        days = request.args.get('days', 90, type=int)
        days = min(days, 365)
        
        db = get_db()
        trends = db.get_skills_trends(days)
        
        # Calculate top growing skills
        if trends:
            weeks = sorted(trends.keys())
            if len(weeks) >= 2:
                first_week = weeks[0]
                last_week = weeks[-1]
                
                growth = {}
                for skill in trends[first_week].keys():
                    first_count = trends[first_week][skill]
                    last_count = trends[last_week][skill]
                    if first_count > 0:
                        growth[skill] = ((last_count - first_count) / first_count * 100)
                
                top_growing = sorted(growth.items(), key=lambda x: x[1], reverse=True)[:10]
            else:
                top_growing = []
        else:
            top_growing = []
        
        return jsonify({
            'days': days,
            'weeks_tracked': len(trends),
            'trends': trends,
            'top_growing': [{'skill': s, 'growth_percent': round(g, 1)} for s, g in top_growing]
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting skills trends: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/admin/init-database', methods=['POST'])
@limiter.exempt
@require_admin_key
def init_database():
    """One-time database initialization - adds indexes and cleans up snapshots"""
    try:
        results = {
            'indexes_added': False,
            'snapshots_cleaned': 0,
            'errors': []
        }
        
        # Add performance indexes
        try:
            db = get_db()
            db.add_performance_indexes()
            results['indexes_added'] = True
            logger.info("✅ Performance indexes added")
        except Exception as e:
            results['errors'].append(f"Index creation error: {str(e)}")
            logger.error(f"Index creation failed: {e}")
        
        # Cleanup old snapshots
        try:
            deleted = db.cleanup_old_snapshots(90)
            results['snapshots_cleaned'] = deleted
            logger.info(f"✅ Deleted {deleted} old snapshots")
        except Exception as e:
            results['errors'].append(f"Snapshot cleanup error: {str(e)}")
            logger.error(f"Snapshot cleanup failed: {e}")
        
        return jsonify({
            'success': len(results['errors']) == 0,
            'message': 'Database initialization complete' if len(results['errors']) == 0 else 'Initialization completed with errors',
            'results': results
        }), 200
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/companies/top')
@limiter.limit("60 per minute")
@optional_auth
def get_top_companies():
    """Get top companies by job count with optional cutoff date"""
    try:
        limit = request.args.get('limit', 20, type=int)
        limit = min(limit, 100)
        cutoff_date = request.args.get('cutoff_date', TRENDS_CUTOFF_DATE)
        
        db = get_db()
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.id, c.company_name, c.ats_type, c.job_count, c.last_scraped
                    FROM companies c
                    ORDER BY c.job_count DESC
                    LIMIT %s
                """, (limit,))
                columns = [desc[0] for desc in cur.description]
                companies = [dict(zip(columns, row)) for row in cur.fetchall()]
                
        return jsonify({
            'companies': companies,
            'count': len(companies),
            'cutoff_date': cutoff_date
        }), 200
    except Exception as e:
        logger.error(f"Error getting top companies: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/trends/salary')
@limiter.limit("30 per minute")
@optional_auth
def get_salary_trend():
    """Get salary trends over time"""
    try:
        days = request.args.get('days', 90, type=int)
        days = min(days, 365)
        
        db = get_db()
        trends = db.get_salary_trends(days)
        
        # Calculate salary inflation
        if len(trends) >= 2:
            first_avg = float(trends[0]['avg_salary'])
            last_avg = float(trends[-1]['avg_salary'])
            inflation = ((last_avg - first_avg) / first_avg * 100) if first_avg > 0 else 0
        else:
            inflation = 0
        
        return jsonify({
            'days': days,
            'data_points': len(trends),
            'salary_inflation_percent': round(inflation, 2),
            'trends': trends,
            'current_average': float(trends[-1]['avg_salary']) if trends else 0,
            'current_median': float(trends[-1]['median_salary']) if trends else 0
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting salary trends: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/trends/departments')
@limiter.limit("30 per minute")
@optional_auth
def get_department_trend():
    """Get hiring trends by department"""
    try:
        days = request.args.get('days', 90, type=int)
        days = min(days, 365)
        
        db = get_db()
        trends = db.get_department_growth_trends(days)
        
        # Calculate top growing departments
        if trends:
            weeks = sorted(trends.keys())
            if len(weeks) >= 2:
                first_week = weeks[0]
                last_week = weeks[-1]
                
                growth = {}
                all_depts = set()
                for week_data in trends.values():
                    all_depts.update(week_data.keys())
                
                for dept in all_depts:
                    first_count = trends[first_week].get(dept, 0)
                    last_count = trends[last_week].get(dept, 0)
                    if first_count > 0:
                        growth[dept] = ((last_count - first_count) / first_count * 100)
                    elif last_count > 0:
                        growth[dept] = 100  # New department
                
                top_growing = sorted(growth.items(), key=lambda x: x[1], reverse=True)[:10]
            else:
                top_growing = []
        else:
            top_growing = []
        
        return jsonify({
            'days': days,
            'weeks_tracked': len(trends),
            'trends': trends,
            'top_growing': [{'department': d, 'growth_percent': round(g, 1)} for d, g in top_growing]
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting department trends: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/metrics/retention')
@limiter.limit("30 per minute")
@optional_auth
def get_retention_metrics():
    """Get job retention and refill metrics"""
    try:
        db = get_db()
        metrics = db.get_retention_metrics()
        
        return jsonify(metrics), 200
        
    except Exception as e:
        logger.error(f"Error getting retention metrics: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


# ============================================================================
# ADMIN: MAINTENANCE & OPTIMIZATION
# ============================================================================

@app.route('/api/admin/cleanup-snapshots', methods=['POST'])
@limiter.exempt
@require_admin_key
def cleanup_snapshots():
    """Cleanup old snapshots"""
    try:
        data = request.get_json() or {}
        days_to_keep = data.get('days_to_keep', 90)
        
        db = get_db()
        deleted = db.cleanup_old_snapshots(days_to_keep)
        
        return jsonify({
            'success': True,
            'deleted_count': deleted,
            'days_kept': days_to_keep,
            'message': f'Deleted {deleted} snapshots older than {days_to_keep} days'
        }), 200
        
    except Exception as e:
        logger.error(f"Snapshot cleanup failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/add-indexes', methods=['POST'])
@limiter.exempt
@require_admin_key
def add_indexes():
    """Add performance indexes to database"""
    try:
        db = get_db()
        db.add_performance_indexes()
        
        return jsonify({
            'success': True,
            'message': 'Performance indexes created successfully'
        }), 200
        
    except Exception as e:
        logger.error(f"Index creation failed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

# ============================================================================
# JOBS API
# ============================================================================
@app.route('/api/jobs')
@limiter.limit("30 per minute")
@optional_auth
def get_jobs_api():
    """Get all jobs with filters"""
    try:
        db = get_db()
        
        # Support higher limits
        limit = int(request.args.get('limit', 50000))
        limit = min(limit, 100000)  # Max 100k
        
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        j.title,
                        j.location,
                        j.department,
                        j.work_type,
                        j.job_url,
                        j.first_seen,
                        j.last_seen,
                        c.company_name,
                        c.ats_type
                    FROM job_archive j
                    JOIN companies c ON j.company_id = c.id
                    WHERE j.status = 'active'
                    ORDER BY j.last_seen DESC
                    LIMIT %s
                """, (limit,))
                
                columns = [desc[0] for desc in cur.description]
                jobs = [dict(zip(columns, row)) for row in cur.fetchall()]
                
                cur.execute("SELECT COUNT(DISTINCT company_id) FROM job_archive WHERE status = 'active'")
                total_companies = cur.fetchone()[0]
                
                logger.info(f"Jobs API: Returning {len(jobs)} jobs (limit: {limit})")
                
                return jsonify({
                    'jobs': jobs,
                    'total_jobs': len(jobs),
                    'total_companies': total_companies,
                    'limit_applied': limit
                }), 200
    except Exception as e:
        logger.error(f"Error getting jobs: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500
        
@app.route('/api/jobs/<int:job_id>')
@limiter.limit(RATE_LIMITS['authenticated_read'])
@require_api_key
def api_job_detail(job_id):
    try:
        db = get_db()
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT j.job_id, j.title, j.location, j.department, j.work_type,
                           j.job_url, j.posted_date, j.salary_min, j.salary_max, j.salary_currency,
                           j.status, j.first_seen, j.last_seen, j.metadata,
                           c.company_name, c.ats_type, c.board_url
                    FROM job_archive j
                    JOIN companies c ON j.company_id = c.id
                    WHERE j.id = %s
                """, (job_id,))
                job = cur.fetchone()
                if not job:
                    return jsonify({'error': 'Job not found'}), 404
                columns = [desc[0] for desc in cur.description]
                return jsonify(dict(zip(columns, job))), 200
    except Exception as e:
        logger.error(f"Error getting job detail: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

# ============================================================================
# SEED MANAGEMENT API
# ============================================================================
@app.route('/api/seeds/stats')
@limiter.limit("30 per minute")
def get_seed_stats():
    """Get seed statistics"""
    try:
        db = get_db()
        stats = db.get_seed_stats()
        return jsonify(stats), 200
    except Exception as e:
        logger.error(f"Error getting seed stats: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/seeds/add', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
def add_seed():
    """Add a company seed manually"""
    try:
        data = request.get_json()
        company_name = data.get('company_name', '').strip()
        website_url = data.get('website_url', '').strip()
        
        if not company_name:
            return jsonify({'error': 'Company name is required'}), 400
        
        if not website_url:
            return jsonify({'error': 'Careers page URL is required'}), 400
        
        # Validate URL format
        if not website_url.startswith(('http://', 'https://')):
            return jsonify({'error': 'Invalid URL format. Must start with http:// or https://'}), 400
        
        logger.info(f"Adding manual seed: {company_name} - {website_url}")
        
        db = get_db()
        # Add to seeds database
        success = db.add_manual_seed(company_name, website_url)
        
        if success:
            return jsonify({
                'success': True,
                'message': f'{company_name} added successfully. It will be tested in the next discovery run.',
                'company_name': company_name,
                'website_url': website_url
            }), 200
        else:
            # Check if it already exists
            existing = db.get_company_id(company_name)
            if existing:
                return jsonify({
                    'error': f'{company_name} is already being tracked in the system.'
                }), 409
            else:
                return jsonify({
                    'error': f'{company_name} seed already exists in the queue.'
                }), 409
    
    except Exception as e:
        logger.error(f"Error adding seed: {e}", exc_info=True)
        return jsonify({'error': 'Internal server error. Please try again.'}), 500

@app.route('/api/seeds/manual', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
@require_api_key
def api_manual_seed():
    try:
        data = request.get_json() or {}
        company_name = data.get('company_name', '').strip()
        website_url = data.get('website_url', '').strip()
        ats_hint = data.get('ats_hint', '').strip().lower()
        
        if not company_name:
            return jsonify({'success': False, 'error': 'Company name is required'}), 400
        
        db = get_db()
        added = db.add_manual_seed(company_name, website_url=website_url)
        
        if not added:
            return jsonify({'success': False, 'error': 'Company already exists'}), 409
        
        if website_url and data.get('test_immediately', False):
            def test_seed():
                try:
                    from collector import JobIntelCollector
                    collector = JobIntelCollector()
                    asyncio.run(collector._test_company(company_name, ats_hint))
                except Exception as e:
                    logger.error(f"Test seed failed: {e}", exc_info=True)
            
            threading.Thread(target=test_seed, daemon=True).start()
            return jsonify({
                'success': True,
                'message': f'Added {company_name} and started testing',
                'company_name': company_name
            }), 201
        
        return jsonify({
            'success': True,
            'message': f'Added {company_name} to seeds',
            'company_name': company_name
        }), 201
    except Exception as e:
        logger.error(f"Error adding manual seed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/seeds/reset', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
def reset_seeds():
    """Reset test counters for low-test seeds"""
    try:
        db = get_db()
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE seed_companies 
                    SET times_tested = 0, 
                        last_tested_at = NULL,
                        success_rate = 0
                    WHERE times_tested > 0 
                    AND times_tested <= 2
                    AND is_blacklisted = false
                """)
                reset_count = cur.rowcount
                conn.commit()
                logger.info(f"Reset {reset_count} seeds")
                return jsonify({'success': True, 'reset_count': reset_count}), 200
    except Exception as e:
        logger.error(f"Error resetting seeds: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/seeds/unblacklist-premium', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
def unblacklist_premium():
    """Unblacklist Tier 1 and 2 seeds"""
    try:
        db = get_db()
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE seed_companies 
                    SET is_blacklisted = false,
                        times_tested = 0,
                        times_successful = 0,
                        success_rate = 0
                    WHERE is_blacklisted = true
                    AND tier IN (1, 2)
                """)
                unblacklisted_count = cur.rowcount
                conn.commit()
                logger.info(f"Unblacklisted {unblacklisted_count} premium seeds")
                return jsonify({'success': True, 'unblacklisted_count': unblacklisted_count}), 200
    except Exception as e:
        logger.error(f"Error unblacklisting seeds: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/seeds/clean-garbage', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
def clean_garbage_seeds():
    """Clean garbage seeds from database"""
    try:
        db = get_db()
        deleted_count = db.cleanup_garbage_seeds()
        return jsonify({'success': True, 'deleted_count': deleted_count}), 200
    except Exception as e:
        logger.error(f"Error cleaning garbage seeds: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/seeds/expand-tier1', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
def expand_tier1_seeds():
    """Expand Tier 1 seeds"""
    try:
        from seed_expander import run_tier1_expansion
        added_count = asyncio.run(run_tier1_expansion())
        return jsonify({'success': True, 'added_count': added_count}), 200
    except Exception as e:
        logger.error(f"Error expanding Tier 1: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/seeds/expand-tier2', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
def expand_tier2_seeds():
    """Expand Tier 2 seeds"""
    try:
        from seed_expander import run_tier2_expansion
        added_count = asyncio.run(run_tier2_expansion())
        return jsonify({'success': True, 'added_count': added_count}), 200
    except Exception as e:
        logger.error(f"Error expanding Tier 2: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/seeds/nuclear-reset', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
def nuclear_reset_seeds():
    """Nuclear option: reset ALL seeds"""
    try:
        db = get_db()
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE seed_companies 
                    SET times_tested = 0,
                        times_successful = 0,
                        last_tested_at = NULL,
                        success_rate = 0,
                        is_blacklisted = false
                """)
                reset_count = cur.rowcount
                conn.commit()
                logger.info(f"Nuclear reset: {reset_count} seeds")
                return jsonify({'success': True, 'reset_count': reset_count}), 200
    except Exception as e:
        logger.error(f"Error in nuclear reset: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/seeds/expand', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
@require_api_key
def api_expand_seeds():
    """Expand seed database with tier 1 or tier 2 companies"""
    try:
        # Get tier from query param OR request body
        tier = request.args.get('tier', 'tier1')
        
        # Try to get JSON body, but don't fail if it's empty
        try:
            data = request.get_json(silent=True) or {}
            if 'tier' in data:
                tier = data['tier']
        except:
            data = {}
        
        # Normalize tier value
        if tier in ['1', 'tier1']:
            tier = 'tier1'
        elif tier in ['2', 'tier2']:
            tier = 'tier2'
        
        logger.info(f"🌱 Seed expansion requested: {tier}")
        
        # Check if seed_expander module exists
        try:
            import sys
            sys.path.insert(0, '/app')
            import seed_expander
            logger.info("✅ seed_expander module found")
        except ImportError as e:
            logger.error(f"❌ seed_expander.py not found: {e}")
            return jsonify({
                'success': False,
                'error': 'Seed expander module not deployed',
                'message': 'seed_expander.py is missing from the deployment. Please deploy it first.',
                'tier': tier
            }), 500
        
        def run_expansion():
            try:
                if tier == 'tier1':
                    logger.info("Starting Tier 1 expansion...")
                    added = asyncio.run(seed_expander.run_tier1_expansion())
                    logger.info(f"✅ Tier 1 expansion complete: {added} seeds added")
                elif tier == 'tier2':
                    logger.info("Starting Tier 2 expansion...")
                    added = asyncio.run(seed_expander.run_tier2_expansion())
                    logger.info(f"✅ Tier 2 expansion complete: {added} seeds added")
                else:
                    logger.info("Starting full expansion...")
                    added = asyncio.run(seed_expander.run_full_expansion())
                    logger.info(f"✅ Full expansion complete: {added} seeds added")
            except Exception as e:
                logger.error(f"❌ Seed expansion failed: {e}", exc_info=True)
        
        # Start expansion in background thread
        thread = threading.Thread(target=run_expansion, daemon=True)
        thread.start()
        
        return jsonify({
            'success': True,
            'message': f'Seed expansion ({tier}) started in background',
            'tier': tier,
            'note': 'Check logs for progress. This will take 2-5 minutes.'
        }), 202
        
    except Exception as e:
        logger.error(f"Error in seed expansion endpoint: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e),
            'message': 'Failed to start seed expansion'
        }), 500

# ============================================================================
# UPGRADE MODULE ENDPOINTS - V7 Collector, Mega Expander, Self-Growth
# ============================================================================

@app.route('/api/collect/v7', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
def api_collect_v7():
    """Run V7 collector for enhanced company discovery"""
    if not COLLECTOR_V7_AVAILABLE:
        return jsonify({
            'success': False,
            'error': 'V7 collector not available',
            'message': 'collector_v7.py is not deployed. Please add it to your repo.'
        }), 503
    
    if v7_collection_state['is_running']:
        return jsonify({
            'success': False,
            'error': 'V7 collection already running',
            'started_at': v7_collection_state['started_at']
        }), 409
    
    try:
        data = request.get_json(silent=True) or {}
        max_seeds = min(data.get('max_seeds', 500), 2000)
        
        def run_v7_thread():
            v7_collection_state['is_running'] = True
            v7_collection_state['started_at'] = datetime.now(timezone.utc).isoformat()
            try:
                logger.info(f"🚀 Starting V7 collection for {max_seeds} seeds")
                db = get_db()  # <-- ADD THIS
                stats = asyncio.run(collector_v7.run_discovery(db=db, max_seeds=max_seeds))  # <-- FIX THIS
                v7_collection_state['last_stats'] = stats
                v7_collection_state['last_run'] = datetime.now(timezone.utc).isoformat()
                logger.info(f"✅ V7 collection complete: {stats}")
            except Exception as e:
                logger.error(f"❌ V7 collection failed: {e}", exc_info=True)
            finally:
                v7_collection_state['is_running'] = False
        
        thread = threading.Thread(target=run_v7_thread, daemon=True)
        thread.start()
        
        return jsonify({
            'success': True,
            'message': f'V7 collection started for {max_seeds} seeds',
            'max_seeds': max_seeds,
            'features': ['15 ATS types', 'parallel testing', 'aggressive token gen', 'self-discovery']
        }), 202
        
    except Exception as e:
        logger.error(f"Error starting V7 collection: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/collect/v7/test', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
def api_collect_v7_test():
    """Test V7 collector on specific companies"""
    if not COLLECTOR_V7_AVAILABLE:
        return jsonify({
            'success': False,
            'error': 'V7 collector not available'
        }), 503
    
    try:
        data = request.get_json() or {}
        companies = data.get('companies', [])
        
        if not companies:
            return jsonify({
                'success': False,
                'error': 'No companies specified',
                'example': {'companies': ['OpenAI', 'Anthropic', 'Stripe']}
            }), 400
        
        if isinstance(companies, str):
            companies = [c.strip() for c in companies.split(',')]
        
        companies = companies[:20]  # Max 20 companies for test
        
        def run_test():
            try:
                logger.info(f"🧪 Testing V7 collector on: {companies}")
                results = asyncio.run(collector_v7.test_companies(companies))
                logger.info(f"✅ V7 test complete: {results}")
            except Exception as e:
                logger.error(f"❌ V7 test failed: {e}", exc_info=True)
        
        thread = threading.Thread(target=run_test, daemon=True)
        thread.start()
        
        return jsonify({
            'success': True,
            'message': f'Testing {len(companies)} companies with V7 collector',
            'companies': companies
        }), 202
        
    except Exception as e:
        logger.error(f"Error in V7 test: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/collect/v7/status')
@limiter.limit("60 per minute")
def api_collect_v7_status():
    """Get V7 collector status"""
    return jsonify({
        'available': COLLECTOR_V7_AVAILABLE,
        'is_running': v7_collection_state['is_running'],
        'started_at': v7_collection_state['started_at'],
        'last_run': v7_collection_state['last_run'],
        'last_stats': v7_collection_state['last_stats']
    }), 200


@app.route('/api/seeds/expand-mega', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
def api_expand_mega():
    """Run mega seed expansion (20+ sources, 50k+ seeds)"""
    if not MEGA_EXPANDER_AVAILABLE:
        return jsonify({
            'success': False,
            'error': 'Mega expander not available',
            'message': 'mega_seed_expander.py is not deployed. Please add it to your repo.'
        }), 503
    
    try:
        data = request.get_json(silent=True) or {}
        tiers = data.get('tiers', [1, 2])
        
        if isinstance(tiers, str):
            tiers = [int(t.strip()) for t in tiers.split(',')]
        
        def run_mega():
            try:
                logger.info(f"🌱 Starting mega seed expansion for tiers: {tiers}")
                db = get_db()  # <-- ADD THIS
                stats = asyncio.run(mega_seed_expander.run_expansion(db=db, tiers=tiers))  # <-- PASS db
                logger.info(f"✅ Mega expansion complete: {stats}")
            except Exception as e:
                logger.error(f"❌ Mega expansion failed: {e}", exc_info=True)
        
        thread = threading.Thread(target=run_mega, daemon=True)
        thread.start()
        
        return jsonify({
            'success': True,
            'message': f'Mega seed expansion started for tiers {tiers}',
            'tiers': tiers,
            'sources': '20+ sources including YC, VCs, Inc 5000, Forbes lists',
            'expected_seeds': '10,000-50,000 depending on tiers'
        }), 202
        
    except Exception as e:
        logger.error(f"Error in mega expansion: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/self-growth/run', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
def api_self_growth_run():
    """Run self-growth intelligence analysis"""
    if not SELF_GROWTH_AVAILABLE:
        return jsonify({
            'success': False,
            'error': 'Self-growth not available',
            'message': 'self_growth_intelligence.py is not deployed. Please add it to your repo.'
        }), 503
    
    try:
        data = request.get_json(silent=True) or {}
        limit = min(data.get('limit', 200), 500)
        
        def run_growth():
            try:
                logger.info(f"🧠 Starting self-growth analysis (limit: {limit})")
                db = get_db()
                stats = asyncio.run(self_growth_intelligence.run_self_growth(db, limit=limit))
                logger.info(f"✅ Self-growth complete: {stats}")
            except Exception as e:
                logger.error(f"❌ Self-growth failed: {e}", exc_info=True)
        
        thread = threading.Thread(target=run_growth, daemon=True)
        thread.start()
        
        return jsonify({
            'success': True,
            'message': f'Self-growth analysis started (analyzing {limit} companies)',
            'limit': limit,
            'features': ['job description mining', 'website crawling', 'news monitoring', 'industry clustering']
        }), 202
        
    except Exception as e:
        logger.error(f"Error in self-growth: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/self-growth/discoveries')
@limiter.limit("30 per minute")
@optional_auth
def api_self_growth_discoveries():
    """Get self-growth discoveries info"""
    return jsonify({
        'message': 'Self-growth discoveries are logged to console and promoted directly to seed_companies table',
        'note': 'Check server logs for discovery details, or query seed_companies WHERE source LIKE \'self_growth_%\'',
        'query_example': "SELECT * FROM seed_companies WHERE source LIKE 'self_growth_%' ORDER BY created_at DESC LIMIT 50"
    }), 200


@app.route('/api/stats/enhanced')
@limiter.limit("30 per minute")
@optional_auth
def api_stats_enhanced():
    """Get enhanced stats with ATS breakdown and upgrade module status"""
    try:
        db = get_db()
        stats = db.get_stats()
        
        # Add ATS breakdown
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ats_type, COUNT(*) as count, SUM(job_count) as total_jobs
                    FROM companies
                    WHERE ats_type IS NOT NULL
                    GROUP BY ats_type
                    ORDER BY count DESC
                """)
                ats_breakdown = [
                    {'ats_type': row[0], 'companies': row[1], 'jobs': row[2] or 0}
                    for row in cur.fetchall()
                ]
        
        stats['ats_breakdown'] = ats_breakdown
        stats['upgrade_modules'] = {
            'collector_v7': {
                'available': COLLECTOR_V7_AVAILABLE,
                'is_running': v7_collection_state['is_running'],
                'last_run': v7_collection_state['last_run']
            },
            'mega_expander': {
                'available': MEGA_EXPANDER_AVAILABLE
            },
            'self_growth': {
                'available': SELF_GROWTH_AVAILABLE
            }
        }
        
        return jsonify(stats), 200
        
    except Exception as e:
        logger.error(f"Error getting enhanced stats: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

# ============================================================================
# COLLECTION & REFRESH (Legacy)
# ============================================================================
@app.route('/api/collect', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
def api_collect():
    if collection_state['is_running']:
        return jsonify({'error': 'Collection already running'}), 409
    
    data = request.get_json(silent=True) or {}
    max_companies = min(data.get('max_companies', 1000), 2000)
    
    def run_collection_thread():
        collection_state['is_running'] = True
        collection_state['mode'] = 'discovery'
        collection_state['current_progress'] = 0
        try:
            stats = asyncio.run(run_collection(max_companies=max_companies))
            run_daily_maintenance()
            collection_state['last_stats'] = {
                'total_tested': stats.total_tested,
                'total_discovered': stats.total_discovered,
                'total_jobs_collected': stats.total_jobs_collected,
                'total_new_jobs': stats.total_new_jobs,
                'total_updated_jobs': stats.total_updated_jobs,
                'total_closed_jobs': stats.total_closed_jobs
            }
            collection_state['last_run'] = datetime.now(timezone.utc).isoformat()
            logger.info(f"✅ Manual collection complete: {collection_state['last_stats']}")
        finally:
            collection_state['is_running'] = False
            collection_state['mode'] = None
            collection_state['current_progress'] = 0
    
    thread = threading.Thread(target=run_collection_thread, daemon=True)
    thread.start()
    
    return jsonify({'message': f'Discovery started for {max_companies} companies'}), 202

@app.route('/api/refresh', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
@require_api_key
def api_refresh():
    try:
        if collection_state['is_running']:
            return jsonify({'success': False, 'error': 'Collection already running'}), 409
        
        data = request.get_json() or {}
        hours_since_update = data.get('hours_since_update', data.get('hours', 24))
        max_companies = min(data.get('max_companies', 500), 1000)
        
        def run_refresh_thread():
            collection_state['is_running'] = True
            collection_state['mode'] = 'refresh'
            try:
                stats = asyncio.run(run_refresh(hours_since_update, max_companies))
                run_daily_maintenance()
                collection_state['last_stats'] = {
                    'total_tested': 0,
                    'total_discovered': 0,
                    'total_jobs_collected': stats.total_jobs_collected,
                    'total_new_jobs': stats.total_new_jobs,
                    'total_updated_jobs': stats.total_updated_jobs,
                    'total_closed_jobs': stats.total_closed_jobs
                }
                collection_state['last_run'] = datetime.now(timezone.utc).isoformat()
                logger.info(f"✅ Refresh complete: {collection_state['last_stats']}")
            except Exception as e:
                logger.error(f"Refresh failed: {e}", exc_info=True)
            finally:
                collection_state['is_running'] = False
                collection_state['mode'] = None
        
        thread = threading.Thread(target=run_refresh_thread, daemon=True)
        thread.start()
        
        return jsonify({
            'success': True,
            'message': f'Refresh started for up to {max_companies} companies'
        }), 202
    except Exception as e:
        logger.error(f"Error starting refresh: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

# ============================================================================
# ADMIN ENDPOINTS
# ============================================================================
@app.route('/api/admin/backfill-worktypes', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
def backfill_worktypes():
    """Backfill work types for existing jobs"""
    try:
        db = get_db()
        updated = db.backfill_work_types()
        return jsonify({
            'success': True,
            'updated_count': updated,
            'message': f'Successfully backfilled work_type for {updated} jobs'
        }), 200
    except Exception as e:
        logger.error(f"Error in backfill: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/admin/sql', methods=['POST'])
@require_admin_key
@limiter.exempt
def admin_sql_query():
    """
    Quick admin endpoint to run raw SQL queries.
    POST JSON: {"query": "SELECT COUNT(*) FROM job_archive WHERE status = 'active';"}
    Returns results as list of dicts.
    """
    try:
        data = request.get_json()
        if not data or 'query' not in data:
            return jsonify({'success': False, 'error': 'Missing "query" in JSON body'}), 400
        
        query = data['query'].strip()
        
        # Safety: Only allow SELECT queries
        if not query.upper().startswith('SELECT'):
            return jsonify({'success': False, 'error': 'Only SELECT queries are allowed'}), 400
        
        db = get_db()
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                columns = [desc[0] for desc in cur.description] if cur.description else []
                rows = cur.fetchall()
                results = [dict(zip(columns, row)) for row in rows]
                
                logging.info(f"Admin SQL executed: {query[:200]}{'...' if len(query) > 200 else ''}")
                
                return jsonify({
                    'success': True,
                    'row_count': len(results),
                    'results': results
                }), 200
                
    except Exception as e:
        logging.error(f"Admin SQL error: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/admin/fix-schema', methods=['POST'])
@limiter.exempt
@require_admin_key
def fix_schema():
    try:
        db = get_db()
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                logger.info("Starting schema fix...")
                
                # Drop foreign key constraints
                logger.info("Dropping foreign key constraints...")
                cur.execute("ALTER TABLE job_archive DROP CONSTRAINT IF EXISTS job_archive_company_id_fkey")
                cur.execute("ALTER TABLE snapshots_6h DROP CONSTRAINT IF EXISTS snapshots_6h_company_id_fkey")
                cur.execute("ALTER TABLE intelligence_events DROP CONSTRAINT IF EXISTS intelligence_events_company_id_fkey")
                
                # Rename old table
                cur.execute("ALTER TABLE IF EXISTS companies RENAME TO companies_old")
                
                # Create new clean table
                cur.execute("""
                    CREATE TABLE companies (
                        id SERIAL PRIMARY KEY,
                        company_name VARCHAR(255) NOT NULL UNIQUE,
                        company_name_token VARCHAR(255) UNIQUE,
                        ats_type VARCHAR(50),
                        board_url TEXT,
                        job_count INTEGER DEFAULT 0,
                        last_scraped TIMESTAMP,
                        created_at TIMESTAMP DEFAULT NOW(),
                        metadata JSONB
                    )
                """)
                
                # Migrate data
                logger.info("Migrating company data...")
                cur.execute("""
                    INSERT INTO companies (company_name, company_name_token, ats_type, board_url, job_count, last_scraped, created_at, metadata)
                    SELECT 
                        company_name,
                        COALESCE(company_name_token, token) as company_name_token,
                        ats_type,
                        COALESCE(board_url, careers_url) as board_url,
                        COALESCE(job_count, 0) as job_count,
                        COALESCE(last_scraped, last_updated) as last_scraped,
                        COALESCE(created_at, first_discovered, NOW()) as created_at,
                        metadata
                    FROM companies_old
                    ON CONFLICT (company_name) DO NOTHING
                """)
                migrated = cur.rowcount
                logger.info(f"Migrated {migrated} companies")
                
                # Create mapping table
                cur.execute("""
                    CREATE TEMP TABLE id_mapping AS
                    SELECT co.id::text as old_id, c.id as new_id, c.company_name
                    FROM companies_old co
                    JOIN companies c ON c.company_name = co.company_name
                """)
                
                # Update foreign keys
                logger.info("Updating job_archive references...")
                cur.execute("""
                    UPDATE job_archive ja
                    SET company_id = im.new_id
                    FROM id_mapping im
                    WHERE ja.company_id::text = im.old_id
                """)
                jobs_updated = cur.rowcount
                
                logger.info("Updating snapshots_6h references...")
                cur.execute("""
                    UPDATE snapshots_6h s
                    SET company_id = im.new_id
                    FROM id_mapping im
                    WHERE s.company_id::text = im.old_id
                """)
                snapshots_updated = cur.rowcount
                
                logger.info("Updating intelligence_events references...")
                cur.execute("""
                    UPDATE intelligence_events ie
                    SET company_id = im.new_id
                    FROM id_mapping im
                    WHERE ie.company_id::text = im.old_id
                """)
                events_updated = cur.rowcount

                logger.info("Converting company_id columns to INTEGER...")
                try:
                    cur.execute("ALTER TABLE job_archive ALTER COLUMN company_id TYPE INTEGER USING company_id::INTEGER")
                    cur.execute("ALTER TABLE snapshots_6h ALTER COLUMN company_id TYPE INTEGER USING company_id::INTEGER")
                    cur.execute("ALTER TABLE intelligence_events ALTER COLUMN company_id TYPE INTEGER USING company_id::INTEGER")
                    logger.info("✅ All company_id columns converted to INTEGER")
                except Exception as conv_error:
                    logger.error(f"Column type conversion failed: {conv_error}")
                    raise
                
                # Drop old table
                cur.execute("DROP TABLE companies_old CASCADE")
                
                # Recreate foreign key constraints
                logger.info("Recreating foreign key constraints...")
                cur.execute("""
                    ALTER TABLE job_archive 
                    ADD CONSTRAINT job_archive_company_id_fkey 
                    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
                """)
                
                cur.execute("""
                    ALTER TABLE snapshots_6h 
                    ADD CONSTRAINT snapshots_6h_company_id_fkey 
                    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
                """)
                
                cur.execute("""
                    ALTER TABLE intelligence_events 
                    ADD CONSTRAINT intelligence_events_company_id_fkey 
                    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
                """)
                
                conn.commit()
                logger.info(f"✅ Schema fixed! {migrated} companies, {jobs_updated} jobs, {snapshots_updated} snapshots, {events_updated} events")
        
        return jsonify({
            'success': True, 
            'message': f'Schema fixed! Migrated {migrated} companies, updated {jobs_updated} jobs, {snapshots_updated} snapshots, {events_updated} events.',
        }), 200
    except Exception as e:
        logger.error(f"Schema fix failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/admin/run-migrations', methods=['POST'])
@limiter.exempt
@require_admin_key
def run_migrations_endpoint():
    try:
        db = get_db()
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                logger.info("Running database migrations...")
                
                # Companies table - ALL missing columns
                cur.execute("ALTER TABLE companies ADD COLUMN IF NOT EXISTS board_url TEXT")
                cur.execute("ALTER TABLE companies ADD COLUMN IF NOT EXISTS last_scraped TIMESTAMP")
                cur.execute("ALTER TABLE companies ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()")
                cur.execute("ALTER TABLE companies ADD COLUMN IF NOT EXISTS metadata JSONB")
                
                # Intelligence events
                cur.execute("ALTER TABLE intelligence_events ADD COLUMN IF NOT EXISTS metadata JSONB")
                
                # Job archive columns
                cur.execute("ALTER TABLE job_archive ADD COLUMN IF NOT EXISTS location TEXT")
                cur.execute("ALTER TABLE job_archive ADD COLUMN IF NOT EXISTS department TEXT")
                cur.execute("ALTER TABLE job_archive ADD COLUMN IF NOT EXISTS work_type VARCHAR(50)")
                cur.execute("ALTER TABLE job_archive ADD COLUMN IF NOT EXISTS salary_min INTEGER")
                cur.execute("ALTER TABLE job_archive ADD COLUMN IF NOT EXISTS salary_max INTEGER")
                cur.execute("ALTER TABLE job_archive ADD COLUMN IF NOT EXISTS salary_currency VARCHAR(10)")
                cur.execute("ALTER TABLE job_archive ADD COLUMN IF NOT EXISTS closed_at TIMESTAMP")
                cur.execute("ALTER TABLE job_archive ADD COLUMN IF NOT EXISTS first_seen TIMESTAMP DEFAULT NOW()")
                cur.execute("ALTER TABLE job_archive ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP DEFAULT NOW()")
                
                # Snapshots
                cur.execute("ALTER TABLE snapshots_6h ADD COLUMN IF NOT EXISTS active_jobs INTEGER")
                cur.execute("ALTER TABLE snapshots_6h ADD COLUMN IF NOT EXISTS locations_count INTEGER")
                cur.execute("ALTER TABLE snapshots_6h ADD COLUMN IF NOT EXISTS departments_count INTEGER")
                
                # Seed companies
                cur.execute("ALTER TABLE seed_companies ADD COLUMN IF NOT EXISTS website_url TEXT")
                cur.execute("ALTER TABLE seed_companies ADD COLUMN IF NOT EXISTS times_tested INTEGER DEFAULT 0")
                cur.execute("ALTER TABLE seed_companies ADD COLUMN IF NOT EXISTS times_successful INTEGER DEFAULT 0")
                cur.execute("ALTER TABLE seed_companies ADD COLUMN IF NOT EXISTS last_tested_at TIMESTAMP")
                cur.execute("ALTER TABLE seed_companies ADD COLUMN IF NOT EXISTS success_rate DECIMAL(5,2) DEFAULT 0")
                cur.execute("ALTER TABLE seed_companies ADD COLUMN IF NOT EXISTS is_blacklisted BOOLEAN DEFAULT FALSE")
                
                conn.commit()
                logger.info("✅ All migrations complete!")
        
        return jsonify({'success': True, 'message': 'Database migrations completed'}), 200
    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500

# One-time database initialization flag
_db_initialized = False

def init_database_once():
    """Run database initialization once on startup"""
    global _db_initialized
    if _db_initialized:
        return
    
    try:
        logger.info("🔧 Running one-time database initialization...")
        db = get_db()
        
        # Check if indexes already exist (to avoid re-running)
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM pg_indexes 
                    WHERE indexname = 'idx_snapshots_time'
                """)
                if cur.fetchone()[0] > 0:
                    logger.info("✅ Database already initialized, skipping")
                    _db_initialized = True
                    return
        
        # Add indexes
        db.add_performance_indexes()
        logger.info("✅ Performance indexes created")
        
        # Cleanup old snapshots
        deleted = db.cleanup_old_snapshots(90)
        logger.info(f"✅ Cleaned up {deleted} old snapshots")
        
        _db_initialized = True
        logger.info("✅ Database initialization complete")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}", exc_info=True)

# Run on startup (but only once)
if os.getenv('RUN_DB_INIT', 'false').lower() == 'true':
    init_database_once()

# ============================================================================
# NOTE: integration.py routes NOT registered - upgrade endpoints are built 
# directly into app.py above. integration.py is kept for reference only.
# ============================================================================

# ============================================================================
# APPLICATION STARTUP
# ============================================================================
if __name__ == '__main__':
    logger.info("=" * 80)
    logger.info("🚀 Job Intelligence Platform Starting...")
    logger.info("=" * 80)
    
    db = get_db()
    stats = db.get_stats()
    logger.info(f"✅ Database connected: {stats['total_companies']} companies, {stats['total_jobs']} jobs")
    logger.info(f"📅 Trends cutoff date: {TRENDS_CUTOFF_DATE}")
    
    # Log upgrade module status
    logger.info("=" * 80)
    logger.info("🔌 UPGRADE MODULES STATUS:")
    logger.info(f"   - Config loaded: {UPGRADE_CONFIG_LOADED}")
    logger.info(f"   - Collector V7: {'✅ Available' if COLLECTOR_V7_AVAILABLE else '❌ Not found'}")
    logger.info(f"   - Mega Expander: {'✅ Available' if MEGA_EXPANDER_AVAILABLE else '❌ Not found'}")
    logger.info(f"   - Self-Growth: {'✅ Available' if SELF_GROWTH_AVAILABLE else '❌ Not found'}")
    logger.info("=" * 80)
    
    scheduler.start()
    logger.info(f"✅ Scheduler started with {len(scheduler.get_jobs())} jobs")
    
    logger.info(f"✅ Authentication enabled (API keys configured)")
    logger.info(f"✅ Rate limiting enabled (Redis)")
    
    logger.info("=" * 80)
    logger.info("✅ Application initialized successfully!")
    logger.info("=" * 80)
    
    port = int(os.getenv('PORT', 8080))
    logger.info(f"🌐 Starting server on port {port}...")
    logger.info(f"📦 Using Waitress WSGI server (production mode)")
    
    serve(app, host='0.0.0.0', port=port, threads=8)
