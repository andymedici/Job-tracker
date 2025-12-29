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
from apscheduler.jobstores.base import JobLookupError

from database import get_db
from collector import run_collection, run_refresh
from market_intel import run_daily_maintenance
from middleware.auth import AuthManager, require_api_key, require_admin_key, optional_auth
from middleware.rate_limit import setup_rate_limiter

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

scheduler.add_job(scheduled_refresh, CronTrigger(hour=6), id='refresh', replace_existing=True)
scheduler.add_job(scheduled_discovery, CronTrigger(hour=7), id='discovery', replace_existing=True)
scheduler.add_job(scheduled_tier1_expansion, CronTrigger(day_of_week='sun', hour=3), id='tier1_expansion', replace_existing=True)
scheduler.add_job(scheduled_tier2_expansion, CronTrigger(day=1, hour=4), id='tier2_expansion', replace_existing=True)

logger.info("📅 Scheduler configured:")
logger.info("   - Refresh: Daily at 6:00 AM UTC")
logger.info("   - Discovery: Daily at 7:00 AM UTC")
logger.info("   - Tier 1 Expansion: Weekly (Sunday 3:00 AM UTC)")
logger.info("   - Tier 2 Expansion: Monthly (1st at 4:00 AM UTC)")

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
        'endpoints': {
            'dashboard': '/dashboard',
            'analytics': '/analytics',
            'companies': '/companies',
            'jobs': '/jobs',
            'submit_seed': '/submit-seed',
            'seed_admin': '/seed-admin',
            'api_stats': '/api/stats',
            'api_intel': '/api/intel',
            'api_companies': '/api/companies',
            'api_jobs': '/api/jobs'
        }
    }), 200

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
                cur.execute("SELECT title FROM job_archive WHERE status = 'active' LIMIT 10")
                job_titles = [row[0] for row in cur.fetchall()]
                
                # Check locations
                cur.execute("SELECT DISTINCT location FROM job_archive WHERE status = 'active' AND location IS NOT NULL LIMIT 10")
                locations = [row[0] for row in cur.fetchall()]
                
                # Check departments
                cur.execute("SELECT DISTINCT department FROM job_archive WHERE status = 'active' AND department IS NOT NULL LIMIT 10")
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

@app.route('/api/analytics/advanced')
@app.route('/api/advanced-analytics')
@limiter.limit("30 per minute")
def get_advanced_analytics():
    """Get comprehensive advanced analytics"""
    try:
        db = get_db()
        analytics = db.get_advanced_analytics()
        
        logger.info(f"Analytics data keys: {analytics.keys() if analytics else 'None'}")
        
        if not analytics:
            return jsonify({
                'top_skills': {},
                'top_hiring_regions': {},
                'department_distribution': {},
                'work_type_distribution': {
                    'remote': 0,
                    'hybrid': 0,
                    'onsite': 0,
                    'remote_percent': 0,
                    'hybrid_percent': 0,
                    'onsite_percent': 0
                },
                'time_to_fill': {
                    'sample_size': 0,
                    'overall_avg_ttf_days': 0,
                    'median_ttf_days': 0,
                    'min_ttf_days': 0,
                    'max_ttf_days': 0,
                    'by_work_type': {},
                    'by_department': {}
                },
                'fastest_growing': [],
                'ats_distribution': [],
                'salary_insights': {},
                'top_companies': [],
                'recent_events': []
            }), 200
        
        return jsonify(analytics), 200
        
    except Exception as e:
        logger.error(f"Error getting advanced analytics: {e}", exc_info=True)
        return jsonify({
            'error': str(e),
            'top_skills': {},
            'top_hiring_regions': {},
            'department_distribution': {},
            'work_type_distribution': {
                'remote': 0,
                'hybrid': 0,
                'onsite': 0,
                'remote_percent': 0,
                'hybrid_percent': 0,
                'onsite_percent': 0
            },
            'time_to_fill': {
                'sample_size': 0,
                'overall_avg_ttf_days': 0,
                'median_ttf_days': 0,
                'min_ttf_days': 0,
                'max_ttf_days': 0,
                'by_work_type': {},
                'by_department': {}
            },
            'fastest_growing': [],
            'ats_distribution': [],
            'salary_insights': {},
            'top_companies': [],
            'recent_events': []
        }), 200

# ============================================================================
# STATS & INTELLIGENCE
# ============================================================================
@app.route('/api/stats')
@limiter.limit(RATE_LIMITS['authenticated_read'])
@require_api_key
def api_stats():
    try:
        db = get_db()
        stats = db.get_stats()
        
        # Add work type distribution
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        COALESCE(LOWER(work_type), 'unknown') as work_type,
                        COUNT(*) as count
                    FROM job_archive
                    WHERE status = 'active'
                    GROUP BY LOWER(work_type)
                """)
                work_types = {
                    'remote': 0,
                    'hybrid': 0,
                    'onsite': 0,
                    'unknown': 0
                }
                
                for row in cur.fetchall():
                    work_type = row[0] if row[0] else 'unknown'
                    count = row[1]
                    
                    # Map variations to standard types
                    if work_type == 'unknown' or work_type is None:
                        work_types['unknown'] += count
                    elif 'remote' in work_type:
                        work_types['remote'] += count
                    elif 'hybrid' in work_type:
                        work_types['hybrid'] += count
                    elif any(x in work_type for x in ['onsite', 'on-site', 'office', 'on site']):
                        work_types['onsite'] += count
                    else:
                        work_types['unknown'] += count
                
                stats['work_type_distribution'] = work_types
        
        stats.update({
            'is_running': collection_state['is_running'],
            'mode': collection_state['mode'],
            'current_progress': collection_state['current_progress'],
            'last_stats': collection_state['last_stats'],
            'last_run': collection_state['last_run']
        })
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
@limiter.limit(RATE_LIMITS['authenticated_read'])
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
                
                cur.execute("""
                    SELECT job_id, title, location, department, work_type, job_url, 
                           posted_date, salary_min, salary_max, salary_currency, status, first_seen, last_seen
                    FROM job_archive
                    WHERE company_id = %s
                    ORDER BY status, first_seen DESC
                """, (company_id,))
                
                jobs_columns = [desc[0] for desc in cur.description]
                jobs = [dict(zip(jobs_columns, row)) for row in cur.fetchall()]
                
                company_data['jobs'] = jobs
                
                return jsonify(company_data), 200
    except Exception as e:
        logger.error(f"Error getting company detail: {e}")
        return jsonify({'error': str(e)}), 500

# ============================================================================
# JOBS API
# ============================================================================
@app.route('/api/jobs')
@limiter.limit(RATE_LIMITS['authenticated_read'])
@require_api_key
def get_jobs_api():
    """Get all jobs with filters"""
    try:
        db = get_db()
        limit = int(request.args.get('limit', 5000))
        
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
                
                return jsonify({
                    'jobs': jobs,
                    'total_jobs': len(jobs),
                    'total_companies': total_companies
                }), 200
    except Exception as e:
        logger.error(f"Error getting jobs: {e}")
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
        logger.error(f"Error getting job detail: {e}")
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
        logger.error(f"Error getting seed stats: {e}")
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
        logger.error(f"Error adding seed: {e}")
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
        logger.error(f"Error resetting seeds: {e}")
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
        logger.error(f"Error unblacklisting seeds: {e}")
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
        logger.error(f"Error cleaning garbage seeds: {e}")
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
        logger.error(f"Error expanding Tier 1: {e}")
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
        logger.error(f"Error expanding Tier 2: {e}")
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
        logger.error(f"Error in nuclear reset: {e}")
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
# COLLECTION & REFRESH
# ============================================================================
@app.route('/api/collect', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
@require_api_key
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
        logger.error(f"Error in backfill: {e}")
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
