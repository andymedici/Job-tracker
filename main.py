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
        stats = asyncio.run(run_collection(max_companies=500))
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
        added = run_tier1_expansion()
        logger.info(f"Tier 1 expansion complete: {added} seeds added")
    finally:
        get_db().release_advisory_lock('tier1_expansion')

def scheduled_tier2_expansion():
    if not get_db().acquire_advisory_lock('tier2_expansion'):
        return
    try:
        logger.info("Starting Tier 2 seed expansion")
        from seed_expander import run_tier2_expansion
        added = run_tier2_expansion()
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
            'api_stats': '/api/stats',
            'api_intel': '/api/intel',
            'api_companies': '/api/companies',
            'api_jobs': '/api/jobs'
        }
    }), 200

@app.route('/dashboard')
@optional_auth
def dashboard():
    return render_template('dashboard.html')

@app.route('/analytics')
@optional_auth
def analytics():
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

@app.route('/api/stats')
@limiter.limit(RATE_LIMITS['authenticated_read'])
@require_api_key
def api_stats():
    try:
        db = get_db()
        stats = db.get_stats()
        stats.update({
            'is_running': collection_state['is_running'],
            'mode': collection_state['mode'],
            'current_progress': collection_state['current_progress'],
            'last_stats': collection_state['last_stats'],
            'last_run': collection_state['last_run']
        })
        return jsonify(stats), 200
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
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

@app.route('/api/advanced-analytics')
@limiter.limit(RATE_LIMITS['authenticated_read'])
@require_api_key
def api_advanced_analytics():
    try:
        db = get_db()
        analytics = db.get_advanced_analytics()
        return jsonify(analytics), 200
    except Exception as e:
        logger.error(f"Error getting advanced analytics: {e}")
        return jsonify({'error': str(e)}), 500

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
@require_api_key
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
                    SELECT id, job_id, title, location, department, work_type, job_url, 
                           posted_date, salary_min, salary_max, salary_currency, status, first_seen, last_seen
                    FROM job_archive
                    WHERE company_id = %s
                    ORDER BY status, first_seen DESC
                    LIMIT 1000
                """, (company_id,))
                
                jobs_columns = [desc[0] for desc in cur.description]
                jobs = [dict(zip(jobs_columns, row)) for row in cur.fetchall()]
                
                company_data['jobs'] = jobs
                
                return jsonify(company_data), 200
    except Exception as e:
        logger.error(f"Error getting company detail: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/jobs')
@limiter.limit(RATE_LIMITS['authenticated_read'])
@require_api_key
def api_jobs():
    try:
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 50, type=int), 100)
        search = request.args.get('search', '').strip()
        company_id = request.args.get('company_id', type=int)
        department = request.args.get('department', '').strip()
        work_type = request.args.get('work_type', '').strip()
        location = request.args.get('location', '').strip()
        status = request.args.get('status', 'active').strip()
        
        db = get_db()
        query = """
            SELECT 
                j.id, j.job_id, j.title, j.location, j.department, j.work_type,
                j.job_url, j.posted_date, j.salary_min, j.salary_max, j.salary_currency,
                j.status, j.first_seen, j.last_seen,
                c.company_name, c.ats_type
            FROM job_archive j
            JOIN companies c ON j.company_id = c.id
            WHERE 1=1
        """
        params = []
        
        if status:
            query += " AND j.status = %s"
            params.append(status)
        
        if search:
            query += " AND (j.title ILIKE %s OR j.department ILIKE %s)"
            params.extend([f'%{search}%', f'%{search}%'])
        
        if company_id:
            query += " AND j.company_id = %s"
            params.append(company_id)
        
        if department:
            query += " AND j.department ILIKE %s"
            params.append(f'%{department}%')
        
        if work_type:
            query += " AND j.work_type ILIKE %s"
            params.append(f'%{work_type}%')
        
        if location:
            query += " AND j.location ILIKE %s"
            params.append(f'%{location}%')
        
        query += " ORDER BY j.first_seen DESC LIMIT %s OFFSET %s"
        params.extend([per_page, (page - 1) * per_page])
        
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                jobs = [dict(zip(columns, row)) for row in cur.fetchall()]
                
                count_query = "SELECT COUNT(*) FROM job_archive j WHERE 1=1"
                count_params = []
                if status:
                    count_query += " AND j.status = %s"
                    count_params.append(status)
                if search:
                    count_query += " AND (j.title ILIKE %s OR j.department ILIKE %s)"
                    count_params.extend([f'%{search}%', f'%{search}%'])
                if company_id:
                    count_query += " AND j.company_id = %s"
                    count_params.append(company_id)
                
                cur.execute(count_query, count_params)
                total = cur.fetchone()[0]
        
        return jsonify({
            'jobs': jobs,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'pages': (total + per_page - 1) // per_page if total > 0 else 1
            }
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
                    SELECT j.*, c.company_name, c.ats_type, c.board_url
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

@app.route('/api/seeds/manual', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
@require_admin_key
def api_manual_seed():
    data = request.get_json() or {}
    company_name = data.get('company_name', '').strip()
    website_url = data.get('website_url', '').strip()
    ats_hint = data.get('ats_hint', '').strip().lower()
    
    if not company_name:
        return jsonify({'error': 'Company name is required'}), 400
    
    try:
        db = get_db()
        added = db.add_manual_seed(company_name, website_url=website_url)
        
        if not added:
            return jsonify({'error': 'Company already exists'}), 409
        
        if website_url and data.get('test_immediately', False):
            def test_seed():
                from collector import JobIntelCollector
                collector = JobIntelCollector()
                asyncio.run(collector._test_company(company_name, ats_hint))
            
            threading.Thread(target=test_seed, daemon=True).start()
            return jsonify({
                'success': True,
                'message': f'Added {company_name} and testing',
                'company_name': company_name
            }), 201
        
        return jsonify({
            'success': True,
            'message': f'Added {company_name} to seeds',
            'company_name': company_name
        }), 201
    except Exception as e:
        logger.error(f"Error adding manual seed: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/collect', methods=['POST'])
@limiter.limit(RATE_LIMITS['admin'])
@require_admin_key
def api_collect():
    if collection_state['is_running']:
        return jsonify({'error': 'Collection already running'}), 409
    
    data = request.get_json() or {}
    max_companies = min(data.get('max_companies', 500), 1000)
    
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
@limiter.limit(RATE_LIMITS['admin'])
@require_admin_key
def api_refresh():
    if collection_state['is_running']:
        return jsonify({'error': 'Collection already running'}), 409
    
    data = request.get_json() or {}
    hours_since_update = data.get('hours_since_update', 24)
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
        finally:
            collection_state['is_running'] = False
            collection_state['mode'] = None
    
    thread = threading.Thread(target=run_refresh_thread, daemon=True)
    thread.start()
    
    return jsonify({'message': f'Refresh started for {max_companies} companies'}), 202

@app.route('/api/expand-seeds', methods=['POST'])
@limiter.limit(RATE_LIMITS['admin'])
@require_admin_key
def api_expand_seeds():
    data = request.get_json() or {}
    tier = data.get('tier', 'tier1')
    
    def run_expansion():
        try:
            if tier == 'tier1':
                from seed_expander import run_tier1_expansion
                added = run_tier1_expansion()
            elif tier == 'tier2':
                from seed_expander import run_tier2_expansion
                added = run_tier2_expansion()
            else:
                from seed_expander import run_full_expansion
                added = run_full_expansion()
            logger.info(f"Seed expansion complete: {added} seeds added")
        except Exception as e:
            logger.error(f"Seed expansion failed: {e}")
    
    thread = threading.Thread(target=run_expansion, daemon=True)
    thread.start()
    
    return jsonify({'message': f'Seed expansion ({tier}) started'}), 202

@app.route('/api/admin/run-migrations', methods=['POST'])
@limiter.exempt
@require_admin_key
def run_migrations_endpoint():
    try:
        db = get_db()
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                logger.info("Running database migrations...")
                
                # Companies table columns
                cur.execute("ALTER TABLE companies ADD COLUMN IF NOT EXISTS board_url TEXT")
                cur.execute("ALTER TABLE companies ADD COLUMN IF NOT EXISTS metadata JSONB")
                
                # Intelligence events
                cur.execute("ALTER TABLE intelligence_events ADD COLUMN IF NOT EXISTS metadata JSONB")
                
                # Job archive columns
                cur.execute("ALTER TABLE job_archive ADD COLUMN IF NOT EXISTS location TEXT")
                cur.execute("ALTER TABLE job_archive ADD COLUMN IF NOT EXISTS salary_min INTEGER")
                cur.execute("ALTER TABLE job_archive ADD COLUMN IF NOT EXISTS salary_max INTEGER")
                cur.execute("ALTER TABLE job_archive ADD COLUMN IF NOT EXISTS salary_currency VARCHAR(10)")
                cur.execute("ALTER TABLE job_archive ADD COLUMN IF NOT EXISTS closed_at TIMESTAMP")
                
                # Snapshots
                cur.execute("ALTER TABLE snapshots_6h ADD COLUMN IF NOT EXISTS active_jobs INTEGER")
                
                # Seed companies
                cur.execute("""
                    ALTER TABLE seed_companies 
                    ADD COLUMN IF NOT EXISTS website_url TEXT,
                    ADD COLUMN IF NOT EXISTS times_tested INTEGER DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS times_successful INTEGER DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS last_tested_at TIMESTAMP,
                    ADD COLUMN IF NOT EXISTS success_rate DECIMAL(5,2) DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS is_blacklisted BOOLEAN DEFAULT FALSE
                """)
                
                conn.commit()
                logger.info("✅ All migrations complete!")
        
        return jsonify({'success': True, 'message': 'Database migrations completed'}), 200
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
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
