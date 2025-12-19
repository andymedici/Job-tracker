"""
Job Intelligence Platform - Production Flask Application
"""
import os
import asyncio
import threading
import logging
from datetime import datetime
from typing import Dict, Any

from flask import Flask, jsonify, render_template, request, g
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.triggers.cron import CronTrigger

# Import application modules (NO 'app.' prefix since files are in root)
from database import get_db
from collector import run_collection, run_refresh, JobIntelCollector
from market_intel import run_daily_maintenance
from seed_expander import run_tier1_expansion, run_tier2_expansion, run_full_expansion

# Import security middleware
from middleware.auth import require_api_key, require_admin_key, optional_auth, auth_manager
from middleware.rate_limit import create_limiter, RATE_LIMITS
from middleware.validators import (
    validate_request, SeedCreateRequest, CollectionRequest,
    RefreshRequest, TrendsRequest, IntelRequest, SeedExpansionRequest
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# Flask Application Setup
# ============================================================================

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max request size

# Force immediate template check
import sys
print("=" * 80, file=sys.stderr)
print("🔍 TEMPLATE CHECK ON STARTUP", file=sys.stderr)
print("=" * 80, file=sys.stderr)
print(f"Working dir: {os.getcwd()}", file=sys.stderr)
print(f"Root path: {app.root_path}", file=sys.stderr)
print(f"Template folder: {app.template_folder}", file=sys.stderr)

template_dir = os.path.join(app.root_path, 'templates')
if os.path.exists(template_dir):
    files = os.listdir(template_dir)
    print(f"✅ Templates found: {files}", file=sys.stderr)
else:
    print(f"❌ NO TEMPLATES AT: {template_dir}", file=sys.stderr)
    print(f"Directory contents: {os.listdir(app.root_path)}", file=sys.stderr)
    # Try to find templates anywhere
    for root, dirs, files in os.walk('/app'):
        if 'dashboard.html' in files:
            print(f"Found dashboard.html at: {root}", file=sys.stderr)

print("=" * 80, file=sys.stderr)

# CORS Configuration
CORS(app, resources={
    r"/api/*": {
        "origins": os.getenv('ALLOWED_ORIGINS', '*').split(','),
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "X-API-Key", "Authorization"]
    }
})

# Initialize rate limiter
limiter = create_limiter(app)

# Add Python builtins to Jinja2 templates
app.jinja_env.globals.update(max=max, min=min, len=len)

# ============================================================================
# Application State (Thread-Safe)
# ============================================================================

from threading import Lock
from dataclasses import dataclass, field

@dataclass
class CollectionState:
    """Thread-safe collection state management"""
    running: bool = False
    last_run: str = None
    last_stats: Dict[str, Any] = None
    last_intel: Dict[str, Any] = None
    current_progress: float = 0.0
    error: str = None
    mode: str = None
    _lock: Lock = field(default_factory=Lock, repr=False)
    
    def update(self, **kwargs):
        with self._lock:
            for key, value in kwargs.items():
                if hasattr(self, key) and not key.startswith('_'):
                    setattr(self, key, value)
    
    def get_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                'running': self.running,
                'last_run': self.last_run,
                'last_stats': self.last_stats,
                'last_intel': self.last_intel,
                'current_progress': self.current_progress,
                'error': self.error,
                'mode': self.mode
            }

collection_state = CollectionState()

def progress_callback(progress: float, stats: Dict[str, Any]):
    """Thread-safe progress callback"""
    collection_state.update(
        current_progress=progress,
        last_stats=stats
    )

# ============================================================================
# Scheduler Setup with PostgreSQL Persistence
# ============================================================================

def get_scheduler():
    """Initialize APScheduler with PostgreSQL jobstore"""
    jobstores = {
        'default': SQLAlchemyJobStore(url=os.getenv('DATABASE_URL'))
    }
    executors = {
        'default': ThreadPoolExecutor(10)
    }
    job_defaults = {
        'coalesce': False,
        'max_instances': 1,
        'misfire_grace_time': 300  # 5 minutes
    }
    
    scheduler = BackgroundScheduler(
        jobstores=jobstores,
        executors=executors,
        job_defaults=job_defaults,
        timezone=os.getenv('SCHEDULER_TIMEZONE', 'UTC')
    )
    return scheduler

scheduler = get_scheduler()

# ============================================================================
# Scheduled Jobs with Distributed Locking
# ============================================================================

def scheduled_refresh():
    """Scheduled refresh with distributed locking"""
    db = get_db()
    
    # Acquire distributed lock
    if not db.acquire_advisory_lock('scheduled_refresh'):
        logger.info("Another instance is running scheduled refresh")
        return
    
    try:
        if collection_state.get_dict()['running']:
            logger.info("Refresh already running locally, skipping")
            return
        
        collection_state.update(
            running=True,
            mode='refresh',
            current_progress=0.0,
            error=None,
            last_run=datetime.utcnow().isoformat()
        )
        
        logger.info("🔄 Starting scheduled refresh...")
        stats = asyncio.run(run_refresh(hours_since_update=6, max_companies=500))
        collection_state.update(last_stats=stats.to_dict() if hasattr(stats, 'to_dict') else stats)
        logger.info("✅ Scheduled refresh complete")
        
    except Exception as e:
        logger.error(f"❌ Scheduled refresh failed: {e}", exc_info=True)
        collection_state.update(error=str(e))
    finally:
        collection_state.update(running=False, current_progress=100)
        run_daily_maintenance()
        db.release_advisory_lock('scheduled_refresh')

def scheduled_discovery():
    """Scheduled discovery with distributed locking"""
    db = get_db()
    
    if not db.acquire_advisory_lock('scheduled_discovery'):
        logger.info("Another instance is running scheduled discovery")
        return
    
    try:
        if collection_state.get_dict()['running']:
            return
        
        collection_state.update(
            running=True,
            mode='discovery',
            current_progress=0.0,
            error=None,
            last_run=datetime.utcnow().isoformat()
        )
        
        logger.info("🔍 Starting scheduled discovery...")
        collector = JobIntelCollector(progress_callback=progress_callback)
        stats = asyncio.run(collector.run_discovery(max_companies=500))
        collection_state.update(last_stats=stats.to_dict())
        logger.info("✅ Scheduled discovery complete")
        
    except Exception as e:
        logger.error(f"❌ Scheduled discovery failed: {e}", exc_info=True)
        collection_state.update(error=str(e))
    finally:
        collection_state.update(running=False, current_progress=100)
        run_daily_maintenance()
        db.release_advisory_lock('scheduled_discovery')

def scheduled_tier1_expansion():
    """Scheduled Tier 1 seed expansion"""
    db = get_db()
    
    if not db.acquire_advisory_lock('scheduled_tier1_expansion'):
        return
    
    try:
        collection_state.update(
            running=True,
            mode='expansion_tier1',
            current_progress=0.0,
            last_run=datetime.utcnow().isoformat()
        )
        
        logger.info("🌱 Starting Tier 1 seed expansion...")
        asyncio.run(run_tier1_expansion())
        logger.info("✅ Tier 1 expansion complete")
        
    except Exception as e:
        logger.error(f"❌ Tier 1 expansion failed: {e}", exc_info=True)
        collection_state.update(error=str(e))
    finally:
        collection_state.update(running=False, current_progress=100)
        db.release_advisory_lock('scheduled_tier1_expansion')

def scheduled_tier2_expansion():
    """Scheduled Tier 2 seed expansion"""
    db = get_db()
    
    if not db.acquire_advisory_lock('scheduled_tier2_expansion'):
        return
    
    try:
        collection_state.update(
            running=True,
            mode='expansion_tier2',
            current_progress=0.0,
            last_run=datetime.utcnow().isoformat()
        )
        
        logger.info("🌳 Starting Tier 2 seed expansion...")
        asyncio.run(run_tier2_expansion())
        logger.info("✅ Tier 2 expansion complete")
        
    except Exception as e:
        logger.error(f"❌ Tier 2 expansion failed: {e}", exc_info=True)
        collection_state.update(error=str(e))
    finally:
        collection_state.update(running=False, current_progress=100)
        db.release_advisory_lock('scheduled_tier2_expansion')

# Register scheduled jobs - UPDATED SCHEDULE
# Refresh: Once per day at 6 AM
scheduler.add_job(scheduled_refresh, CronTrigger(hour=6), id='refresh', replace_existing=True)

# Discovery: DAILY at 7 AM (for first 6 months, then can reduce)
scheduler.add_job(scheduled_discovery, CronTrigger(hour=7), id='discovery', replace_existing=True)

# Tier 1 Seed Expansion: Weekly on Sunday at 3 AM
scheduler.add_job(scheduled_tier1_expansion, CronTrigger(day_of_week='sun', hour=3), id='tier1_expansion', replace_existing=True)

# Tier 2 Seed Expansion: Monthly on 1st at 4 AM
scheduler.add_job(scheduled_tier2_expansion, CronTrigger(day=1, hour=4), id='tier2_expansion', replace_existing=True)

logger.info("📅 Scheduler configured:")
logger.info("    - Refresh: Daily at 6:00 AM UTC")
logger.info("    - Discovery: Daily at 7:00 AM UTC")
logger.info("    - Tier 1 Expansion: Weekly (Sunday 3:00 AM UTC)")
logger.info("    - Tier 2 Expansion: Monthly (1st at 4:00 AM UTC)")

# ============================================================================
# Web Routes (Public)
# ============================================================================

@app.route('/api/admin/run-migrations', methods=['POST'])
@limiter.exempt
@require_admin_key
def run_migrations_endpoint():
    """
    Run database migrations to add missing columns.
    NOTE: This is largely redundant if database.py's _create_tables runs correctly.
    Keeping for manual admin trigger, but only running simplified migration check.
    """
    try:
        db = get_db()
        
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                logger.info("Running database migrations...")
                
                # Check 1: Add metadata column to intelligence_events if missing
                cur.execute("""
                    ALTER TABLE intelligence_events  
                    ADD COLUMN IF NOT EXISTS metadata JSONB
                """)
                logger.info("✅ Checked/Added metadata column to intelligence_events")
                
                # Check 2: Verify job_archive has location column
                cur.execute("""
                    ALTER TABLE job_archive  
                    ADD COLUMN IF NOT EXISTS location TEXT
                """)
                logger.info("✅ Checked/Added location column to job_archive")
                
                # Check 3: Verify job_archive has closed_at column (The key fix from last session)
                cur.execute("""
                    ALTER TABLE job_archive  
                    ADD COLUMN IF NOT EXISTS closed_at TIMESTAMP
                """)
                logger.info("✅ Checked/Added closed_at column to job_archive")
                
                conn.commit()
                logger.info("✅ All migrations complete!")
        
        return jsonify({
            'success': True,
            'message': 'Database migrations completed successfully'
        }), 200
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/')
@app.route('/dashboard')
@optional_auth
def dashboard():
    """Main dashboard page"""
    return render_template('dashboard.html')

@app.route('/analytics')
@optional_auth
def analytics():
    """Analytics dashboard page"""
    return render_template('analytics.html')

@app.route('/health')
@limiter.exempt
def health():
    """Health check endpoint for monitoring"""
    try:
        db = get_db()
        # NOTE: db.get_stats() MUST exist in database.py
        stats = db.get_stats() 
        
        scheduler_running = scheduler.running
        job_count = len(scheduler.get_jobs()) if scheduler_running else 0
        
        health_data = {
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat(),
            'database': 'ok',
            'scheduler': 'running' if scheduler_running else 'stopped',
            'scheduled_jobs': job_count,
            'collection_running': collection_state.get_dict()['running'],
            'total_companies': stats.get('total_companies', 0),
            'total_jobs': stats.get('total_jobs', 0)
        }
        
        return jsonify(health_data), 200
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 503

@app.route('/ready')
@limiter.exempt
def ready():
    """Readiness probe for Railway"""
    try:
        db = get_db()
        # NOTE: db.get_stats() MUST exist in database.py
        db.get_stats()
        return jsonify({'status': 'ready'}), 200
    except Exception as e:
        return jsonify({'status': 'not_ready', 'error': str(e)}), 503

@app.route('/submit-seed')
@require_admin_key
def submit_seed_page():
    """Manual seed submission page (Admin only)"""
    return render_template('submit-seed.html')

# ============================================================================
# API Routes - Statistics (Read)
# ============================================================================

@app.route('/api/stats')
@limiter.limit(RATE_LIMITS['authenticated_read'])
@require_api_key
def api_stats():
    """Get dashboard statistics"""
    try:
        db = get_db()
        stats = db.get_stats()
        state = collection_state.get_dict()
        
        stats.update({
            'last_updated': datetime.utcnow().isoformat(),
            'is_running': state['running'],
            'current_progress': state['current_progress'],
            'mode': state['mode'],
            'last_run': state['last_run'],
            'last_error': state['error'],
            'last_stats': state['last_stats'],
            'last_intel': state['last_intel'],
        })
        
        return jsonify(stats), 200
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/trends')
@limiter.limit(RATE_LIMITS['authenticated_read'])
@require_api_key
def api_trends():
    """Get market trends"""
    validated, error = validate_request(TrendsRequest, {
        'days': request.args.get('days', 7, type=int)
    })
    
    if error:
        return jsonify(error), 400
    
    try:
        db = get_db()
        # NOTE: db.get_market_trends() MUST exist in database.py
        granular = db.get_market_trends(days=validated.days)
        monthly = db.get_monthly_snapshots()
        
        return jsonify({
            'granular': granular,
            'monthly': monthly
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting trends: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/intel')
@limiter.limit(RATE_LIMITS['authenticated_read'])
@require_api_key
def api_intel():
    """Get intelligence data (surges, declines, expansions)"""
    validated, error = validate_request(IntelRequest, {
        'days': request.args.get('days', 7, type=int)
    })
    
    if error:
        return jsonify(error), 400
    
    try:
        db = get_db()
        surges, declines = db.get_job_count_changes(days=validated.days)
        # NOTE: db.get_location_expansions() MUST exist in database.py
        expansions = db.get_location_expansions(days=validated.days)
        
        return jsonify({
            'surges': surges,
            'declines': declines,
            'expansions': expansions
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting intelligence: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/advanced-analytics')
@limiter.limit(RATE_LIMITS['authenticated_read'])
@require_api_key
def api_advanced_analytics():
    """Get comprehensive analytics"""
    try:
        db = get_db()
        # NOTE: db.get_advanced_analytics() MUST exist in database.py
        analytics = db.get_advanced_analytics()
        return jsonify(analytics), 200
        
    except Exception as e:
        logger.error(f"Error getting analytics: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

# Add these routes after your existing dashboard/analytics routes

@app.route('/api/companies')
@limiter.limit(RATE_LIMITS['authenticated_read'])
@require_api_key
def api_companies():
    """Get list of tracked companies with job counts"""
    try:
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 50, type=int), 100)
        search = request.args.get('search', '').strip()
        ats_type = request.args.get('ats_type', '').strip()
        
        db = get_db()
        
        # Build query - FIXED to handle missing jobs
        query = """
            SELECT 
                c.id,
                c.company_name,
                c.ats_type,
                c.board_url,
                c.job_count,
                c.last_scraped,
                c.created_at,
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
        
        query += """
            ORDER BY c.job_count DESC NULLS LAST, c.company_name
            LIMIT %s OFFSET %s
        """
        params.extend([per_page, (page - 1) * per_page])
        
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                companies = [dict(zip(columns, row)) for row in cur.fetchall()]
                
                # Get total count
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
        return jsonify({'error': str(e), 'details': 'Check if companies table exists'}), 500
@app.route('/company/<int:company_id>')
@optional_auth
def company_page(company_id):
    """Single company detail page"""
    return render_template('company.html')

@app.route('/jobs')
@optional_auth
def jobs_page():
    """Jobs browser page"""
    return render_template('jobs.html')

# ============================================================================
# API Routes - Job Browsing & Company Details
# ============================================================================

@app.route('/api/companies/<int:company_id>')
@limiter.limit(RATE_LIMITS['authenticated_read'])
@require_api_key
def api_company_detail(company_id):
    """Get detailed company information with jobs"""
    try:
        db = get_db()
        
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                # Get company info
                cur.execute("""
                    SELECT 
                        id, company_name, ats_type, board_url, job_count,
                        last_scraped, created_at
                    FROM companies
                    WHERE id = %s
                """, (company_id,))
                
                company = cur.fetchone()
                if not company:
                    return jsonify({'error': 'Company not found'}), 404
                
                columns = [desc[0] for desc in cur.description]
                company_data = dict(zip(columns, company))
                
                # Get jobs for this company - FIXED TABLE NAME
                cur.execute("""
                    SELECT 
                        id, title, location, department, work_type,
                        job_url, posted_date, status, created_at
                    FROM job_archive -- <-- FIX: Changed 'jobs' to 'job_archive'
                    WHERE company_id = %s
                    ORDER BY 
                        CASE WHEN status = 'active' THEN 0 ELSE 1 END,
                        created_at DESC
                    LIMIT 1000
                """, (company_id,))
                
                jobs_columns = [desc[0] for desc in cur.description]
                jobs = [dict(zip(jobs_columns, row)) for row in cur.fetchall()]
                
                # Get job stats - FIXED TABLE NAME
                cur.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN status = 'active' THEN 1 END) as active,
                        COUNT(CASE WHEN status = 'closed' THEN 1 END) as closed,
                        COUNT(DISTINCT department) as departments,
                        COUNT(DISTINCT location) as locations
                    FROM job_archive -- <-- FIX: Changed 'jobs' to 'job_archive'
                    WHERE company_id = %s
                """, (company_id,))
                
                stats = dict(zip([desc[0] for desc in cur.description], cur.fetchone()))
        
        return jsonify({
            'company': company_data,
            'jobs': jobs,
            'stats': stats
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting company detail: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/jobs')
@limiter.limit(RATE_LIMITS['authenticated_read'])
@require_api_key
def api_jobs():
    """Search and filter jobs"""
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
        
        # Build query - FIXED TABLE NAME
        query = """
            SELECT 
                j.id, j.title, j.location, j.department, j.work_type,
                j.job_url, j.posted_date, j.status, j.created_at,
                c.company_name, c.ats_type
            FROM job_archive j -- <-- FIX: Changed 'jobs' to 'job_archive'
            JOIN companies c ON j.company_id = c.id
            WHERE 1=1
        """
        params = []
        
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
            query += " AND j.work_type = %s"
            params.append(work_type)
        
        if location:
            query += " AND j.location ILIKE %s"
            params.append(f'%{location}%')
        
        if status:
            query += " AND j.status = %s"
            params.append(status)
        
        query += """
            ORDER BY j.created_at DESC
            LIMIT %s OFFSET %s
        """
        params.extend([per_page, (page - 1) * per_page])
        
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                jobs = [dict(zip(columns, row)) for row in cur.fetchall()]
                
                # Get total count - FIXED TABLE NAME
                count_query = """
                    SELECT COUNT(*) 
                    FROM job_archive j -- <-- FIX: Changed 'jobs' to 'job_archive'
                    JOIN companies c ON j.company_id = c.id
                    WHERE 1=1
                """
                count_params = []
                if search:
                    count_query += " AND (j.title ILIKE %s OR j.department ILIKE %s)"
                    count_params.extend([f'%{search}%', f'%{search}%'])
                if company_id:
                    count_query += " AND j.company_id = %s"
                    count_params.append(company_id)
                if department:
                    count_query += " AND j.department ILIKE %s"
                    count_params.append(f'%{department}%')
                if work_type:
                    count_query += " AND j.work_type = %s"
                    count_params.append(work_type)
                if location:
                    count_query += " AND j.location ILIKE %s"
                    count_params.append(f'%{location}%')
                if status:
                    count_query += " AND j.status = %s"
                    count_params.append(status)
                
                cur.execute(count_query, count_params)
                total = cur.fetchone()[0]
        
        return jsonify({
            'jobs': jobs,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'pages': (total + per_page - 1) // per_page
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting jobs: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/jobs/<int:job_id>')
@limiter.limit(RATE_LIMITS['authenticated_read'])
@require_api_key
def api_job_detail(job_id):
    """Get detailed job information"""
    try:
        db = get_db()
        
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                # FIXED TABLE NAME
                cur.execute("""
                    SELECT 
                        j.*,
                        c.company_name, c.ats_type, c.board_url
                    FROM job_archive j -- <-- FIX: Changed 'jobs' to 'job_archive'
                    JOIN companies c ON j.company_id = c.id
                    WHERE j.id = %s
                """, (job_id,))
                
                job = cur.fetchone()
                if not job:
                    return jsonify({'error': 'Job not found'}), 404
                
                columns = [desc[0] for desc in cur.description]
                job_data = dict(zip(columns, job))
        
        return jsonify(job_data), 200
        
    except Exception as e:
        logger.error(f"Error getting job detail: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/debug/tables')
@limiter.exempt
def debug_tables():
    """Debug endpoint to check table status"""
    try:
        db = get_db()
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                # Check companies
                cur.execute("SELECT COUNT(*) FROM companies")
                company_count = cur.fetchone()[0]
                
                # Check seeds
                cur.execute("SELECT COUNT(*) FROM seed_companies")
                seed_count = cur.fetchone()[0]
                
                # Check if job_archive exists
                try:
                    cur.execute("SELECT COUNT(*) FROM job_archive")
                    job_count = cur.fetchone()[0]
                except:
                    # NOTE: If this fails, it's a critical DB setup failure
                    job_count = "Table doesn't exist (job_archive)"
                
                return jsonify({
                    'companies': company_count,
                    'seeds': seed_count,
                    'jobs': job_count,
                    'status': 'ok'
                })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================================================
# API Routes - Seeds Management
# ============================================================================

@app.route('/api/seeds/manual', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
@require_admin_key
def api_manual_seed():
    """Submit a manual seed company (Admin only)"""
    data = request.get_json() or {}
    
    company_name = data.get('company_name', '').strip()
    website_url = data.get('website_url', '').strip()
    ats_hint = data.get('ats_hint', '').strip().lower()
    
    if not company_name:
        return jsonify({'error': 'Company name is required'}), 400
    
    try:
        db = get_db()
        
        # Add as manual seed
        added = db.add_manual_seed(company_name, website_url=website_url)
        
        if not added:
            return jsonify({'error': 'Company already exists in seeds or tracked companies'}), 409
        
        # If website URL provided, optionally test immediately
        if website_url and data.get('test_immediately', False):
            # Trigger immediate test in background
            def test_seed():
                collector = JobIntelCollector()
                asyncio.run(collector.test_single_company(company_name, website_url, ats_hint))
            
            threading.Thread(target=test_seed, daemon=True).start()
            
            return jsonify({
                'success': True,
                'message': f'Added {company_name} and testing immediately',
                'company_name': company_name
            }), 201
            
        return jsonify({
            'success': True,
            'message': f'Added {company_name} to seed database',
            'company_name': company_name
        }), 201
        
    except Exception as e:
        logger.error(f"Error adding manual seed: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/seeds', methods=['GET'])
@limiter.limit(RATE_LIMITS['authenticated_read'])
@require_api_key
def api_seeds_get():
    """Get seed companies"""
    limit = request.args.get('limit', 100, type=int)
    limit = min(max(limit, 1), 500)  # Clamp between 1-500
    
    try:
        db = get_db()
        seeds = db.get_seeds(limit=limit)
        return jsonify({'seeds': seeds, 'count': len(seeds)}), 200
        
    except Exception as e:
        logger.error(f"Error getting seeds: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500
