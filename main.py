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

# After these lines:
# app = Flask(__name__)
# CORS(app, resources={r"/api/*": {"origins": "*"}})

# ADD THIS:
import os
import sys

# Force immediate template check
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
logger.info("   - Refresh: Daily at 6:00 AM UTC")
logger.info("   - Discovery: Daily at 7:00 AM UTC")
logger.info("   - Tier 1 Expansion: Weekly (Sunday 3:00 AM UTC)")
logger.info("   - Tier 2 Expansion: Monthly (1st at 4:00 AM UTC)")

# ============================================================================
# Web Routes (Public)
# ============================================================================

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
        
        # Build query
        query = """
            SELECT 
                c.id,
                c.company_name,
                c.ats_type,
                c.board_url,
                c.job_count,
                c.last_scraped,
                c.created_at,
                COUNT(j.id) as total_jobs_all_time,
                COUNT(CASE WHEN j.status = 'active' THEN 1 END) as active_jobs
            FROM companies c
            LEFT JOIN jobs j ON c.id = j.company_id
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
            GROUP BY c.id
            ORDER BY c.job_count DESC, c.company_name
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
                'pages': (total + per_page - 1) // per_page
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting companies: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

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
                
                # Get jobs for this company
                cur.execute("""
                    SELECT 
                        id, title, location, department, work_type,
                        job_url, posted_date, status, created_at
                    FROM jobs
                    WHERE company_id = %s
                    ORDER BY 
                        CASE WHEN status = 'active' THEN 0 ELSE 1 END,
                        created_at DESC
                    LIMIT 1000
                """, (company_id,))
                
                jobs_columns = [desc[0] for desc in cur.description]
                jobs = [dict(zip(jobs_columns, row)) for row in cur.fetchall()]
                
                # Get job stats
                cur.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN status = 'active' THEN 1 END) as active,
                        COUNT(CASE WHEN status = 'closed' THEN 1 END) as closed,
                        COUNT(DISTINCT department) as departments,
                        COUNT(DISTINCT location) as locations
                    FROM jobs
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
        
        query = """
            SELECT 
                j.id, j.title, j.location, j.department, j.work_type,
                j.job_url, j.posted_date, j.status, j.created_at,
                c.company_name, c.ats_type
            FROM jobs j
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
                
                # Get total count
                count_query = """
                    SELECT COUNT(*) 
                    FROM jobs j
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
                cur.execute("""
                    SELECT 
                        j.*,
                        c.company_name, c.ats_type, c.board_url
                    FROM jobs j
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

@app.route('/api/seeds', methods=['POST'])
@limiter.limit(RATE_LIMITS['write'])
@require_admin_key
def api_seeds_post():
    """Add manual seed companies (Admin only)"""
    data = request.get_json() or {}
    validated, error = validate_request(SeedCreateRequest, data)
    
    if error:
        return jsonify(error), 400
    
    try:
        db = get_db()
        added = sum(1 for name in validated.companies if db.add_manual_seed(name))
        
        logger.info(f"Added {added}/{len(validated.companies)} manual seeds")
        
        return jsonify({
            'added': added,
            'total_submitted': len(validated.companies),
            'success': True
        }), 201
        
    except Exception as e:
        logger.error(f"Error adding seeds: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

# ============================================================================
# API Routes - Collection Operations (Admin Only)
# ============================================================================

@app.route('/api/collect', methods=['POST'])
@limiter.limit(RATE_LIMITS['expensive'])
@require_admin_key
def api_collect():
    """Start company discovery collection (Admin only)"""
    if collection_state.get_dict()['running']:
        return jsonify({'error': 'Collection already running'}), 409
    
    data = request.get_json() or {}
    validated, error = validate_request(CollectionRequest, data)
    
    if error:
        return jsonify(error), 400
    
    def run():
        collection_state.update(
            running=True,
            mode='discovery',
            current_progress=0.0,
            error=None,
            last_run=datetime.utcnow().isoformat()
        )
        
        try:
            collector = JobIntelCollector(progress_callback=progress_callback)
            stats = asyncio.run(collector.run_discovery(max_companies=validated.max_companies))
            collection_state.update(last_stats=stats.to_dict())
            logger.info(f"✅ Manual collection complete: {stats.to_dict()}")
            
        except Exception as e:
            logger.error(f"❌ Manual collection failed: {e}", exc_info=True)
            collection_state.update(error=str(e))
        finally:
            collection_state.update(running=False, current_progress=100)
            run_daily_maintenance()
    
    threading.Thread(target=run, daemon=True).start()
    
    return jsonify({
        'status': 'started',
        'message': 'Company discovery initiated',
        'max_companies': validated.max_companies
    }), 202

@app.route('/api/refresh', methods=['POST'])
@limiter.limit(RATE_LIMITS['expensive'])
@require_admin_key
def api_refresh():
    """Refresh existing companies (Admin only)"""
    if collection_state.get_dict()['running']:
        return jsonify({'error': 'Collection already running'}), 409
    
    data = request.get_json() or {}
    validated, error = validate_request(RefreshRequest, data)
    
    if error:
        return jsonify(error), 400
    
    def run():
        collection_state.update(
            running=True,
            mode='refresh',
            current_progress=0.0,
            error=None,
            last_run=datetime.utcnow().isoformat()
        )
        
        try:
            stats = asyncio.run(run_refresh(
                hours_since_update=validated.hours_since_update,
                max_companies=validated.max_companies
            ))
            collection_state.update(last_stats=stats.to_dict() if hasattr(stats, 'to_dict') else stats)
            logger.info(f"✅ Manual refresh complete")
            
        except Exception as e:
            logger.error(f"❌ Manual refresh failed: {e}", exc_info=True)
            collection_state.update(error=str(e))
        finally:
            collection_state.update(running=False, current_progress=100)
            run_daily_maintenance()
    
    threading.Thread(target=run, daemon=True).start()
    
    return jsonify({
        'status': 'started',
        'message': 'Company refresh initiated',
        'hours_since_update': validated.hours_since_update,
        'max_companies': validated.max_companies
    }), 202

@app.route('/api/expand-seeds', methods=['POST'])
@limiter.limit(RATE_LIMITS['very_expensive'])
@require_admin_key
def api_expand_seeds():
    """Expand seed database (Admin only)"""
    if collection_state.get_dict()['running']:
        return jsonify({'error': 'Collection already running'}), 409
    
    data = request.get_json() or {}
    validated, error = validate_request(SeedExpansionRequest, data)
    
    if error:
        return jsonify(error), 400
    
    def run():
        collection_state.update(
            running=True,
            mode=f'expansion_{validated.tier}',
            current_progress=0.0,
            error=None,
            last_run=datetime.utcnow().isoformat()
        )
        
        try:
            if validated.tier == 'tier1':
                asyncio.run(run_tier1_expansion())
            elif validated.tier == 'tier2':
                asyncio.run(run_tier2_expansion())
            else:
                asyncio.run(run_full_expansion())
            
            logger.info(f"✅ Seed expansion complete: {validated.tier}")
            
        except Exception as e:
            logger.error(f"❌ Seed expansion failed: {e}", exc_info=True)
            collection_state.update(error=str(e))
        finally:
            collection_state.update(running=False, current_progress=100)
    
    threading.Thread(target=run, daemon=True).start()
    
    return jsonify({
        'status': 'started',
        'message': f'Seed expansion initiated: {validated.tier}',
        'tier': validated.tier
    }), 202

# ============================================================================
# Error Handlers
# ============================================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found', 'message': 'Endpoint does not exist'}), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({'error': 'Method not allowed', 'message': 'HTTP method not supported for this endpoint'}), 405

@app.errorhandler(429)
def rate_limit_handler(error):
    return jsonify({
        'error': 'Rate limit exceeded',
        'message': 'Too many requests. Please try again later.',
        'retry_after': str(error.description)
    }), 429

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {error}", exc_info=True)
    return jsonify({'error': 'Internal server error'}), 500

# ============================================================================
# Application Startup
# ============================================================================

def initialize_app():
    """Initialize application on startup"""
    logger.info("=" * 80)
    logger.info("🚀 Job Intelligence Platform Starting...")
    logger.info("=" * 80)
    
    # Test database connection
    try:
        db = get_db()
        stats = db.get_stats()
        logger.info(f"✅ Database connected: {stats.get('total_companies', 0)} companies, {stats.get('total_jobs', 0)} jobs")
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        raise
    
    # Start scheduler
    try:
        scheduler.start()
        logger.info(f"✅ Scheduler started with {len(scheduler.get_jobs())} jobs")
    except Exception as e:
        logger.error(f"❌ Scheduler failed to start: {e}")
        raise
    
    # Log security configuration
    logger.info(f"✅ Authentication enabled (API keys configured)")
    logger.info(f"✅ Rate limiting enabled ({'Redis' if os.getenv('REDIS_URL') else 'Memory'})")
    
    logger.info("=" * 80)
    logger.info("✅ Application initialized successfully!")
    logger.info("=" * 80)

# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == '__main__':
    initialize_app()
    
    port = int(os.getenv('PORT', 8080))
    debug = os.getenv('DEBUG', 'false').lower() == 'true'
    
    logger.info(f"🌐 Starting server on port {port}...")
    
    try:
        # Use Waitress for production
        from waitress import serve
        logger.info("📦 Using Waitress WSGI server (production mode)")
        serve(app, host='0.0.0.0', port=port, threads=8)
    except ImportError:
        logger.warning("⚠️ Waitress not available, using Flask development server")
        logger.warning("⚠️ NOT RECOMMENDED FOR PRODUCTION")
        app.run(host='0.0.0.0', port=port, debug=debug, threaded=True)
