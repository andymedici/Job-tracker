import os
import re
import json
import asyncio
import threading
import logging
from datetime import datetime
from typing import Dict, Any

from flask import Flask, jsonify, render_template, request
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.triggers.cron import CronTrigger

from database import get_db
from collector import run_collection, run_refresh, JobIntelCollector
from market_intel import run_daily_maintenance
from seed_expander import run_tier1_expansion, run_tier2_expansion, run_full_expansion

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Add Python builtins to Jinja2
app.jinja_env.globals.update(max=max, min=min)

# Global collection state for dashboard progress
collection_state = {
    'running': False,
    'last_run': None,
    'last_stats': None,
    'last_intel': None,
    'current_progress': 0.0,
    'error': None,
    'mode': None  # 'discovery', 'refresh', 'expansion'
}

def progress_callback(progress: float, stats: Dict[str, Any]):
    """Callback from collector to update live progress."""
    collection_state['current_progress'] = progress
    collection_state['last_stats'] = stats

def get_stats() -> Dict[str, Any]:
    try:
        db = get_db()
        stats = db.get_stats()
        stats.update({
            'last_updated': datetime.utcnow().isoformat(),
            'is_running': collection_state['running'],
            'current_progress': collection_state['current_progress'],
            'mode': collection_state['mode'],
            'last_run': collection_state['last_run'],
            'last_error': collection_state['error'],
            'last_stats': collection_state['last_stats'],
            'last_intel': collection_state['last_intel'],
        })
        return stats
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return {'error': str(e), 'total_jobs': 0, 'total_companies': 0}

# ============================================================================
# SCHEDULER SETUP (APScheduler with PostgreSQL persistence)
# ============================================================================

jobstores = {
    'default': SQLAlchemyJobStore(url=os.getenv('DATABASE_URL'))
}
executors = {
    'default': ThreadPoolExecutor(10)
}
job_defaults = {
    'coalesce': False,
    'max_instances': 1
}

scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)

def scheduled_refresh():
    if collection_state['running']:
        logger.info("Refresh already running, skipping scheduled.")
        return
    collection_state.update({
        'running': True,
        'mode': 'refresh',
        'current_progress': 0.0,
        'error': None,
        'last_run': datetime.utcnow().isoformat()
    })
    logger.info("Starting scheduled refresh...")
    try:
        stats = asyncio.run(run_refresh(hours_since_update=6, max_companies=500))
        collection_state['last_stats'] = stats.to_dict()
    except Exception as e:
        logger.error(f"Scheduled refresh failed: {e}")
        collection_state['error'] = str(e)
    finally:
        collection_state['running'] = False
        collection_state['current_progress'] = 100
        run_daily_maintenance()
        collection_state['last_intel'] = {}  # Update as needed

def scheduled_discovery():
    if collection_state['running']:
        return
    collection_state.update({
        'running': True,
        'mode': 'discovery',
        'current_progress': 0.0,
        'error': None,
        'last_run': datetime.utcnow().isoformat()
    })
    logger.info("Starting scheduled discovery...")
    try:
        collector = JobIntelCollector(progress_callback=progress_callback)
        stats = asyncio.run(collector.run_discovery(max_companies=500))
        collection_state['last_stats'] = stats.to_dict()
    except Exception as e:
        logger.error(f"Scheduled discovery failed: {e}")
        collection_state['error'] = str(e)
    finally:
        collection_state['running'] = False
        collection_state['current_progress'] = 100
        run_daily_maintenance()

def scheduled_tier1_expansion():
    if collection_state['running']:
        return
    collection_state.update({
        'running': True,
        'mode': 'expansion_tier1',
        'current_progress': 0.0,
        'last_run': datetime.utcnow().isoformat()
    })
    logger.info("Starting scheduled Tier 1 seed expansion...")
    try:
        asyncio.run(run_tier1_expansion())
    except Exception as e:
        logger.error(f"Tier 1 expansion failed: {e}")
        collection_state['error'] = str(e)
    finally:
        collection_state['running'] = False
        collection_state['current_progress'] = 100

def scheduled_tier2_expansion():
    if collection_state['running']:
        return
    collection_state.update({
        'running': True,
        'mode': 'expansion_tier2',
        'current_progress': 0.0,
        'last_run': datetime.utcnow().isoformat()
    })
    logger.info("Starting scheduled Tier 2 seed expansion...")
    try:
        asyncio.run(run_tier2_expansion())
    except Exception as e:
        logger.error(f"Tier 2 expansion failed: {e}")
        collection_state['error'] = str(e)
    finally:
        collection_state['running'] = False
        collection_state['current_progress'] = 100

# Schedule jobs
scheduler.add_job(scheduled_refresh, CronTrigger(hour='*/6'))  # Every 6 hours
scheduler.add_job(scheduled_discovery, CronTrigger(day_of_week='sun', hour=2))
scheduler.add_job(scheduled_tier1_expansion, CronTrigger(day_of_week='sun', hour=3))
scheduler.add_job(scheduled_tier2_expansion, CronTrigger(day=1, hour=4))  # 1st of month

scheduler.start()
logger.info("APScheduler started with persistent jobs")

# ============================================================================
# WEB ROUTES
# ============================================================================

@app.route('/')
@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

@app.route('/analytics')
def analytics():
    return render_template('analytics.html')

@app.route('/health')
def health():
    try:
        get_db().get_stats()
        return jsonify({'status': 'ok', 'running': collection_state['running']}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500

# ============================================================================
# API ROUTES
# ============================================================================

@app.route('/api/stats')
def api_stats():
    return jsonify(get_stats())

@app.route('/api/trends')
def api_trends():
    days = request.args.get('days', 7, type=int)
    db = get_db()
    granular = db.get_market_trends(days=days)
    monthly = db.get_monthly_snapshots() if hasattr(db, 'get_monthly_snapshots') else []
    return jsonify({'granular': granular, 'monthly': monthly})

@app.route('/api/intel')
def api_intel():
    days = request.args.get('days', 7, type=int)
    db = get_db()
    surges, declines = db.get_job_count_changes(days=days)
    expansions = db.get_location_expansions(days=days)
    return jsonify({'surges': surges, 'declines': declines, 'expansions': expansions})

@app.route('/api/advanced-analytics')
def api_advanced_analytics():
    db = get_db()
    data = db.get_advanced_analytics()
    return jsonify(data)

@app.route('/api/seeds', methods=['GET', 'POST'])
def api_seeds():
    db = get_db()
    if request.method == 'POST':
        data = request.get_json() or {}
        companies = data.get('companies', [])
        if isinstance(companies, str):
            companies = [c.strip() for c in companies.replace(',', '\n').split('\n') if c.strip()]
        added = sum(1 for name in companies if db.add_manual_seed(name))
        return jsonify({'added': added})
    else:
        seeds = db.get_seeds(limit=request.args.get('limit', 100, type=int))
        return jsonify({'seeds': seeds})

# Manual triggers
@app.route('/api/collect', methods=['POST'])
def api_collect():
    if collection_state['running']:
        return jsonify({'error': 'Already running'}), 409
    def run():
        collection_state.update({'running': True, 'mode': 'discovery', 'current_progress': 0.0, 'last_run': datetime.utcnow().isoformat()})
        try:
            collector = JobIntelCollector(progress_callback=progress_callback)
            stats = asyncio.run(collector.run_discovery(max_companies=500))
            collection_state['last_stats'] = stats.to_dict()
        except Exception as e:
            collection_state['error'] = str(e)
        finally:
            collection_state['running'] = False
            collection_state['current_progress'] = 100
            run_daily_maintenance()
    threading.Thread(target=run, daemon=True).start()
    return jsonify({'status': 'started'})

@app.route('/api/refresh', methods=['POST'])
def api_refresh():
    if collection_state['running']:
        return jsonify({'error': 'Already running'}), 409
    def run():
        collection_state.update({'running': True, 'mode': 'refresh', 'current_progress': 0.0, 'last_run': datetime.utcnow().isoformat()})
        try:
            stats = asyncio.run(run_refresh(hours_since_update=1, max_companies=500))
            collection_state['last_stats'] = stats.to_dict()
        except Exception as e:
            collection_state['error'] = str(e)
        finally:
            collection_state['running'] = False
            collection_state['current_progress'] = 100
            run_daily_maintenance()
    threading.Thread(target=run, daemon=True).start()
    return jsonify({'status': 'started'})

@app.route('/api/expand-seeds', methods=['POST'])
def api_expand_seeds():
    if collection_state['running']:
        return jsonify({'error': 'Already running'}), 409
    tier = request.get_json().get('tier', 'full').lower()
    def run():
        collection_state.update({'running': True, 'mode': f'expansion_{tier}', 'current_progress': 0.0, 'last_run': datetime.utcnow().isoformat()})
        try:
            if tier == 'tier1':
                asyncio.run(run_tier1_expansion())
            elif tier == 'tier2':
                asyncio.run(run_tier2_expansion())
            else:
                asyncio.run(run_full_expansion())
        except Exception as e:
            collection_state['error'] = str(e)
        finally:
            collection_state['running'] = False
            collection_state['current_progress'] = 100
    threading.Thread(target=run, daemon=True).start()
    return jsonify({'status': 'started'})

# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    try:
        from waitress import serve
        serve(app, host='0.0.0.0', port=port)
    except ImportError:
        app.run(host='0.0.0.0', port=port, debug=False)
