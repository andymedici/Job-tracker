"""
Job Intelligence Dashboard & Scheduler
======================================
Web dashboard and background scheduler for Railway deployment.
"""

import os
import json
import asyncio
import threading
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any

from flask import Flask, jsonify, render_template, request
import schedule
import time

# Import modules (assuming these are correctly implemented as per snippets)
from database import get_db
from collector import run_collection, JobIntelCollector
from market_intel import run_daily_maintenance
from seed_expander import run_tier1_expansion, run_tier2_expansion, run_full_expansion

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


app = Flask(__name__)

# Add Python builtins to Jinja2 environment
app.jinja_env.globals['max'] = max
app.jinja_env.globals['min'] = min

# Global state
collection_state = {
    'running': False,
    'last_run': None,
    'last_stats': None,
    'last_intel': None,
    'current_progress': 0,
    'error': None
}


def get_db():
    """Get database instance."""
    from database import get_db as db_get_db
    return db_get_db()


def get_stats() -> Dict[str, Any]:
    """Get current statistics from database."""
    try:
        db = get_db()
        stats = db.get_stats()
        # NOTE: db.get_stats is assumed to return:
        # total_companies, total_jobs, remote_jobs, hybrid_jobs, onsite_jobs,
        # greenhouse_companies, lever_companies, greenhouse_jobs, lever_jobs,
        # total_seeds, seeds_tested, top_hiring_companies (list)
        stats['last_updated'] = datetime.utcnow().isoformat()
        stats['is_running'] = collection_state['running']
        stats['last_run_time'] = collection_state['last_run']
        stats['current_progress'] = collection_state['current_progress']
        stats['last_stats'] = collection_state['last_stats']
        return stats
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return {
            'error': str(e), 
            'last_updated': datetime.utcnow().isoformat(), 
            'is_running': collection_state['running'],
            'total_jobs': 0, 'total_companies': 0, 'remote_jobs': 0
        }


def background_collection_job():
    """Scheduled job to run the data collection."""
    if collection_state['running']:
        logger.info("Collection already running, skipping scheduled run.")
        return
        
    logger.info("Starting scheduled data collection...")
    collection_state['running'] = True
    collection_state['error'] = None
    collection_state['current_progress'] = 0
    collection_state['last_run'] = datetime.utcnow().isoformat()

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # This function would ideally update collection_state['current_progress'] 
        # but for simplicity in this file, we assume it completes fully.
        stats = loop.run_until_complete(run_collection())
        
        collection_state['last_stats'] = stats.to_dict()
        logger.info(f"Scheduled collection complete. Stats: {collection_state['last_stats']}")

    except Exception as e:
        logger.error(f"Scheduled collection failed: {e}", exc_info=True)
        collection_state['error'] = str(e)
    finally:
        collection_state['running'] = False
        collection_state['current_progress'] = 100
        # Run maintenance after collection
        run_daily_maintenance()


def start_scheduler():
    """Initialize and run the background scheduler."""
    schedule.every(8).hours.do(background_collection_job)
    schedule.every().day.at("03:00").do(run_daily_maintenance) 
    schedule.every().sunday.at("04:00").do(lambda: asyncio.run(run_full_expansion()))

    while True:
        schedule.run_pending()
        time.sleep(1)


# --- WEB ROUTES ---

@app.route('/')
@app.route('/dashboard')
def dashboard():
    """Render the main dashboard page."""
    return render_template('dashboard.html')

@app.route('/analytics')
def analytics():
    """Render the analytics page."""
    return render_template('analytics.html')

@app.route('/health')
def health():
    """Health check for Railway."""
    try:
        db = get_db()
        db.get_stats() 
        return jsonify({'status': 'ok', 'db': 'connected', 'collector': collection_state}), 200
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({'status': 'error', 'db': 'failed', 'error': str(e)}), 500

# --- API ROUTES ---

@app.route('/api/stats')
def api_stats():
    """Get current, real-time statistics."""
    return jsonify(get_stats())

@app.route('/api/history')
def api_history():
    """Get historical daily/monthly job counts."""
    days = request.args.get('days', type=int, default=7)
    try:
        db = get_db()
        history = db.get_history(days=days) 
        monthly_snapshots = db.get_monthly_snapshots()
        
        return jsonify({
            'history': history,
            'monthly_snapshots': monthly_snapshots
        })
    except Exception as e:
        logger.error(f"Error in /api/history: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/companies')
def api_companies():
    """Get a list of companies with filtering and sorting."""
    try:
        db = get_db()
        search = request.args.get('search', '')
        platform = request.args.get('platform', '')
        sort_by = request.args.get('sort_by', 'jobs')
        
        companies = db.get_companies(
            search_term=search,
            platform_filter=platform,
            sort_by=sort_by,
            limit=2000
        )
        return jsonify({'companies': companies})
    except Exception as e:
        logger.error(f"Error in /api/companies: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/intel')
def api_intel():
    """Get market intelligence: surges, declines, and expansions (last 7 days)."""
    try:
        db = get_db()
        # Assumes these methods are available on the database object.
        surges, declines = db.get_job_count_changes(days=7, change_percent_threshold=0.3) 
        expansions = db.get_location_expansions(days=7)
        
        return jsonify({
            'surges': surges,
            'declines': declines,
            'expansions': expansions,
        })
    except Exception as e:
        logger.error(f"Error in /api/intel: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/locations-stats')
def api_locations_stats():
    """Get location statistics."""
    try:
        db = get_db()
        locations_stats = db.get_location_stats(top_n=None)
        return jsonify({'locations': locations_stats})
    except Exception as e:
        logger.error(f"Error in /api/locations-stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/source-stats')
def api_source_stats():
    """Get hit rate statistics for all seed sources."""
    try:
        db = get_db()
        stats = db.get_source_stats()
        high_performers = db.get_high_performing_sources(min_tested=50, min_hit_rate=0.01)
        
        return jsonify({
            'sources': stats,
            'high_performers': high_performers,
            'total_sources': len(stats)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/collect', methods=['POST'])
def api_collect():
    """Manually trigger a data collection cycle."""
    if collection_state['running']:
        return jsonify({'status': 'error', 'message': 'Collection is already running'}), 409
    
    def run_collection_sync():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            stats = loop.run_until_complete(run_collection()) 
            collection_state['last_stats'] = stats.to_dict()
        except Exception as e:
            logger.error(f"Manual collection failed: {e}", exc_info=True)
            collection_state['error'] = str(e)
        finally:
            collection_state['running'] = False
            collection_state['current_progress'] = 100
            run_daily_maintenance()

    collection_state['running'] = True
    collection_state['error'] = None
    collection_state['current_progress'] = 0
    collection_state['last_run'] = datetime.utcnow().isoformat()
    
    thread = threading.Thread(target=run_collection_sync, daemon=True)
    thread.start()
    
    return jsonify({
        'status': 'success',
        'message': 'Data collection started in background'
    })


@app.route('/api/expand-seeds', methods=['POST'])
def api_expand_seeds():
    """Manually trigger the seed expansion process."""
    tier = request.json.get('tier', 'full').lower()
    
    if collection_state['running']:
        return jsonify({'status': 'error', 'message': 'Cannot run expansion while collection is running'}), 409

    def run_expansion():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        logger.info(f"Starting seed expansion for tier: {tier}")
        try:
            if tier == 'tier1':
                loop.run_until_complete(run_tier1_expansion())
            elif tier == 'tier2':
                loop.run_until_complete(run_tier2_expansion())
            else: # full
                loop.run_until_complete(run_full_expansion())
        finally:
            loop.close()
    
    thread = threading.Thread(target=run_expansion, daemon=True)
    thread.start()
    
    return jsonify({
        'status': 'success',
        'message': f'Seed expansion ({tier}) started in background'
    })


def main():
    """Main entry point."""
    logger.info("Starting Job Intelligence Dashboard...")
    
    # Start scheduler in background
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()
    logger.info("Background scheduler started")
    
    # Start Flask
    port = int(os.environ.get('PORT', 8080))
    
    # Using waitress for simple production-ready WSGI server
    from waitress import serve
    logger.info(f"Dashboard running on port {port}")
    serve(app, host='0.0.0.0', port=port)

if __name__ == "__main__":
    main()
