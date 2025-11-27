"""
Job Intelligence Dashboard & Scheduler
======================================
Web dashboard and background scheduler for Railway deployment.

Features:
- Real-time statistics dashboard
- Manual collection triggers
- API endpoints for data access
- Background scheduled collection
- Health checks for Railway
- PostgreSQL database support
"""

import os
import json
import asyncio
import threading
import logging
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional

from flask import Flask, jsonify, render_template, request, Response
import schedule
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# NEW IMPORTS for collection and maintenance
from collector import run_collection 
from market_intel import run_daily_maintenance 
from seed_expander import run_full_expansion, run_tier1_expansion, run_tier2_expansion
from database import get_db, Database

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
    return get_db()


def get_stats():
    """Get current statistics from database."""
    try:
        db = get_db()
        stats = db.get_stats()
        stats['last_updated'] = datetime.utcnow().isoformat()
        return stats
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return {}


def run_collection_task(max_companies: Optional[int] = None):
    """Wrapper to run collection in the background thread."""
    if collection_state['running']:
        logger.warning("Collection already running, skipping scheduled run.")
        return

    collection_state['running'] = True
    collection_state['error'] = None
    logger.info(f"Starting collection task (Max: {max_companies})...")
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        stats = loop.run_until_complete(run_collection(max_companies=max_companies))
        collection_state['last_run'] = datetime.utcnow().isoformat()
        collection_state['last_stats'] = stats.to_dict()
        logger.info("Collection finished successfully.")
    except Exception as e:
        logger.error(f"Collection failed: {e}")
        collection_state['error'] = str(e)
    finally:
        collection_state['running'] = False
        loop.close()


def trigger_collection(max_companies: Optional[int] = None):
    """Starts the collection task in a new thread."""
    thread = threading.Thread(target=run_collection_task, args=(max_companies,), daemon=True)
    thread.start()


def run_daily_maintenance_task():
    """Wrapper to run market intelligence daily maintenance (UPGRADE)."""
    logger.info("Starting daily maintenance task...")
    try:
        # Maintenance also includes purging old data and creating snapshots
        results = run_daily_maintenance()
        logger.info(f"Maintenance complete: {results}")
        collection_state['last_intel'] = {
            'timestamp': datetime.utcnow().isoformat(),
            'stats': results
        }
        # Force stats refresh after maintenance
        collection_state['last_stats'] = get_stats() 
    except Exception as e:
        logger.error(f"Daily maintenance error: {e}")


def start_scheduler():
    """Sets up and runs the background scheduling loop."""
    logger.info("Setting up scheduler jobs...")
    
    # Core Collection: Every 6 hours to get fresh data
    schedule.every(6).hours.do(trigger_collection, max_companies=500).tag('core-collection')
    logger.info("  - Core collection scheduled (every 6 hours)")

    # Seed Expansion: Tier 1 daily, Tier 2 weekly
    schedule.every().day.at("02:00").do(trigger_expansion, tier=1).tag('seed-expansion')
    schedule.every().monday.at("03:00").do(trigger_expansion, tier=2).tag('seed-expansion')
    logger.info("  - Seed expansion scheduled (Tier 1 daily, Tier 2 weekly)")
    
    # Daily Maintenance (UPGRADE)
    schedule.every().day.at("04:00").do(run_daily_maintenance_task).tag('maintenance')
    logger.info("  - Daily maintenance scheduled (4:00 AM UTC)")
    
    # Run loop
    while True:
        schedule.run_pending()
        time.sleep(1)

# ========================================================================
# FLASK ENDPOINTS
# ========================================================================

@app.route('/')
def dashboard():
    """The main dashboard view."""
    return render_template('dashboard.html')


@app.route('/analytics')
def analytics():
    """The analytics and historical view."""
    return render_template('analytics.html')


@app.route('/health')
def health_check():
    """Health check endpoint for Railway."""
    try:
        db = get_db()
        # Test connection by fetching a simple stat
        stats = db.get_stats()
        if stats is not None:
            return jsonify({'status': 'ok', 'db': 'connected'}), 200
        return jsonify({'status': 'error', 'db': 'disconnected'}), 500
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({'status': 'error', 'db': str(e)}), 500


@app.route('/api/stats')
def api_stats():
    """Get all current statistics and status."""
    stats = get_stats()
    stats['collection_status'] = collection_state
    
    # Calculate key metrics
    total_companies = stats.get('total_companies', 0)
    total_jobs = stats.get('total_jobs', 0)
    
    stats['average_jobs_per_company'] = round(total_jobs / total_companies, 2) if total_companies else 0
    stats['seeds_hit_rate'] = round(stats.get('seeds_hit', 0) / stats.get('seeds_tested', 1), 4) * 100
    
    # Include untested seeds stat (UPGRADE)
    stats['untested_seeds'] = stats.get('untested_seeds', 0) 
    
    return jsonify(stats)


@app.route('/api/collection/run', methods=['POST'])
def api_run_collection():
    """Manually trigger a collection run."""
    if collection_state['running']:
        return jsonify({'status': 'error', 'message': 'Collection already running'}), 409
    
    data = request.get_json(silent=True)
    max_companies = data.get('max_companies') if data and data.get('max_companies') else None
    
    thread = threading.Thread(target=run_collection_task, args=(max_companies,), daemon=True)
    thread.start()
    
    return jsonify({'status': 'success', 'message': f'Collection (Max: {max_companies}) started in background'})


@app.route('/api/expansion/run/<int:tier>', methods=['POST'])
def api_run_expansion(tier):
    """Manually trigger seed expansion for a specific tier."""
    
    def run_expansion():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            if tier == 1:
                results = loop.run_until_complete(run_tier1_expansion())
                logger.info(f"Tier 1 expansion complete: {len(results.get('unique_names', []))} new seeds")
            elif tier == 2:
                results = loop.run_until_complete(run_tier2_expansion())
                logger.info(f"Tier 2 expansion complete: {len(results.get('unique_names', []))} new seeds")
            else:
                results = loop.run_until_complete(run_full_expansion())
                logger.info(f"Full expansion complete: {len(results.get('total_unique', []))} unique companies")
        finally:
            loop.close()
    
    thread = threading.Thread(target=run_expansion, daemon=True)
    thread.start()
    
    return jsonify({
        'status': 'success',
        'message': f'Seed expansion (Tier {tier}) started in background'
    })


@app.route('/api/source-stats')
def api_source_stats():
    """Get hit rate statistics for all seed sources."""
    try:
        db = get_db()
        stats = db.get_source_stats()
        
        # Also get summary
        high_performers = db.get_high_performing_sources(min_tested=50, min_hit_rate=0.01)
        
        return jsonify({
            'sources': stats,
            'high_performers': high_performers,
            'total_sources': len(stats)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/historical-data')
def api_historical_data():
    """Get monthly snapshot data for analytics page."""
    try:
        db = get_db()
        # This is a placeholder; you'd need a method in database.py to get all snapshots
        # For now, return a mock or a simple list from a simplified DB method if available
        # Assuming database.py has a get_monthly_snapshots method
        # snapshots = db.get_monthly_snapshots()
        
        # Mock historical data for rendering the chart
        snapshots = [
            {'snapshot_month': (datetime.utcnow() - timedelta(days=90)).isoformat(), 'total_jobs': 5000, 'total_companies': 100, 'avg_remote_pct': 30.5, 'avg_hybrid_pct': 15.0, 'avg_onsite_pct': 54.5},
            {'snapshot_month': (datetime.utcnow() - timedelta(days=60)).isoformat(), 'total_jobs': 5500, 'total_companies': 110, 'avg_remote_pct': 31.0, 'avg_hybrid_pct': 16.0, 'avg_onsite_pct': 53.0},
            {'snapshot_month': (datetime.utcnow() - timedelta(days=30)).isoformat(), 'total_jobs': 6200, 'total_companies': 125, 'avg_remote_pct': 32.5, 'avg_hybrid_pct': 14.5, 'avg_onsite_pct': 53.0},
            {'snapshot_month': datetime.utcnow().isoformat(), 'total_jobs': get_stats().get('total_jobs', 0), 'total_companies': get_stats().get('total_companies', 0), 'avg_remote_pct': 35.0, 'avg_hybrid_pct': 15.0, 'avg_onsite_pct': 50.0},
        ]
        
        return jsonify({'snapshots': snapshots})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def main():
    """Main entry point."""
    logger.info("Starting Job Intelligence Dashboard...")
    
    # Start scheduler in background
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()
    logger.info("Background scheduler started")
    
    # Start Flask
    port = int(os.environ.get('PORT', 8080))
    logger.info(f"Starting Flask server on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)


if __name__ == "__main__":
    main()
