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

from flask import Flask, jsonify, render_template, request, Response
import schedule
import time

# NEW IMPORTS for collection and maintenance
# We import the new resilient get_db directly
from collector import run_collection 
from market_intel import run_daily_maintenance, send_weekly_report
from seed_expander import run_tier1_expansion, run_tier2_expansion, run_full_expansion
from database import get_db, Database # UPGRADED: Database import for type hint

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

# The original get_db() helper is implicitly replaced by the direct import, 
# but we'll use a direct reference in get_stats.

def get_stats():
    """Get current statistics from database."""
    try:
        db = get_db()
        stats = db.get_stats()
        stats['last_updated'] = datetime.utcnow().isoformat()
        return stats
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return {
            'total_companies': 0,
            'total_jobs': 0,
            'error': str(e),
            'last_updated': datetime.utcnow().isoformat()
        }


def run_collection_sync():
    """Run collection in sync context (for thread)."""
    global collection_state
    
    if collection_state['running']:
        logger.info("Collection already running, skipping")
        return
    
    try:
        collection_state['running'] = True
        collection_state['error'] = None
        collection_state['last_run'] = datetime.utcnow().isoformat()
        
        logger.info("Starting scheduled collection...")
        
        # Run async collection
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            stats = loop.run_until_complete(run_collection())
            collection_state['last_stats'] = stats.to_dict()
            logger.info(f"Collection completed: {stats.to_dict()}")
            
            # Run market intelligence after collection
            logger.info("Running market intelligence analysis...")
            
            intel_results = run_daily_maintenance()
            collection_state['last_intel'] = intel_results
            logger.info(f"Intelligence analysis complete: {intel_results}")
            
        finally:
            loop.close()
        
    except Exception as e:
        logger.error(f"Collection failed: {e}")
        collection_state['error'] = str(e)
    finally:
        collection_state['running'] = False


def run_weekly_email():
    """Send weekly email report."""
    try:
        success = send_weekly_report()
        logger.info(f"Weekly email report: {'sent' if success else 'failed'}")
    except Exception as e:
        logger.error(f"Failed to send weekly report: {e}")


def run_tier1_expansion_sync():
    """Run Tier 1 seed expansion (weekly - high quality sources) in sync context."""
    global collection_state
    
    if collection_state['running']:
        logger.info("Collection running, skipping Tier 1 expansion")
        return
    
    try:
        logger.info("🚀 Starting Tier 1 seed expansion...")
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            results = loop.run_until_complete(run_tier1_expansion())
            total = sum(len(c) for c in results.values() if isinstance(c, list))
            logger.info(f"✅ Tier 1 expansion complete: {total} companies discovered")
        finally:
            loop.close()
            
    except Exception as e:
        logger.error(f"Tier 1 expansion failed: {e}")
        
        
def run_tier2_expansion_sync():
    """Run Tier 2 seed expansion (monthly - broader sources) in sync context."""
    global collection_state
    
    if collection_state['running']:
        logger.info("Collection running, skipping Tier 2 expansion")
        return
    
    try:
        logger.info("🚀 Starting Tier 2 seed expansion...")
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            results = loop.run_until_complete(run_tier2_expansion())
            total = sum(len(c) for c in results.values() if isinstance(c, list))
            logger.info(f"✅ Tier 2 expansion complete: {total} companies discovered")
        finally:
            loop.close()
            
    except Exception as e:
        logger.error(f"Tier 2 expansion failed: {e}")
        
        
def start_scheduler():
    """Configure and start the background job scheduler."""
    
    # 1. Daily Collection: Run every 6 hours (4 times a day)
    schedule.every(6).hours.do(
        lambda: threading.Thread(target=run_collection_sync).start()
    ).tag('collection')
    
    # 2. Weekly Tier 1 Expansion (High-quality seeds): Run every Sunday at 3:00 AM UTC
    schedule.every().sunday.at("03:00").do(
        lambda: threading.Thread(target=run_tier1_expansion_sync).start()
    ).tag('expansion_t1')
    
    # 3. Monthly Tier 2 Expansion (Broader seeds): Run on the 1st of every month at 4:00 AM UTC
    schedule.every().day.at("04:00").do(
        lambda: datetime.now().day == 1 and threading.Thread(target=run_tier2_expansion_sync).start()
    ).tag('expansion_t2')
    
    # 4. Weekly Intelligence Report Email: Run every Monday at 9:00 AM UTC
    schedule.every().monday.at("09:00").do(
        lambda: threading.Thread(target=run_weekly_email).start()
    ).tag('report')
    
    # Start the first collection immediately upon boot (after a brief delay)
    logger.info("Initial collection scheduled to run in 60 seconds.")
    schedule.every(1).minutes.do(
        lambda: threading.Thread(target=run_collection_sync).start() and schedule.clear('initial_run')
    ).tag('initial_run').at(":00")
    
    # Main scheduler loop
    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            logger.error(f"Scheduler error: {e}")
        time.sleep(1)


# ==================== FLASK ROUTES ====================

# Helper for /health
@app.route('/health')
def health_check():
    """Health check endpoint for Railway."""
    # Check if the database connection is alive
    try:
        get_db().test_connection()
        return jsonify({'status': 'ok', 'db': 'connected'}), 200
    except Exception as e:
        logger.error(f"Health check failed: Database connection error: {e}")
        return jsonify({'status': 'error', 'db': 'disconnected', 'error': str(e)}), 503

@app.route('/')
def dashboard():
    """Main dashboard view."""
    stats = get_stats()
    return render_template(
        "dashboard.html",
        stats=stats,
        state=collection_state
    )

@app.route('/analytics')
def analytics():
    """Comprehensive analytics dashboard."""
    return render_template("analytics.html")

@app.route('/api/stats')
def api_stats():
    """API endpoint for statistics."""
    stats = get_stats()
    stats['collection_running'] = collection_state.get('running', False)
    stats['last_run'] = collection_state.get('last_run')
    stats['last_error'] = collection_state.get('error')
    stats['last_stats'] = collection_state.get('last_stats')
    stats['last_intel'] = collection_state.get('last_intel')
    return jsonify(stats)

@app.route('/api/run-collection', methods=['POST'])
def api_run_collection():
    """Trigger a manual collection run."""
    if collection_state['running']:
        return jsonify({'status': 'error', 'message': 'Collection already running'}), 409
    
    thread = threading.Thread(target=run_collection_sync, daemon=True)
    thread.start()
    
    return jsonify({
        'status': 'success', 
        'message': 'Collection started in background'
    })

@app.route('/api/run-expansion', methods=['POST'])
def api_run_expansion():
    """Trigger a manual seed expansion run."""
    tier = request.args.get('tier', 'full').lower()
    
    def run_expansion():
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            if tier == 'tier1':
                results = loop.run_until_complete(run_tier1_expansion())
                logger.info(f"Tier 1 expansion complete: {len(results.get('total_unique', []))} unique companies")
            elif tier == 'tier2':
                results = loop.run_until_complete(run_tier2_expansion())
                logger.info(f"Tier 2 expansion complete: {len(results.get('total_unique', []))} unique companies")
            elif tier == 'full':
                results = loop.run_until_complete(run_full_expansion())
                logger.info(f"Full expansion complete: {len(results.get('total_unique', []))} unique companies")
        finally:
            loop.close()
    
    thread = threading.Thread(target=run_expansion, daemon=True)
    thread.start()
    
    return jsonify({
        'status': 'success',
        'message': f'Seed expansion ({tier}) started in background'
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


def main():
    """Main entry point."""
    logger.info("Starting Job Intelligence Dashboard...")
    
    # FIX: Force initial database connection and wait for service health
    try:
        # The get_db() function now includes retry logic and will wait 
        # for the database to become available before returning.
        get_db()
        logger.info("Initial database connection successful. Proceeding to start services.")
    except Exception as e:
        # If get_db() fails after all retries, log a fatal error and exit 
        # to let the container restart (which triggers the retry again).
        logger.fatal(f"Failed to initialize database connection. Shutting down. Error: {e}")
        exit(1)
    
    # Start scheduler in background
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()
    logger.info("Background scheduler started")
    
    # Start Flask
    port = int(os.environ.get('PORT', 8080))
    logger.info(f"Starting Flask server on port {port}")
    # The default Flask debug=False is crucial for production
    app.run(host='0.0.0.0', port=port, debug=False)


if __name__ == "__main__":
    main()
