"""
Job Intelligence Dashboard & Scheduler
======================================
Web dashboard and background scheduler for Railway deployment.

MERGED: All analytical endpoints restored + Manual seed addition
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
    'error': None,
    'mode': None  # 'discovery' or 'refresh'
}


def get_stats() -> Dict[str, Any]:
    """Get current statistics from database."""
    try:
        db = get_db()
        stats = db.get_stats()
        stats['last_updated'] = datetime.utcnow().isoformat()
        stats['is_running'] = collection_state['running']
        stats['collection_running'] = collection_state['running']
        stats['last_run'] = collection_state['last_run']
        stats['last_error'] = collection_state['error']
        stats['last_stats'] = collection_state['last_stats']
        stats['last_intel'] = collection_state['last_intel']
        return stats
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return {
            'error': str(e), 
            'last_updated': datetime.utcnow().isoformat(), 
            'is_running': collection_state['running'],
            'total_jobs': 0, 
            'total_companies': 0, 
            'remote_jobs': 0
        }


def run_refresh_scheduled():
    """Scheduled job to refresh existing company data (every 6 hours)."""
    if collection_state['running']:
        logger.info("Collection already running, skipping scheduled refresh.")
        return
        
    logger.info("Starting scheduled refresh...")
    collection_state['running'] = True
    collection_state['mode'] = 'refresh'
    collection_state['error'] = None
    collection_state['current_progress'] = 0
    collection_state['last_run'] = datetime.utcnow().isoformat()

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        stats = loop.run_until_complete(run_refresh(hours_since_update=6, max_companies=500))
        collection_state['last_stats'] = stats.to_dict()
        logger.info(f"Scheduled refresh complete. Stats: {collection_state['last_stats']}")
    except Exception as e:
        logger.error(f"Scheduled refresh failed: {e}", exc_info=True)
        collection_state['error'] = str(e)
    finally:
        collection_state['running'] = False
        collection_state['current_progress'] = 100
        # Run maintenance after refresh
        intel_results = run_daily_maintenance()
        collection_state['last_intel'] = intel_results


def run_discovery_scheduled():
    """Scheduled job to discover new companies (weekly)."""
    if collection_state['running']:
        logger.info("Collection already running, skipping scheduled discovery.")
        return
        
    logger.info("Starting scheduled discovery...")
    collection_state['running'] = True
    collection_state['mode'] = 'discovery'
    collection_state['error'] = None
    collection_state['current_progress'] = 0
    collection_state['last_run'] = datetime.utcnow().isoformat()

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        stats = loop.run_until_complete(run_collection(max_companies=500))
        collection_state['last_stats'] = stats.to_dict()
        logger.info(f"Scheduled discovery complete. Stats: {collection_state['last_stats']}")
    except Exception as e:
        logger.error(f"Scheduled discovery failed: {e}", exc_info=True)
        collection_state['error'] = str(e)
    finally:
        collection_state['running'] = False
        collection_state['current_progress'] = 100
        intel_results = run_daily_maintenance()
        collection_state['last_intel'] = intel_results


def start_scheduler():
    """Initialize and run the background scheduler."""
    # Every 6 hours: Refresh existing companies
    schedule.every(6).hours.do(run_refresh_scheduled)
    
    # Sunday 2am: Discover new companies
    schedule.every().sunday.at("02:00").do(run_discovery_scheduled)
    
    # Sunday 3am: Tier 1 seed expansion
    schedule.every().sunday.at("03:00").do(lambda: asyncio.run(run_tier1_expansion()))
    
    # 1st of month 4am: Tier 2 seed expansion
    def monthly_expansion():
        if datetime.utcnow().day == 1:
            asyncio.run(run_tier2_expansion())
    schedule.every().day.at("04:00").do(monthly_expansion)

    logger.info("Scheduler initialized with: 6h refresh, Sunday discovery, weekly/monthly expansion")
    
    while True:
        schedule.run_pending()
        time.sleep(60)


# ============================================================================
# WEB ROUTES
# ============================================================================

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
        return jsonify({
            'status': 'ok', 
            'db': 'connected', 
            'running': collection_state['running'],
            'last_run': collection_state['last_run']
        }), 200
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({'status': 'error', 'db': 'failed', 'error': str(e)}), 500


# ============================================================================
# API ROUTES
# ============================================================================

@app.route('/api/stats')
def api_stats():
    """Get current, real-time statistics."""
    return jsonify(get_stats())


@app.route('/api/trends')
def api_trends():
    """Get granular trend data for charts."""
    days = request.args.get('days', type=int, default=7)
    try:
        db = get_db()
        granular = db.get_market_trends(days=days)
        monthly = db.get_monthly_snapshots()
        
        # Serialize datetime objects
        for item in granular:
            if 'timestamp' in item and item['timestamp']:
                item['timestamp'] = item['timestamp'].isoformat()
        for item in monthly:
            if 'month' in item and item['month']:
                item['month'] = item['month'].isoformat()
        
        return jsonify({
            'granular': granular,
            'monthly': monthly
        })
    except Exception as e:
        logger.error(f"Error in /api/trends: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/trends/company/<company_id>')
def api_company_trends(company_id):
    """Get trend data for a specific company."""
    days = request.args.get('days', type=int, default=7)
    try:
        db = get_db()
        trends = db.get_company_trends(company_id, days=days)
        
        for item in trends:
            if 'timestamp' in item and item['timestamp']:
                item['timestamp'] = item['timestamp'].isoformat()
        
        return jsonify({
            'company_id': company_id,
            'trends': trends
        })
    except Exception as e:
        logger.error(f"Error in /api/trends/company: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/history')
def api_history():
    """Get historical daily/monthly job counts."""
    days = request.args.get('days', type=int, default=7)
    try:
        db = get_db()
        history = db.get_history(days=days)
        monthly_snapshots = db.get_monthly_snapshots()
        
        for item in history:
            if 'timestamp' in item and item['timestamp']:
                item['timestamp'] = item['timestamp'].isoformat()
        for item in monthly_snapshots:
            if 'month' in item and item['month']:
                item['month'] = item['month'].isoformat()
        
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
        limit = request.args.get('limit', type=int, default=500)
        
        companies = db.get_companies(
            search_term=search,
            platform_filter=platform,
            sort_by=sort_by,
            limit=min(limit, 2000)
        )
        
        # Serialize datetime and JSONB
        for c in companies:
            for key in ['first_seen', 'last_seen', 'last_updated']:
                if key in c and c[key]:
                    c[key] = c[key].isoformat()
        
        return jsonify({'companies': companies})
    except Exception as e:
        logger.error(f"Error in /api/companies: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/changes')
def api_changes():
    """Get job count surges and declines."""
    days = request.args.get('days', type=int, default=7)
    try:
        db = get_db()
        surges, declines = db.get_job_count_changes(days=days)
        
        for item in surges + declines:
            if 'detected_at' in item and item['detected_at']:
                item['detected_at'] = item['detected_at'].isoformat()
        
        return jsonify({
            'surges': surges,
            'declines': declines
        })
    except Exception as e:
        logger.error(f"Error in /api/changes: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/expansions')
def api_expansions():
    """Get location expansions."""
    days = request.args.get('days', type=int, default=7)
    try:
        db = get_db()
        expansions = db.get_location_expansions(days=days)
        
        for item in expansions:
            if 'detected_at' in item and item['detected_at']:
                item['detected_at'] = item['detected_at'].isoformat()
        
        return jsonify({'expansions': expansions})
    except Exception as e:
        logger.error(f"Error in /api/expansions: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/intel')
def api_intel():
    """Get market intelligence: surges, declines, and expansions."""
    days = request.args.get('days', type=int, default=7)
    try:
        db = get_db()
        surges, declines = db.get_job_count_changes(days=days)
        expansions = db.get_location_expansions(days=days)
        
        for item in surges + declines + expansions:
            if 'detected_at' in item and item['detected_at']:
                item['detected_at'] = item['detected_at'].isoformat()
        
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
        locations_stats = db.get_location_stats(top_n=100)
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
        
        for item in stats:
            if 'last_updated' in item and item['last_updated']:
                item['last_updated'] = item['last_updated'].isoformat()
        
        return jsonify({
            'sources': stats,
            'high_performers': high_performers,
            'total_sources': len(stats)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/seeds', methods=['GET', 'POST'])
def api_seeds():
    """Get seeds or add new seeds manually."""
    db = get_db()
    
    if request.method == 'GET':
        try:
            limit = request.args.get('limit', type=int, default=100)
            source = request.args.get('source', None)
            seeds = db.get_seeds(limit=limit, source_filter=source)
            
            for s in seeds:
                if 'last_tested' in s and s['last_tested']:
                    s['last_tested'] = s['last_tested'].isoformat()
                if 'discovered_at' in s and s['discovered_at']:
                    s['discovered_at'] = s['discovered_at'].isoformat()
            
            return jsonify({'seeds': seeds, 'count': len(seeds)})
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    elif request.method == 'POST':
        try:
            data = request.get_json(silent=True) or {}
            companies = data.get('companies', [])
            
            # Handle both single string and array
            if isinstance(companies, str):
                # Split by newlines or commas
                companies = [c.strip() for c in companies.replace(',', '\n').split('\n') if c.strip()]
            
            added = 0
            for name in companies:
                if name and len(name) >= 2:
                    if db.add_manual_seed(name):
                        added += 1
            
            return jsonify({
                'status': 'success',
                'added': added,
                'total_submitted': len(companies)
            })
        except Exception as e:
            logger.error(f"Error adding seeds: {e}")
            return jsonify({'error': str(e)}), 500


# ============================================================================
# ACTION ROUTES
# ============================================================================

@app.route('/api/collect', methods=['POST'])
def api_collect():
    """Manually trigger a discovery cycle (new companies)."""
    if collection_state['running']:
        return jsonify({'status': 'error', 'message': 'Collection already running'}), 409
    
    def run_collection_sync():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            data = request.get_json(silent=True) or {}
            max_companies = data.get('max_companies', 500)
            stats = loop.run_until_complete(run_collection(max_companies=max_companies))
            collection_state['last_stats'] = stats.to_dict()
        except Exception as e:
            logger.error(f"Manual discovery failed: {e}", exc_info=True)
            collection_state['error'] = str(e)
        finally:
            collection_state['running'] = False
            collection_state['current_progress'] = 100
            intel_results = run_daily_maintenance()
            collection_state['last_intel'] = intel_results

    collection_state['running'] = True
    collection_state['mode'] = 'discovery'
    collection_state['error'] = None
    collection_state['current_progress'] = 0
    collection_state['last_run'] = datetime.utcnow().isoformat()
    
    thread = threading.Thread(target=run_collection_sync, daemon=True)
    thread.start()
    
    return jsonify({
        'status': 'success',
        'message': 'Discovery started in background'
    })


@app.route('/api/refresh', methods=['POST'])
def api_refresh():
    """Manually trigger a refresh cycle (existing companies)."""
    if collection_state['running']:
        return jsonify({'status': 'error', 'message': 'Collection already running'}), 409
    
    def run_refresh_sync():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            stats = loop.run_until_complete(run_refresh(hours_since_update=1, max_companies=500))
            collection_state['last_stats'] = stats.to_dict()
        except Exception as e:
            logger.error(f"Manual refresh failed: {e}", exc_info=True)
            collection_state['error'] = str(e)
        finally:
            collection_state['running'] = False
            collection_state['current_progress'] = 100
            intel_results = run_daily_maintenance()
            collection_state['last_intel'] = intel_results

    collection_state['running'] = True
    collection_state['mode'] = 'refresh'
    collection_state['error'] = None
    collection_state['current_progress'] = 0
    collection_state['last_run'] = datetime.utcnow().isoformat()
    
    thread = threading.Thread(target=run_refresh_sync, daemon=True)
    thread.start()
    
    return jsonify({
        'status': 'success',
        'message': 'Refresh started in background'
    })


@app.route('/api/expand-seeds', methods=['POST'])
def api_expand_seeds():
    """Manually trigger the seed expansion process."""
    data = request.get_json(silent=True) or {}
    tier = data.get('tier', 'full').lower()
    
    if collection_state['running']:
        return jsonify({'status': 'error', 'message': 'Cannot run expansion while collection is running'}), 409

    def run_expansion():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        logger.info(f"Starting seed expansion for tier: {tier}")
        try:
            if tier == 'tier1' or tier == '1':
                loop.run_until_complete(run_tier1_expansion())
            elif tier == 'tier2' or tier == '2':
                loop.run_until_complete(run_tier2_expansion())
            else:
                loop.run_until_complete(run_full_expansion())
        finally:
            loop.close()
    
    thread = threading.Thread(target=run_expansion, daemon=True)
    thread.start()
    
    return jsonify({
        'status': 'success',
        'message': f'Seed expansion ({tier}) started in background'
    })


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main entry point."""
    logger.info("Starting Job Intelligence Dashboard...")
    
    # Start scheduler in background
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()
    logger.info("Background scheduler started")
    
    # Start Flask
    port = int(os.environ.get('PORT', 8080))
    
    try:
        from waitress import serve
        logger.info(f"Dashboard running on port {port} (waitress)")
        serve(app, host='0.0.0.0', port=port)
    except ImportError:
        logger.info(f"Dashboard running on port {port} (Flask dev server)")
        app.run(host='0.0.0.0', port=port, debug=False)


if __name__ == "__main__":
    main()
