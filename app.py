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
        from collector import run_collection
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            stats = loop.run_until_complete(run_collection())
            collection_state['last_stats'] = stats.to_dict()
            logger.info(f"Collection completed: {stats.to_dict()}")
            
            # Run market intelligence after collection
            logger.info("Running market intelligence analysis...")
            from market_intel import run_daily_maintenance, MarketIntelligence
            
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
        from market_intel import send_weekly_report
        success = send_weekly_report()
        logger.info(f"Weekly email report: {'sent' if success else 'failed'}")
    except Exception as e:
        logger.error(f"Failed to send weekly report: {e}")


def run_tier1_expansion():
    """Run Tier 1 seed expansion (weekly - high quality sources)."""
    global collection_state
    
    if collection_state['running']:
        logger.info("Collection running, skipping Tier 1 expansion")
        return
    
    try:
        logger.info("🚀 Starting Tier 1 seed expansion...")
        import asyncio
        from seed_expander import run_tier1_expansion
        
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


def run_tier2_expansion():
    """Run Tier 2 seed expansion (monthly - broader sources)."""
    global collection_state
    
    if collection_state['running']:
        logger.info("Collection running, skipping Tier 2 expansion")
        return
    
    try:
        logger.info("🚀 Starting Tier 2 seed expansion...")
        import asyncio
        from seed_expander import run_tier2_expansion
        
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
    """Start background scheduler."""
    # Schedule collection every 6 hours
    schedule.every(6).hours.do(lambda: threading.Thread(target=run_collection_sync).start())
    
    # Also run daily at specific times
    schedule.every().day.at("06:00").do(lambda: threading.Thread(target=run_collection_sync).start())
    schedule.every().day.at("12:00").do(lambda: threading.Thread(target=run_collection_sync).start())
    schedule.every().day.at("18:00").do(lambda: threading.Thread(target=run_collection_sync).start())
    
    # Weekly email report on Mondays at 9 AM
    schedule.every().monday.at("09:00").do(lambda: threading.Thread(target=run_weekly_email).start())
    
    # SEED EXPANSION SCHEDULES
    # Tier 1 expansion weekly (Sundays at 3 AM) - high quality tech sources
    schedule.every().sunday.at("03:00").do(lambda: threading.Thread(target=run_tier1_expansion).start())
    
    # Tier 2 expansion monthly (1st of month at 4 AM) - broader sources
    schedule.every().day.at("04:00").do(
        lambda: threading.Thread(target=run_tier2_expansion).start() 
        if datetime.utcnow().day == 1 else None
    )
    
    # Run initial collection after startup delay
    def initial_run():
        time.sleep(30)  # Wait for app to stabilize
        if not collection_state['running']:
            threading.Thread(target=run_collection_sync).start()
    
    threading.Thread(target=initial_run, daemon=True).start()
    
    # Scheduler loop
    while True:
        schedule.run_pending()
        time.sleep(60)



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
    stats['collection_state'] = collection_state
    return jsonify(stats)


@app.route('/api/companies')
def api_companies():
    """API endpoint for company data."""
    try:
        db = get_db()
        limit = request.args.get('limit', 100, type=int)
        ats_type = request.args.get('ats_type')
        min_jobs = request.args.get('min_jobs', 0, type=int)
        
        with db.get_cursor() as cursor:
            query = """
                SELECT * FROM companies 
                WHERE job_count >= %s
            """
            params = [min_jobs]
            
            if ats_type:
                query += " AND ats_type = %s"
                params.append(ats_type)
            
            query += " ORDER BY job_count DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(query, params)
            companies = []
            for row in cursor:
                company = dict(row)
                # JSONB fields are already parsed in PostgreSQL
                companies.append(company)
            
            return jsonify({
                'count': len(companies),
                'companies': companies
            })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/collect', methods=['POST'])
def api_collect():
    """Trigger a collection run."""
    global collection_state
    
    if collection_state['running']:
        return jsonify({
            'status': 'error',
            'message': 'Collection already in progress'
        }), 409
    
    # Start collection in background thread
    thread = threading.Thread(target=run_collection_sync, daemon=True)
    thread.start()
    
    return jsonify({
        'status': 'success',
        'message': 'Collection started'
    })


@app.route('/health')
def health():
    """Health check endpoint for Railway."""
    try:
        db = get_db()
        stats = db.get_stats()
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat(),
            'database': 'connected',
            'companies': stats.get('total_companies', 0),
            'collection_running': collection_state['running']
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500


@app.route('/api/intelligence')
def api_intelligence():
    """Get market intelligence summary."""
    try:
        from market_intel import MarketIntelligence
        
        days = request.args.get('days', 7, type=int)
        intel = MarketIntelligence()
        report = intel.generate_report(days=days)
        
        return jsonify({
            'period': {
                'start': report.period_start.isoformat(),
                'end': report.period_end.isoformat(),
                'days': days
            },
            'summary': {
                'total_companies': report.total_companies,
                'total_jobs': report.total_jobs,
                'new_companies': report.new_companies,
                'remote_jobs': report.remote_jobs,
                'hybrid_jobs': report.hybrid_jobs,
                'onsite_jobs': report.onsite_jobs
            },
            'location_expansions': [
                {
                    'company': exp.company_name,
                    'ats_type': exp.ats_type,
                    'new_location': exp.new_location,
                    'detected_at': exp.detected_at.isoformat()
                }
                for exp in report.location_expansions[:20]
            ],
            'job_surges': [
                {
                    'company': s.company_name,
                    'ats_type': s.ats_type,
                    'previous': s.previous_count,
                    'current': s.current_count,
                    'change_percent': s.change_percent
                }
                for s in report.job_surges[:20]
            ],
            'job_declines': [
                {
                    'company': d.company_name,
                    'ats_type': d.ats_type,
                    'previous': d.previous_count,
                    'current': d.current_count,
                    'change_percent': d.change_percent
                }
                for d in report.job_declines[:20]
            ],
            'new_entrants': report.new_entrants[:20],
            'month_over_month_change': report.month_over_month_change
        })
    except Exception as e:
        logger.error(f"Intelligence API error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/expansions')
def api_expansions():
    """Get location expansions."""
    try:
        db = get_db()
        days = request.args.get('days', 30, type=int)
        limit = request.args.get('limit', 100, type=int)
        
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT company_name, ats_type, new_location, 
                       job_count_at_detection, detected_at
                FROM location_expansions
                WHERE detected_at > NOW() - INTERVAL '%s days'
                ORDER BY detected_at DESC
                LIMIT %s
            """, (days, limit))
            
            expansions = [dict(row) for row in cursor]
            
            return jsonify({
                'count': len(expansions),
                'days': days,
                'expansions': expansions
            })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/changes')
def api_changes():
    """Get job count changes (surges and declines)."""
    try:
        db = get_db()
        days = request.args.get('days', 7, type=int)
        change_type = request.args.get('type')  # 'surge', 'decline', or None for both
        limit = request.args.get('limit', 50, type=int)
        
        with db.get_cursor() as cursor:
            if change_type:
                cursor.execute("""
                    SELECT company_name, ats_type, previous_count, current_count,
                           change_percent, change_type, detected_at
                    FROM job_count_changes
                    WHERE detected_at > NOW() - INTERVAL '%s days'
                    AND change_type = %s
                    ORDER BY ABS(change_percent) DESC 
                    LIMIT %s
                """, (days, change_type, limit))
            else:
                cursor.execute("""
                    SELECT company_name, ats_type, previous_count, current_count,
                           change_percent, change_type, detected_at
                    FROM job_count_changes
                    WHERE detected_at > NOW() - INTERVAL '%s days'
                    ORDER BY ABS(change_percent) DESC 
                    LIMIT %s
                """, (days, limit))
            
            changes = [dict(row) for row in cursor]
            
            return jsonify({
                'count': len(changes),
                'days': days,
                'filter': change_type,
                'changes': changes
            })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/history/<company_id>')
def api_company_history(company_id):
    """Get historical job data for a specific company."""
    try:
        db = get_db()
        
        with db.get_cursor() as cursor:
            # Get company info
            cursor.execute("SELECT * FROM companies WHERE id = %s", (company_id,))
            company = cursor.fetchone()
            
            if not company:
                return jsonify({'error': 'Company not found'}), 404
            
            company_data = dict(company)
            
            # Get monthly snapshots
            cursor.execute("""
                SELECT year, month, job_count, remote_count, hybrid_count, onsite_count
                FROM monthly_snapshots
                WHERE company_id = %s
                ORDER BY year DESC, month DESC
                LIMIT 24
            """, (company_id,))
            snapshots = [dict(row) for row in cursor]
            
            # Get location expansions
            cursor.execute("""
                SELECT new_location, detected_at
                FROM location_expansions
                WHERE company_id = %s
                ORDER BY detected_at DESC
            """, (company_id,))
            expansions = [dict(row) for row in cursor]
            
            # Get job count changes
            cursor.execute("""
                SELECT previous_count, current_count, change_percent, 
                       change_type, detected_at
                FROM job_count_changes
                WHERE company_id = %s
                ORDER BY detected_at DESC
                LIMIT 20
            """, (company_id,))
            changes = [dict(row) for row in cursor]
            
            return jsonify({
                'company': company_data,
                'monthly_snapshots': snapshots,
                'location_expansions': expansions,
                'job_changes': changes
            })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/weekly-stats')
def api_weekly_stats():
    """Get weekly aggregated statistics for trends."""
    try:
        db = get_db()
        weeks = request.args.get('weeks', 12, type=int)
        
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM weekly_stats
                ORDER BY week_start DESC
                LIMIT %s
            """, (weeks,))
            
            stats = [dict(row) for row in cursor]
            
            return jsonify({
                'weeks': len(stats),
                'stats': stats
            })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/send-report', methods=['POST'])
def api_send_report():
    """Manually trigger an email report."""
    try:
        from market_intel import MarketIntelligence
        
        data = request.get_json() or {}
        recipient = data.get('recipient') or os.environ.get('EMAIL_RECIPIENT')
        days = data.get('days', 7)
        
        if not recipient:
            return jsonify({'error': 'No recipient specified'}), 400
        
        intel = MarketIntelligence()
        report = intel.generate_report(days=days)
        success = intel.send_email_report(report, recipient)
        
        return jsonify({
            'status': 'success' if success else 'failed',
            'recipient': recipient,
            'report_days': days
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/trends')
def api_trends():
    """Get job market trends."""
    try:
        db = get_db()
        with db.get_cursor() as cursor:
            # Get monthly snapshots
            cursor.execute("""
                SELECT 
                    year, month,
                    COUNT(DISTINCT company_id) as companies,
                    COALESCE(SUM(job_count), 0) as total_jobs,
                    COALESCE(SUM(remote_count), 0) as remote_jobs
                FROM monthly_snapshots
                GROUP BY year, month
                ORDER BY year DESC, month DESC
                LIMIT 12
            """)
            
            trends = [dict(row) for row in cursor]
            
            return jsonify({
                'trends': trends
            })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/search')
def api_search():
    """Search companies by name."""
    try:
        query = request.args.get('q', '')
        if not query or len(query) < 2:
            return jsonify({'error': 'Query too short'}), 400
        
        db = get_db()
        with db.get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM companies 
                WHERE company_name ILIKE %s OR token ILIKE %s
                ORDER BY job_count DESC
                LIMIT 50
            """, (f'%{query}%', f'%{query}%'))
            
            companies = [dict(row) for row in cursor]
            return jsonify({
                'count': len(companies),
                'companies': companies
            })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/seeds', methods=['GET', 'POST'])
def api_seeds():
    """Get or add seed companies."""
    try:
        db = get_db()
        
        if request.method == 'POST':
            # Add new seed companies
            data = request.get_json()
            companies = data.get('companies', [])
            source = data.get('source', 'api')
            
            if not companies:
                return jsonify({'error': 'No companies provided'}), 400
            
            added = db.save_seed_companies(companies, source)
            
            return jsonify({
                'status': 'success',
                'added': added,
                'total_submitted': len(companies)
            })
        
        else:
            # Get seed companies
            with db.get_cursor() as cursor:
                cursor.execute("""
                    SELECT name, source, tested, has_greenhouse, has_lever
                    FROM seed_companies
                    ORDER BY discovered_at DESC
                    LIMIT 500
                """)
                seeds = [dict(row) for row in cursor]
                
                cursor.execute("SELECT COUNT(*) as count FROM seed_companies")
                total = cursor.fetchone()['count']
                
                return jsonify({
                    'count': len(seeds),
                    'total': total,
                    'seeds': seeds
                })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/expand-seeds', methods=['POST'])
def api_expand_seeds():
    """Trigger seed expansion from specified tier(s)."""
    global collection_state
    
    if collection_state['running']:
        return jsonify({
            'status': 'error',
            'message': 'Collection already running'
        }), 409
    
    data = request.get_json() or {}
    tier = data.get('tier', 'all')  # 'tier1', 'tier2', or 'all'
    
    def run_expansion():
        import asyncio
        from seed_expander import run_tier1_expansion, run_tier2_expansion, run_full_expansion
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            if tier == 'tier1':
                results = loop.run_until_complete(run_tier1_expansion())
                logger.info(f"Tier 1 expansion complete")
            elif tier == 'tier2':
                results = loop.run_until_complete(run_tier2_expansion())
                logger.info(f"Tier 2 expansion complete")
            else:
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
    
    # Start scheduler in background
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()
    logger.info("Background scheduler started")
    
    # Start Flask
    port = int(os.environ.get('PORT', 8080))
    logger.info(f"Starting web server on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)


if __name__ == '__main__':
    main()
