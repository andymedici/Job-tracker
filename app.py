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

from flask import Flask, jsonify, render_template_string, request, Response
import schedule
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

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


# Dashboard HTML template
DASHBOARD_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Job Intelligence Dashboard</title>
    <meta http-equiv="refresh" content="30">
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            min-height: 100vh;
            color: #fff;
            padding: 20px;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        
        header {
            text-align: center;
            margin-bottom: 40px;
        }
        
        header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            background: linear-gradient(90deg, #00d4ff, #7b2cbf);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        
        header p {
            color: #8892b0;
            font-size: 1.1em;
        }
        
        .status-bar {
            background: rgba(255,255,255,0.05);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 30px;
            border: 1px solid rgba(255,255,255,0.1);
        }
        
        .status-bar.running {
            border-color: #ffd93d;
            background: rgba(255,217,61,0.1);
        }
        
        .status-bar.error {
            border-color: #ff6b6b;
            background: rgba(255,107,107,0.1);
        }
        
        .status-indicator {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 10px;
            animation: pulse 2s infinite;
        }
        
        .status-indicator.idle { background: #4ade80; }
        .status-indicator.running { background: #ffd93d; }
        .status-indicator.error { background: #ff6b6b; }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .metric-card {
            background: rgba(255,255,255,0.05);
            border-radius: 16px;
            padding: 25px;
            text-align: center;
            border: 1px solid rgba(255,255,255,0.1);
            transition: transform 0.2s, box-shadow 0.2s;
        }
        
        .metric-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
        }
        
        .metric-value {
            font-size: 3em;
            font-weight: 700;
            background: linear-gradient(90deg, #00d4ff, #7b2cbf);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        
        .metric-label {
            color: #8892b0;
            margin-top: 10px;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        .platform-cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .platform-card {
            background: rgba(255,255,255,0.05);
            border-radius: 16px;
            padding: 25px;
            border: 1px solid rgba(255,255,255,0.1);
        }
        
        .platform-card.greenhouse {
            border-left: 4px solid #4ade80;
        }
        
        .platform-card.lever {
            border-left: 4px solid #f97316;
        }
        
        .platform-card h3 {
            font-size: 1.3em;
            margin-bottom: 15px;
        }
        
        .platform-stat {
            display: flex;
            justify-content: space-between;
            padding: 10px 0;
            border-bottom: 1px solid rgba(255,255,255,0.05);
        }
        
        .platform-stat:last-child {
            border-bottom: none;
        }
        
        .actions {
            display: flex;
            gap: 15px;
            justify-content: center;
            margin-bottom: 30px;
        }
        
        .btn {
            padding: 12px 30px;
            border-radius: 8px;
            border: none;
            cursor: pointer;
            font-size: 1em;
            font-weight: 600;
            transition: all 0.2s;
            text-decoration: none;
            display: inline-block;
        }
        
        .btn-primary {
            background: linear-gradient(90deg, #00d4ff, #7b2cbf);
            color: white;
        }
        
        .btn-primary:hover {
            transform: scale(1.05);
            box-shadow: 0 5px 20px rgba(0,212,255,0.4);
        }
        
        .btn-primary:disabled {
            opacity: 0.5;
            cursor: not-allowed;
            transform: none;
        }
        
        .btn-secondary {
            background: rgba(255,255,255,0.1);
            color: white;
            border: 1px solid rgba(255,255,255,0.2);
        }
        
        .btn-secondary:hover {
            background: rgba(255,255,255,0.2);
        }
        
        .footer {
            text-align: center;
            color: #8892b0;
            font-size: 0.9em;
            padding: 20px;
        }
        
        .footer a {
            color: #00d4ff;
            text-decoration: none;
        }
        
        .recent-info {
            background: rgba(255,255,255,0.03);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 30px;
        }
        
        .recent-info h3 {
            margin-bottom: 15px;
            color: #8892b0;
        }
        
        @media (max-width: 768px) {
            header h1 { font-size: 1.8em; }
            .metric-value { font-size: 2em; }
            .metrics-grid { grid-template-columns: repeat(2, 1fr); }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🔍 Job Intelligence Dashboard</h1>
            <p>Real-time job market monitoring across ATS platforms</p>
        </header>
        
        <div class="status-bar {{ 'running' if state.running else 'error' if state.error else '' }}">
            <span class="status-indicator {{ 'running' if state.running else 'error' if state.error else 'idle' }}"></span>
            <strong>Status:</strong> 
            {% if state.running %}
                Collection in progress...
            {% elif state.error %}
                Error: {{ state.error }}
            {% else %}
                Idle - Ready for collection
            {% endif %}
            
            {% if state.last_run %}
                <span style="float: right; color: #8892b0;">
                    Last run: {{ state.last_run }}
                </span>
            {% endif %}
        </div>
        
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-value">{{ "{:,}".format(stats.total_companies or 0) }}</div>
                <div class="metric-label">Total Companies</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{{ "{:,}".format(stats.total_jobs or 0) }}</div>
                <div class="metric-label">Total Jobs</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{{ stats.greenhouse_companies or 0 }}</div>
                <div class="metric-label">Greenhouse</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{{ stats.lever_companies or 0 }}</div>
                <div class="metric-label">Lever</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{{ stats.updated_last_24h or 0 }}</div>
                <div class="metric-label">Updated (24h)</div>
            </div>
        </div>
        
        <div class="platform-cards">
            <div class="platform-card greenhouse">
                <h3>🌱 Greenhouse Platform</h3>
                <div class="platform-stat">
                    <span>Companies</span>
                    <strong>{{ stats.greenhouse_companies or 0 }}</strong>
                </div>
                <div class="platform-stat">
                    <span>Job Openings</span>
                    <strong>{{ "{:,}".format(stats.greenhouse_jobs or 0) }}</strong>
                </div>
                <div class="platform-stat">
                    <span>Avg Jobs/Company</span>
                    <strong>{{ "%.1f"|format((stats.greenhouse_jobs or 0) / max(stats.greenhouse_companies or 1, 1)) }}</strong>
                </div>
            </div>
            
            <div class="platform-card lever">
                <h3>🔧 Lever Platform</h3>
                <div class="platform-stat">
                    <span>Companies</span>
                    <strong>{{ stats.lever_companies or 0 }}</strong>
                </div>
                <div class="platform-stat">
                    <span>Job Openings</span>
                    <strong>{{ "{:,}".format(stats.lever_jobs or 0) }}</strong>
                </div>
                <div class="platform-stat">
                    <span>Avg Jobs/Company</span>
                    <strong>{{ "%.1f"|format((stats.lever_jobs or 0) / max(stats.lever_companies or 1, 1)) }}</strong>
                </div>
            </div>
        </div>
        
        <div class="actions">
            <button class="btn btn-primary" onclick="triggerCollection()" {{ 'disabled' if state.running }}>
                {{ '⏳ Running...' if state.running else '🚀 Run Collection' }}
            </button>
            <button class="btn btn-secondary" onclick="expandSeeds()">🌱 Expand Seeds</button>
            <a href="/api/stats" class="btn btn-secondary">📊 API Stats</a>
            <a href="/api/companies" class="btn btn-secondary">🏢 Companies</a>
            <a href="/api/seeds" class="btn btn-secondary">🌱 Seeds</a>
        </div>
        
        {% if state.last_stats %}
        <div class="recent-info">
            <h3>📈 Last Collection Results</h3>
            <p>Duration: {{ "%.1f"|format(state.last_stats.duration_minutes or 0) }} minutes</p>
            <p>Companies Tested: {{ state.last_stats.companies_tested or 0 }}</p>
            <p>New Greenhouse: {{ state.last_stats.greenhouse_found or 0 }}</p>
            <p>New Lever: {{ state.last_stats.lever_found or 0 }}</p>
            <p>Discovery Rate: {{ state.last_stats.discovery_rate or 'N/A' }}</p>
        </div>
        {% endif %}
        
        {% if state.last_intel %}
        <div class="recent-info" style="border-left: 4px solid #f97316;">
            <h3>🔍 Market Intelligence</h3>
            <p>📍 Location Expansions: {{ state.last_intel.expansions or 0 }} detected</p>
            <p>📈 Hiring Surges: {{ state.last_intel.surges or 0 }} companies</p>
            <p>📉 Hiring Declines: {{ state.last_intel.declines or 0 }} companies</p>
            <p style="margin-top: 10px;">
                <a href="/api/intelligence" style="color: #00d4ff;">View Full Report →</a> |
                <a href="/api/expansions" style="color: #00d4ff;">Expansions</a> |
                <a href="/api/changes?type=surge" style="color: #00d4ff;">Surges</a> |
                <a href="/api/changes?type=decline" style="color: #00d4ff;">Declines</a>
            </p>
        </div>
        {% endif %}
        
        <div class="footer">
            <p>Auto-refresh every 30 seconds | Last updated: {{ stats.last_updated }}</p>
            <p><a href="/health">Health Check</a> | <a href="/api/companies?limit=100">Export Data</a></p>
        </div>
    </div>
    
    <script>
        async function triggerCollection() {
            if (confirm('Start a new collection run? This may take several minutes.')) {
                const btn = document.querySelector('.btn-primary');
                btn.disabled = true;
                btn.textContent = '⏳ Starting...';
                
                try {
                    const response = await fetch('/api/collect', { method: 'POST' });
                    const data = await response.json();
                    alert(data.message || 'Collection started!');
                    setTimeout(() => location.reload(), 2000);
                } catch (e) {
                    alert('Error starting collection: ' + e.message);
                    btn.disabled = false;
                    btn.textContent = '🚀 Run Collection';
                }
            }
        }
        
        async function expandSeeds() {
            if (confirm('Expand seed database from YC, GitHub, and other sources? This fetches 2000+ new company names.')) {
                try {
                    const response = await fetch('/api/expand-seeds', { method: 'POST' });
                    const data = await response.json();
                    alert(data.message || 'Seed expansion started!');
                } catch (e) {
                    alert('Error: ' + e.message);
                }
            }
        }
    </script>
</body>
</html>
"""


@app.route('/')
def dashboard():
    """Main dashboard view."""
    stats = get_stats()
    return render_template_string(
        DASHBOARD_TEMPLATE,
        stats=stats,
        state=collection_state
    )


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
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/expand-seeds', methods=['POST'])
def api_expand_seeds():
    """Trigger seed expansion from all sources."""
    global collection_state
    
    if collection_state['running']:
        return jsonify({
            'status': 'error',
            'message': 'Collection already running'
        }), 409
    
    def run_expansion():
        import asyncio
        from seed_expander import SeedExpander
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            expander = SeedExpander()
            results = loop.run_until_complete(expander.expand_all())
            
            # Save to database
            for source, companies in results.items():
                if source != 'total_unique' and companies:
                    expander.save_to_db(companies, source)
            
            loop.run_until_complete(expander.close())
            logger.info(f"Seed expansion complete: {len(results.get('total_unique', []))} unique companies")
        finally:
            loop.close()
    
    thread = threading.Thread(target=run_expansion, daemon=True)
    thread.start()
    
    return jsonify({
        'status': 'success',
        'message': 'Seed expansion started in background'
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
    logger.info(f"Starting web server on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)


if __name__ == '__main__':
    main()
