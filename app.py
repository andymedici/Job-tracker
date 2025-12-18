"""
Job Intelligence Dashboard & Scheduler
======================================
Web dashboard and background scheduler.
"""

import os
import json
import asyncio
import threading
import logging
from datetime import datetime, timedelta
from flask import Flask, jsonify, render_template, request
import schedule
import time

from database import get_db
from collector import JobIntelCollector
from market_intel import run_daily_maintenance
from seed_expander import run_full_expansion

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

state = {
    'running': False,
    'last_run': None,
    'last_stats': None
}

# --- BACKGROUND WORKERS ---

def run_async_task(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)

def job_refresh():
    if state['running']: return
    state['running'] = True
    try:
        collector = JobIntelCollector()
        stats = run_async_task(collector.run_refresh(hours=6, limit=1000))
        state['last_stats'] = stats.to_dict()
        run_daily_maintenance()
    finally:
        state['running'] = False
        state['last_run'] = datetime.utcnow().isoformat()

def job_discovery():
    if state['running']: return
    state['running'] = True
    try:
        collector = JobIntelCollector()
        stats = run_async_task(collector.run_discovery(max_companies=500))
        state['last_stats'] = stats.to_dict()
        run_daily_maintenance()
    finally:
        state['running'] = False

def job_expansion():
    run_async_task(run_full_expansion())

def scheduler_loop():
    schedule.every(6).hours.do(job_refresh)
    schedule.every().sunday.at("02:00").do(job_discovery)
    schedule.every().day.at("04:00").do(job_expansion) # AutoIngest runs daily
    
    while True:
        schedule.run_pending()
        time.sleep(60)

# --- ROUTES ---

@app.route('/')
@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

@app.route('/analytics')
def analytics():
    return render_template('analytics.html')

@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'running': state['running']})

@app.route('/api/stats')
def api_stats():
    db = get_db()
    stats = db.get_stats()
    stats['is_running'] = state['running']
    return jsonify(stats)

@app.route('/api/trends')
def api_trends():
    db = get_db()
    return jsonify({
        'granular': db.get_market_trends(),
        'monthly': db.get_monthly_snapshots()
    })

@app.route('/api/expansions')
def api_expansions():
    db = get_db()
    return jsonify({'expansions': db.get_location_expansions(7)})

@app.route('/api/collect', methods=['POST'])
def trigger_collect():
    if state['running']: return jsonify({'error': 'Running'}), 409
    threading.Thread(target=job_discovery, daemon=True).start()
    return jsonify({'status': 'started'})

@app.route('/api/refresh', methods=['POST'])
def trigger_refresh():
    if state['running']: return jsonify({'error': 'Running'}), 409
    threading.Thread(target=job_refresh, daemon=True).start()
    return jsonify({'status': 'started'})

@app.route('/api/expand-seeds', methods=['POST'])
def trigger_expand():
    threading.Thread(target=job_expansion, daemon=True).start()
    return jsonify({'status': 'started'})

if __name__ == "__main__":
    threading.Thread(target=scheduler_loop, daemon=True).start()
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
