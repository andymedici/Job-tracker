"""
Database Module for PostgreSQL
==============================
Handles all database operations with PostgreSQL for Railway deployment.
"""

import os
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from contextlib import contextmanager
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, database_url: str = None):
        self.database_url = database_url or os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is required")
        
        self.conn_params = self._parse_database_url(self.database_url)
        self.pool = pool.ThreadedConnectionPool(minconn=1, maxconn=10, **self.conn_params)
        self._init_schema()
    
    def _parse_database_url(self, url: str) -> Dict[str, Any]:
        parsed = urlparse(url)
        return {
            'host': parsed.hostname,
            'port': parsed.port or 5432,
            'database': parsed.path[1:],
            'user': parsed.username,
            'password': parsed.password,
            'sslmode': 'require'
        }
    
    @contextmanager
    def get_cursor(self, dict_cursor: bool = True):
        conn = self.pool.getconn()
        try:
            cursor_factory = RealDictCursor if dict_cursor else None
            cursor = conn.cursor(cursor_factory=cursor_factory)
            yield cursor
            conn.commit()
            cursor.close()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            self.pool.putconn(conn)
    
    def _init_schema(self):
        with self.get_cursor() as cursor:
            # Companies Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    id TEXT PRIMARY KEY,
                    ats_type TEXT NOT NULL,
                    token TEXT NOT NULL,
                    company_name TEXT NOT NULL,
                    job_count INTEGER DEFAULT 0,
                    remote_count INTEGER DEFAULT 0,
                    hybrid_count INTEGER DEFAULT 0,
                    onsite_count INTEGER DEFAULT 0,
                    last_job_count INTEGER DEFAULT 0,
                    locations JSONB,
                    departments JSONB,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ats_type, token)
                )
            """)
            
            # Jobs Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    company_id TEXT REFERENCES companies(id) ON DELETE CASCADE,
                    title TEXT NOT NULL,
                    location TEXT,
                    department TEXT,
                    work_type TEXT,
                    url TEXT,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Seed Companies (Queue)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS seed_companies (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    token_slug TEXT, -- UPGRADE: Pre-calculated slug
                    source TEXT,
                    source_tier INTEGER DEFAULT 2,
                    priority INTEGER DEFAULT 50,
                    tested_count INTEGER DEFAULT 0, -- FIX: Ensure this column exists
                    hit_count INTEGER DEFAULT 0,
                    last_tested TIMESTAMP,
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Source Stats (FIX: Ensure schema matches queries)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS source_stats (
                    source TEXT PRIMARY KEY,
                    tier INTEGER DEFAULT 2,
                    seeds_discovered INTEGER DEFAULT 0,
                    seeds_tested INTEGER DEFAULT 0, -- FIX: Used in app.py
                    seeds_found INTEGER DEFAULT 0,
                    hit_rate REAL DEFAULT 0.0,
                    enabled BOOLEAN DEFAULT TRUE,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Snapshots (6h granular)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS snapshots_6h (
                    id SERIAL PRIMARY KEY,
                    company_id TEXT NOT NULL,
                    snapshot_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    job_count INTEGER,
                    remote_count INTEGER,
                    hybrid_count INTEGER,
                    onsite_count INTEGER
                )
            """)
            
            # Monthly Snapshots
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS monthly_snapshots (
                    id SERIAL PRIMARY KEY,
                    company_id TEXT NOT NULL,
                    year INTEGER,
                    month INTEGER,
                    job_count INTEGER,
                    remote_count INTEGER,
                    hybrid_count INTEGER,
                    onsite_count INTEGER,
                    snapshot_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(company_id, year, month)
                )
            """)

            # Market Intel Tables
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS location_expansions (
                    id SERIAL PRIMARY KEY,
                    company_id TEXT NOT NULL,
                    new_location TEXT,
                    job_count_at_detection INTEGER,
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(company_id, new_location)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_count_changes (
                    id SERIAL PRIMARY KEY,
                    company_id TEXT NOT NULL,
                    previous_count INTEGER,
                    current_count INTEGER,
                    change_percent REAL,
                    change_type TEXT,
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Job History Archive (for diffing)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_history_archive (
                    id SERIAL PRIMARY KEY,
                    company_id TEXT NOT NULL,
                    archive_date DATE NOT NULL,
                    locations_json JSONB,
                    UNIQUE(company_id, archive_date)
                )
            """)

    # --- CORE UPSERT METHODS ---

    def upsert_company(self, data: Dict):
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO companies (
                    id, ats_type, token, company_name, job_count, remote_count, 
                    hybrid_count, onsite_count, last_job_count, locations, departments, 
                    last_seen, last_updated
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (id) DO UPDATE SET
                    last_job_count = companies.job_count,
                    job_count = EXCLUDED.job_count,
                    remote_count = EXCLUDED.remote_count,
                    hybrid_count = EXCLUDED.hybrid_count,
                    onsite_count = EXCLUDED.onsite_count,
                    locations = EXCLUDED.locations,
                    departments = EXCLUDED.departments,
                    last_seen = NOW(),
                    last_updated = NOW()
            """, (
                data['id'], data['ats_type'], data['token'], data['company_name'],
                data['job_count'], data['remote_count'], data['hybrid_count'], data['onsite_count'],
                data['job_count'], # Initial last_job_count
                json.dumps(data.get('locations', [])),
                json.dumps(data.get('departments', []))
            ))

    def upsert_seed_company(self, name: str, token_slug: str, source: str, tier: int, priority: int):
        """Insert seed with token slug logic."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO seed_companies (name, token_slug, source, source_tier, priority)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (name) DO NOTHING
                """, (name, token_slug, source, tier, priority))
                
                # Update source stats discovery count
                if cursor.rowcount > 0:
                    cursor.execute("""
                        INSERT INTO source_stats (source, tier, seeds_discovered) 
                        VALUES (%s, %s, 1)
                        ON CONFLICT (source) DO UPDATE SET 
                            seeds_discovered = source_stats.seeds_discovered + 1
                    """, (source, tier))
        except Exception as e:
            logger.error(f"Seed upsert error: {e}")

    # --- SEED FETCHING FOR COLLECTOR ---

    def get_seeds_for_collection(self, limit: int = 500) -> List[Tuple[int, str, str, str]]:
        """
        Returns list of (id, name, token_slug, source).
        Prioritizes: Untested -> High Priority -> Oldest Tested
        """
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT id, name, token_slug, source 
                FROM seed_companies
                WHERE (last_tested IS NULL OR last_tested < NOW() - INTERVAL '7 days')
                AND tested_count < 5
                ORDER BY 
                    CASE WHEN tested_count = 0 THEN 0 ELSE 1 END ASC,
                    priority DESC,
                    last_tested ASC
                LIMIT %s
            """, (limit,))
            # Ensure token_slug is populated (fallback to name if null)
            return [(r['id'], r['name'], r['token_slug'] or r['name'].lower(), r['source']) for r in cursor]

    def mark_seed_result(self, seed_id: int, source: str, found: bool):
        """Updates seed metrics and source stats."""
        with self.get_cursor() as cursor:
            # Update seed
            cursor.execute("""
                UPDATE seed_companies 
                SET tested_count = tested_count + 1,
                    hit_count = hit_count + %s,
                    last_tested = NOW()
                WHERE id = %s
            """, (1 if found else 0, seed_id))
            
            # Update Source Stats
            cursor.execute("""
                UPDATE source_stats
                SET seeds_tested = seeds_tested + 1,
                    seeds_found = seeds_found + %s,
                    hit_rate = (seeds_found + %s)::decimal / (seeds_tested + 1),
                    last_updated = NOW()
                WHERE source = %s
            """, (1 if found else 0, 1 if found else 0, source))

    # --- ANALYTICS QUERIES FOR APP.PY (RESTORED) ---

    def get_stats(self) -> Dict:
        """Global stats for dashboard."""
        with self.get_cursor() as cursor:
            stats = {}
            # Totals
            cursor.execute("SELECT COUNT(*) as c, SUM(job_count) as j, SUM(remote_count) as r, SUM(hybrid_count) as h, SUM(onsite_count) as o FROM companies")
            row = cursor.fetchone()
            stats['total_companies'] = row['c'] or 0
            stats['total_jobs'] = row['j'] or 0
            stats['remote_jobs'] = row['r'] or 0
            stats['hybrid_jobs'] = row['h'] or 0
            stats['onsite_jobs'] = row['o'] or 0
            
            # ATS Breakdown
            cursor.execute("SELECT ats_type, COUNT(*) as c, SUM(job_count) as j FROM companies GROUP BY ats_type")
            for r in cursor:
                stats[f"{r['ats_type']}_companies"] = r['c']
                stats[f"{r['ats_type']}_jobs"] = r['j']
            
            # Seed Stats (Using correct column names)
            cursor.execute("SELECT COUNT(*) as total, SUM(tested_count) as tested FROM seed_companies")
            s_row = cursor.fetchone()
            stats['total_seeds'] = s_row['total'] or 0
            stats['seeds_tested'] = s_row['tested'] or 0 # The fix for the error log
            
            # Top Hiring
            # FIX: Changed 'location' (singular) to 'locations' (plural JSONB column)
            cursor.execute("SELECT company_name, ats_type, locations, job_count, remote_count FROM companies ORDER BY job_count DESC LIMIT 5")
            stats['top_hiring_companies'] = list(cursor)
            
            return stats

    def get_history(self, days: int = 7) -> List[Dict]:
        """Get daily aggregates for charts."""
        cutoff = datetime.utcnow() - timedelta(days=days)
        with self.get_cursor() as cursor:
            # Union snapshots_6h (recent) with averaged monthly for older data if needed
            # Simplified: just query snapshots_6h aggregated by day
            cursor.execute("""
                SELECT 
                    DATE_TRUNC('day', snapshot_time) as timestamp,
                    SUM(job_count) as total_jobs,
                    SUM(remote_count) as remote_jobs,
                    SUM(hybrid_count) as hybrid_jobs,
                    SUM(onsite_count) as onsite_jobs,
                    SUM(CASE WHEN c.ats_type='greenhouse' THEN s.job_count ELSE 0 END) as greenhouse_jobs,
                    SUM(CASE WHEN c.ats_type='lever' THEN s.job_count ELSE 0 END) as lever_jobs
                FROM snapshots_6h s
                JOIN companies c ON s.company_id = c.id
                WHERE snapshot_time > %s
                GROUP BY 1
                ORDER BY 1
            """, (cutoff,))
            return list(cursor)

    def get_companies(self, search_term: str = "", platform_filter: str = "", sort_by: str = "jobs", limit: int = 100) -> List[Dict]:
        """Rich filtering for companies page."""
        query = "SELECT * FROM companies WHERE 1=1"
        params = []
        
        if search_term:
            query += " AND (company_name ILIKE %s OR ats_type ILIKE %s)"
            params.extend([f"%{search_term}%", f"%{search_term}%"])
        if platform_filter:
            query += " AND ats_type = %s"
            params.append(platform_filter)
            
        if sort_by == 'jobs':
            query += " ORDER BY job_count DESC"
        elif sort_by == 'remote':
            query += " ORDER BY remote_count DESC"
        elif sort_by == 'name':
            query += " ORDER BY company_name ASC"
            
        query += " LIMIT %s"
        params.append(limit)
        
        with self.get_cursor() as cursor:
            cursor.execute(query, tuple(params))
            return list(cursor)

    def get_job_count_changes(self, days: int, change_percent_threshold: float) -> Tuple[List, List]:
        """Get surges and declines."""
        cutoff = datetime.utcnow() - timedelta(days=days)
        with self.get_cursor() as cursor:
            # Surges
            cursor.execute("""
                SELECT j.*, c.company_name, c.ats_type 
                FROM job_count_changes j JOIN companies c ON j.company_id = c.id
                WHERE detected_at > %s AND change_type = 'surge'
                ORDER BY change_percent DESC LIMIT 50
            """, (cutoff,))
            surges = list(cursor)
            
            # Declines
            cursor.execute("""
                SELECT j.*, c.company_name, c.ats_type 
                FROM job_count_changes j JOIN companies c ON j.company_id = c.id
                WHERE detected_at > %s AND change_type = 'decline'
                ORDER BY change_percent ASC LIMIT 50
            """, (cutoff,))
            declines = list(cursor)
            return surges, declines

    def get_location_expansions(self, days: int) -> List[Dict]:
        cutoff = datetime.utcnow() - timedelta(days=days)
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT l.*, c.company_name, c.ats_type
                FROM location_expansions l JOIN companies c ON l.company_id = c.id
                WHERE detected_at > %s
                ORDER BY detected_at DESC LIMIT 50
            """, (cutoff,))
            return list(cursor)

    def get_location_stats(self, top_n: int = None) -> List[Dict]:
        """Unpack JSON locations and count them."""
        with self.get_cursor() as cursor:
            # This query unpacks the JSON array of locations and counts frequency
            cursor.execute("""
                SELECT 
                    TRIM(jsonb_array_elements_text(locations)) as location,
                    COUNT(*) as total_companies
                FROM companies
                WHERE locations IS NOT NULL
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT %s
            """, (top_n or 100,))
            return list(cursor)

    def get_source_stats(self) -> List[Dict]:
        with self.get_cursor() as cursor:
            cursor.execute("SELECT * FROM source_stats ORDER BY hit_rate DESC")
            return list(cursor)
    
    def get_high_performing_sources(self, min_tested=50, min_hit_rate=0.01):
        with self.get_cursor() as cursor:
            cursor.execute("SELECT source FROM source_stats WHERE seeds_tested > %s AND hit_rate > %s", (min_tested, min_hit_rate))
            return [r['source'] for r in cursor]

    # --- SNAPSHOT LOGIC ---
    def create_6h_snapshots(self):
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO snapshots_6h (company_id, job_count, remote_count, hybrid_count, onsite_count)
                SELECT id, job_count, remote_count, hybrid_count, onsite_count FROM companies
            """)
            # Cleanup > 30 days
            cursor.execute("DELETE FROM snapshots_6h WHERE snapshot_time < NOW() - INTERVAL '30 days'")

    def create_monthly_snapshot(self):
        with self.get_cursor() as cursor:
            now = datetime.utcnow()
            cursor.execute("""
                INSERT INTO monthly_snapshots (company_id, year, month, job_count, remote_count, hybrid_count, onsite_count)
                SELECT id, %s, %s, job_count, remote_count, hybrid_count, onsite_count FROM companies
                ON CONFLICT (company_id, year, month) DO UPDATE SET job_count = EXCLUDED.job_count
            """, (now.year, now.month))

# Singleton
_db_instance = None
def get_db(max_retries=15, delay=2):
    global _db_instance
    if _db_instance is None:
        for attempt in range(max_retries):
            try:
                _db_instance = Database()
                return _db_instance
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(delay)
                else:
                    raise e
    return _db_instance
