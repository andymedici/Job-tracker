"""
Database Module for PostgreSQL
==============================
Handles all database operations with PostgreSQL for Railway deployment.
Updated with Job Hashing and Snowball Queue.
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
            
            # Jobs Table (Updated with Hash for Ghost Job Filtering)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    company_id TEXT REFERENCES companies(id) ON DELETE CASCADE,
                    title TEXT NOT NULL,
                    location TEXT,
                    department TEXT,
                    work_type TEXT,
                    url TEXT,
                    job_hash TEXT, -- New: Hash of title+loc+company to track uniqueness
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_hash ON jobs(job_hash)")
            
            # Seed Companies
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS seed_companies (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    token_slug TEXT,
                    source TEXT,
                    source_tier INTEGER DEFAULT 2,
                    priority INTEGER DEFAULT 50,
                    tested_count INTEGER DEFAULT 0,
                    hit_count INTEGER DEFAULT 0,
                    last_tested TIMESTAMP,
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Snowball Queue (New)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS snowball_queue (
                    id SERIAL PRIMARY KEY,
                    domain TEXT UNIQUE NOT NULL,
                    found_via TEXT,
                    status TEXT DEFAULT 'pending', -- pending, processed, ignored
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Source Stats
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS source_stats (
                    source TEXT PRIMARY KEY,
                    tier INTEGER DEFAULT 2,
                    seeds_discovered INTEGER DEFAULT 0,
                    seeds_tested INTEGER DEFAULT 0,
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
            
            # Job History Archive
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_history_archive (
                    id SERIAL PRIMARY KEY,
                    company_id TEXT NOT NULL,
                    archive_date DATE NOT NULL,
                    locations_json JSONB,
                    departments_json JSONB,
                    job_count INTEGER,
                    UNIQUE(company_id, archive_date)
                )
            """)

    # --- UPSERT METHODS ---

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
                data['job_count'],
                json.dumps(data.get('locations', [])),
                json.dumps(data.get('departments', []))
            ))

    def upsert_seed_companies(self, companies: List[str], source: str, tier: int, priority: int) -> int:
        added = 0
        with self.get_cursor() as cursor:
            for name in companies:
                token_slug = name.lower().strip().replace(' ', '-').replace('.', '')
                try:
                    cursor.execute("""
                        INSERT INTO seed_companies (name, token_slug, source, source_tier, priority)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (name) DO NOTHING
                    """, (name, token_slug, source, tier, priority))
                    if cursor.rowcount > 0:
                        added += 1
                except Exception:
                    continue
            
            if added > 0:
                cursor.execute("""
                    INSERT INTO source_stats (source, tier, seeds_discovered) 
                    VALUES (%s, %s, %s)
                    ON CONFLICT (source) DO UPDATE SET 
                        seeds_discovered = source_stats.seeds_discovered + %s,
                        last_updated = NOW()
                """, (source, tier, added, added))
        return added
    
    def upsert_snowball_domain(self, domain: str, found_via: str):
        """Add a discovered domain to the snowball queue."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO snowball_queue (domain, found_via)
                VALUES (%s, %s)
                ON CONFLICT (domain) DO NOTHING
            """, (domain, found_via))

    # --- FETCH METHODS ---

    def get_seeds_for_collection(self, limit: int = 500) -> List[Tuple[int, str, str, str]]:
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
            return [(r['id'], r['name'], r['token_slug'] or r['name'].lower(), r['source']) for r in cursor]

    def mark_seeds_tested(self, seed_ids: List[int], tested_at: datetime):
        if not seed_ids: return
        with self.get_cursor() as cursor:
            cursor.execute("UPDATE seed_companies SET tested_count = tested_count + 1, last_tested = %s WHERE id = ANY(%s)", (tested_at, seed_ids))
            
            cursor.execute("""
                UPDATE source_stats SET seeds_tested = seeds_tested + 1 
                WHERE source IN (SELECT source FROM seed_companies WHERE id = ANY(%s))
            """, (seed_ids,))

    def mark_seed_hit(self, seed_id: int):
        with self.get_cursor() as cursor:
            cursor.execute("UPDATE seed_companies SET hit_count = hit_count + 1 WHERE id = %s RETURNING source", (seed_id,))
            row = cursor.fetchone()
            if row:
                cursor.execute("""
                    UPDATE source_stats 
                    SET seeds_found = seeds_found + 1,
                        hit_rate = (seeds_found + 1.0) / NULLIF(seeds_tested, 0)
                    WHERE source = %s
                """, (row['source'],))

    # --- ANALYTICS ---

    def get_stats(self) -> Dict:
        with self.get_cursor() as cursor:
            stats = {}
            cursor.execute("SELECT COUNT(*) as c, SUM(job_count) as j FROM companies")
            row = cursor.fetchone()
            stats['total_companies'] = row['c'] or 0
            stats['total_jobs'] = row['j'] or 0
            
            cursor.execute("SELECT COUNT(*) as t FROM seed_companies")
            stats['total_seeds'] = cursor.fetchone()['t'] or 0
            
            cursor.execute("SELECT COUNT(*) as t FROM seed_companies WHERE tested_count = 0")
            stats['untested_seeds'] = cursor.fetchone()['t'] or 0

            return stats

    def get_market_trends(self, days: int = 7) -> List[Dict]:
        cutoff = datetime.utcnow() - timedelta(days=days)
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    DATE_TRUNC('day', snapshot_time) as timestamp,
                    SUM(job_count) as total_jobs,
                    SUM(remote_count) as remote_jobs
                FROM snapshots_6h 
                WHERE snapshot_time >= %s
                GROUP BY 1 ORDER BY 1
            """, (cutoff,))
            return list(cursor)

    def get_company_trends(self, company_id: str, days: int = 7) -> List[Dict]:
        cutoff = datetime.utcnow() - timedelta(days=days)
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT snapshot_time as timestamp, job_count 
                FROM snapshots_6h 
                WHERE company_id = %s AND snapshot_time >= %s
                ORDER BY snapshot_time ASC
            """, (company_id, cutoff))
            return list(cursor)

    def get_monthly_snapshots(self) -> List[Dict]:
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    TO_DATE(CONCAT(year, '-', month, '-01'), 'YYYY-MM-DD') as month,
                    SUM(job_count) as total_jobs
                FROM monthly_snapshots
                GROUP BY 1 ORDER BY 1 DESC LIMIT 12
            """)
            return list(cursor)
    
    def get_companies(self, search_term: str = "", platform_filter: str = "", sort_by: str = "jobs", limit: int = 100) -> List[Dict]:
        query = "SELECT * FROM companies WHERE 1=1"
        params = []
        if search_term:
            query += " AND (company_name ILIKE %s OR ats_type ILIKE %s)"
            params.extend([f"%{search_term}%", f"%{search_term}%"])
        if platform_filter:
            query += " AND ats_type = %s"
            params.append(platform_filter)
        
        if sort_by == 'jobs': query += " ORDER BY job_count DESC"
        elif sort_by == 'remote': query += " ORDER BY remote_count DESC"
        elif sort_by == 'name': query += " ORDER BY company_name ASC"
        
        query += " LIMIT %s"
        params.append(limit)
        
        with self.get_cursor() as cursor:
            cursor.execute(query, tuple(params))
            return list(cursor)

    def get_companies_for_refresh(self, hours_old: int, limit: int) -> List[Dict]:
        cutoff = datetime.utcnow() - timedelta(hours=hours_old)
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT id, company_name, ats_type, token 
                FROM companies 
                WHERE last_updated < %s 
                ORDER BY last_updated ASC 
                LIMIT %s
            """, (cutoff, limit))
            return list(cursor)

    def get_job_count_changes(self, days: int) -> Tuple[List, List]:
        cutoff = datetime.utcnow() - timedelta(days=days)
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT j.*, c.company_name, c.ats_type 
                FROM job_count_changes j JOIN companies c ON j.company_id = c.id
                WHERE detected_at >= %s AND change_type = 'surge'
                ORDER BY detected_at DESC LIMIT 50
            """, (cutoff,))
            surges = list(cursor)
            
            cursor.execute("""
                SELECT j.*, c.company_name, c.ats_type 
                FROM job_count_changes j JOIN companies c ON j.company_id = c.id
                WHERE detected_at >= %s AND change_type = 'decline'
                ORDER BY detected_at DESC LIMIT 50
            """, (cutoff,))
            declines = list(cursor)
            return surges, declines

    def get_location_expansions(self, days: int) -> List[Dict]:
        cutoff = datetime.utcnow() - timedelta(days=days)
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT l.*, c.company_name, c.ats_type
                FROM location_expansions l JOIN companies c ON l.company_id = c.id
                WHERE detected_at >= %s
                ORDER BY detected_at DESC LIMIT 50
            """, (cutoff,))
            return list(cursor)
    
    def get_location_stats(self, top_n: int = 100) -> List[Dict]:
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    TRIM(jsonb_array_elements_text(locations)) as location,
                    COUNT(*) as count
                FROM companies
                WHERE locations IS NOT NULL
                GROUP BY 1 ORDER BY 2 DESC LIMIT %s
            """, (top_n,))
            return list(cursor)

    def get_seeds(self, limit: int = 100, source_filter: str = None) -> List[Dict]:
        query = "SELECT * FROM seed_companies WHERE 1=1"
        params = []
        if source_filter:
            query += " AND source = %s"
            params.append(source_filter)
        query += " ORDER BY discovered_at DESC LIMIT %s"
        params.append(limit)
        
        with self.get_cursor() as cursor:
            cursor.execute(query, tuple(params))
            return list(cursor)

    def add_manual_seed(self, name: str) -> bool:
        try:
            token = name.lower().strip().replace(' ', '-').replace('.', '')
            with self.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO seed_companies (name, token_slug, source, source_tier, priority)
                    VALUES (%s, %s, 'manual', 3, 100)
                    ON CONFLICT (name) DO NOTHING
                """, (name, token))
            return True
        except Exception as e:
            logger.error(f"Error adding manual seed: {e}")
            return False

    def get_source_stats(self) -> List[Dict]:
        with self.get_cursor() as cursor:
            cursor.execute("SELECT * FROM source_stats ORDER BY hit_rate DESC")
            return list(cursor)

    def get_high_performing_sources(self, min_tested=50, min_hit_rate=0.01) -> List[str]:
        with self.get_cursor() as cursor:
            cursor.execute("SELECT source FROM source_stats WHERE seeds_tested > %s AND hit_rate > %s", (min_tested, min_hit_rate))
            return [r['source'] for r in cursor]

    def create_monthly_snapshot(self):
        with self.get_cursor() as cursor:
            now = datetime.utcnow()
            cursor.execute("""
                INSERT INTO monthly_snapshots (company_id, year, month, job_count, remote_count, hybrid_count, onsite_count)
                SELECT id, %s, %s, job_count, remote_count, hybrid_count, onsite_count FROM companies
                ON CONFLICT (company_id, year, month) DO UPDATE SET job_count = EXCLUDED.job_count
            """, (now.year, now.month))
    
    def create_6h_snapshots(self):
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO snapshots_6h (company_id, job_count, remote_count, hybrid_count, onsite_count)
                SELECT id, job_count, remote_count, hybrid_count, onsite_count FROM companies
            """)
            cursor.execute("DELETE FROM snapshots_6h WHERE snapshot_time < NOW() - INTERVAL '30 days'")

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
