import os
import json
import logging
import time
import hashlib
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
            with conn.cursor(cursor_factory=cursor_factory) as cursor:
                yield cursor
                conn.commit()
        finally:
            self.pool.putconn(conn)

    def _init_schema(self):
        with self.get_cursor(dict_cursor=False) as cursor:
            # 1. Seeds Table (no change)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS seeds (
                    id SERIAL PRIMARY KEY,
                    company_name TEXT NOT NULL UNIQUE,
                    token_slug TEXT NOT NULL,
                    source TEXT NOT NULL,
                    tier INTEGER NOT NULL,
                    last_expanded TIMESTAMP,
                    last_tested TIMESTAMP,
                    is_hit BOOLEAN DEFAULT FALSE,
                    enabled BOOLEAN DEFAULT TRUE,
                    hit_rate REAL DEFAULT 0.0,
                    total_tested INTEGER DEFAULT 0,
                    total_hits INTEGER DEFAULT 0
                )
            """)
            
            # 2. Companies Table (UPDATED for Granular Locations and Skills)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    id TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL UNIQUE,
                    ats_type TEXT NOT NULL,
                    token TEXT NOT NULL,
                    job_count INTEGER DEFAULT 0,
                    remote_count INTEGER DEFAULT 0,
                    hybrid_count INTEGER DEFAULT 0,
                    onsite_count INTEGER DEFAULT 0,
                    locations JSONB DEFAULT '[]'::jsonb,
                    departments JSONB DEFAULT '[]'::jsonb,
                    normalized_locations JSONB DEFAULT '{}'::jsonb, -- NEW: {city: {name: count}, region: {name: count}, country: {name: count}}
                    extracted_skills JSONB DEFAULT '{}'::jsonb,      -- NEW: {skill: count, ...}
                    first_discovered TIMESTAMP DEFAULT NOW(),
                    last_updated TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # 3. Snapshots Tables (no change)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS snapshots_6h (
                    id SERIAL PRIMARY KEY,
                    snapshot_time TIMESTAMP DEFAULT NOW(),
                    company_id TEXT NOT NULL,
                    job_count INTEGER,
                    remote_count INTEGER,
                    hybrid_count INTEGER,
                    onsite_count INTEGER
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS monthly_snapshots (
                    id SERIAL PRIMARY KEY,
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    company_id TEXT NOT NULL,
                    job_count INTEGER,
                    remote_count INTEGER,
                    hybrid_count INTEGER,
                    onsite_count INTEGER,
                    UNIQUE (company_id, year, month)
                )
            """)

            # 4. Job Archive Table (NEW: for Hashing and Time-to-Fill)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_archive (
                    job_hash TEXT PRIMARY KEY,
                    company_id TEXT NOT NULL,
                    job_title TEXT,
                    city TEXT,
                    region TEXT,
                    country TEXT,
                    work_type TEXT, -- remote, hybrid, onsite
                    skills TEXT[] DEFAULT '{}'::TEXT[],
                    first_seen TIMESTAMP NOT NULL,
                    last_seen TIMESTAMP NOT NULL,
                    status TEXT DEFAULT 'open', -- open, closed
                    time_to_fill INTEGER -- NEW: in days
                )
            """)

    def upsert_company(self, data: Dict[str, Any]):
        """Upsert company data into the companies table."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO companies (
                    id, company_name, ats_type, token, job_count, 
                    remote_count, hybrid_count, onsite_count, locations, 
                    departments, normalized_locations, extracted_skills, 
                    last_updated
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (id) DO UPDATE SET
                    job_count = EXCLUDED.job_count,
                    remote_count = EXCLUDED.remote_count,
                    hybrid_count = EXCLUDED.hybrid_count,
                    onsite_count = EXCLUDED.onsite_count,
                    locations = EXCLUDED.locations,
                    departments = EXCLUDED.departments,
                    normalized_locations = EXCLUDED.normalized_locations,
                    extracted_skills = EXCLUDED.extracted_skills,
                    last_updated = NOW()
            """, (
                data['id'], data['company_name'], data['ats_type'], data['token'],
                data['job_count'], data['remote_count'], data['hybrid_count'],
                data['onsite_count'], json.dumps(data['locations']),
                json.dumps(data['departments']), json.dumps(data['normalized_locations']),
                json.dumps(data['extracted_skills'])
            ))
    
    # --- JOB ARCHIVE METHODS (NEW) ---
    def archive_jobs(self, company_id: str, jobs: List[Dict]):
        """
        Inserts new job hashes and updates the last_seen time for active jobs.
        """
        now = datetime.utcnow()
        with self.get_cursor(dict_cursor=False) as cursor:
            for job in jobs:
                # job['hash'] is the job_hash
                cursor.execute("""
                    INSERT INTO job_archive (
                        job_hash, company_id, job_title, city, region, country, 
                        work_type, skills, first_seen, last_seen
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (job_hash) DO UPDATE SET
                        last_seen = EXCLUDED.last_seen
                """, (
                    job['hash'], company_id, job.get('title'), job.get('city'), 
                    job.get('region'), job.get('country'), job.get('work_type'), 
                    job.get('skills', []), now, now
                ))

    def mark_stale_jobs_closed(self, company_id: str, refresh_time: datetime):
        """
        Marks any job in job_archive for a company that was not seen during the 
        last refresh run as 'closed' and calculates time_to_fill.
        """
        # We assume any job where last_seen < refresh_time (the start of the
        # collector run) that is currently 'open' must have been closed.
        with self.get_cursor(dict_cursor=False) as cursor:
            # First, update time_to_fill and status
            cursor.execute("""
                UPDATE job_archive
                SET 
                    status = 'closed',
                    time_to_fill = EXTRACT(DAY FROM (%s - first_seen))
                WHERE 
                    company_id = %s 
                    AND status = 'open' 
                    AND last_seen < %s
            """, (refresh_time, company_id, refresh_time))
            
            logger.info(f"Closed {cursor.rowcount} stale jobs for {company_id}.")
            return cursor.rowcount

    # --- TIME-TO-FILL METRICS (NEW) ---
    def get_time_to_fill_metrics(self) -> Dict[str, Any]:
        """Calculates average time-to-fill metrics across different groupings."""
        metrics = {}
        with self.get_cursor() as cursor:
            
            # 1. Overall Average Time-to-Fill
            cursor.execute("""
                SELECT AVG(time_to_fill) AS avg_ttf
                FROM job_archive 
                WHERE status = 'closed' AND time_to_fill IS NOT NULL
            """)
            metrics['overall_avg_ttf_days'] = cursor.fetchone().get('avg_ttf')
            
            # 2. Time-to-Fill by Work Type
            cursor.execute("""
                SELECT work_type, AVG(time_to_fill) AS avg_ttf
                FROM job_archive 
                WHERE status = 'closed' AND time_to_fill IS NOT NULL
                GROUP BY work_type
            """)
            metrics['avg_ttf_by_work_type'] = {
                row['work_type']: row['avg_ttf'] for row in cursor.fetchall()
            }
            
            # 3. Time-to-Fill by Top 5 Countries (using job_archive country field)
            cursor.execute("""
                SELECT country, AVG(time_to_fill) AS avg_ttf, COUNT(*) as closed_count
                FROM job_archive 
                WHERE status = 'closed' AND time_to_fill IS NOT NULL AND country IS NOT NULL
                GROUP BY country
                ORDER BY closed_count DESC
                LIMIT 5
            """)
            metrics['avg_ttf_by_top_countries'] = {
                row['country']: row['avg_ttf'] for row in cursor.fetchall()
            }
        
        return metrics

    # --- Existing methods (truncated) ---
    def get_stats(self) -> Dict[str, Any]:
        """Get overall statistics for the dashboard."""
        stats = {}
        with self.get_cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM seeds")
            stats['total_seeds'] = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) FROM seeds WHERE last_tested IS NULL")
            stats['untested_seeds'] = cursor.fetchone()['count']
            
            cursor.execute("SELECT SUM(job_count) FROM companies")
            stats['total_jobs'] = cursor.fetchone()['sum'] or 0
            
            cursor.execute("SELECT COUNT(*) FROM companies")
            stats['total_companies'] = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) FROM job_archive WHERE status = 'closed'")
            stats['total_closed_jobs'] = cursor.fetchone()['count']
            
            return stats
            
    def insert_seeds(self, seeds: List[Tuple[str, str, str, int]]):
        """Insert new seed names into the database."""
        with self.get_cursor(dict_cursor=False) as cursor:
            # Insert names, on conflict do nothing (ensures uniqueness)
            args = ','.join(cursor.mogrify("(%s, %s, %s, %s)", s).decode('utf-8') for s in seeds)
            cursor.execute(f"""
                INSERT INTO seeds (company_name, token_slug, source, tier)
                VALUES {args}
                ON CONFLICT (company_name) DO NOTHING
            """)

    def get_seeds_for_collection(self, limit: int) -> List[Tuple[int, str, str, str]]:
        """Get a batch of untested seeds for discovery."""
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("""
                SELECT id, company_name, token_slug, source FROM seeds
                WHERE enabled = TRUE AND is_hit = FALSE AND last_tested IS NULL
                ORDER BY tier ASC, id ASC
                LIMIT %s
            """, (limit,))
            return cursor.fetchall()
    
    def mark_seeds_tested(self, seed_ids: List[int], timestamp: datetime):
        """Mark a list of seeds as tested."""
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute(f"""
                UPDATE seeds SET 
                    last_tested = %s,
                    total_tested = total_tested + 1
                WHERE id = ANY(%s)
            """, (timestamp, seed_ids))
            
    def mark_seed_hit(self, seed_id: int):
        """Mark a seed as successfully linked to an ATS."""
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("""
                UPDATE seeds SET 
                    is_hit = TRUE,
                    total_hits = total_hits + 1
                WHERE id = %s
            """, (seed_id,))
            
    def get_companies_for_refresh(self, hours_since_update: int, limit: int) -> List[Dict[str, Any]]:
        """Get a batch of companies that need refreshing."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM companies
                WHERE last_updated < NOW() - INTERVAL '%s hours'
                ORDER BY last_updated ASC
                LIMIT %s
            """, (hours_since_update, limit))
            return cursor.fetchall()

    def get_all_sources(self) -> List[str]:
        """Get a list of all unique seed sources."""
        with self.get_cursor() as cursor:
            cursor.execute("SELECT DISTINCT source FROM seeds")
            return [r['source'] for r in cursor]

    # --- SNAPSHOT LOGIC ---
    def create_6h_snapshots(self):
        """Create a 6-hourly snapshot of all company job counts."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO snapshots_6h (company_id, job_count, remote_count, hybrid_count, onsite_count)
                SELECT id, job_count, remote_count, hybrid_count, onsite_count FROM companies
            """)
            # Cleanup > 30 days
            cursor.execute("DELETE FROM snapshots_6h WHERE snapshot_time < NOW() - INTERVAL '30 days'")

    def create_monthly_snapshot(self):
        """Create or update a monthly snapshot of all company job counts."""
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
