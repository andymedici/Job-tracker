import os
import json
import logging
import time
import re  # Added for _name_to_token in add_manual_seed
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from contextlib import contextmanager
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Helper for manual seed token (used in add_manual_seed)
def _name_to_token(name: str) -> str:
    token = name.lower()
    token = re.sub(r'\s+(inc|llc|ltd|co|corp|gmbh|sa)\.?$', '', token, flags=re.IGNORECASE)
    token = re.sub(r'[^a-z0-9\s-]', '', token)
    token = re.sub(r'[\s-]+', '-', token).strip('-')
    return token

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
        except Exception as e:
            conn.rollback()
            logger.error(f"Database transaction failed: {e}")
            raise
        finally:
            self.pool.putconn(conn)

    def _init_schema(self):
        with self.get_cursor(dict_cursor=False) as cursor:
            # Seeds Table
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

            # Companies Table
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
                    normalized_locations JSONB DEFAULT '{}'::jsonb,
                    extracted_skills JSONB DEFAULT '{}'::jsonb,
                    careers_url TEXT DEFAULT '',
                    first_discovered TIMESTAMP DEFAULT NOW(),
                    last_updated TIMESTAMP DEFAULT NOW()
                )
            """)

            # Snapshots Tables
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

            # Job Archive Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_archive (
                    job_hash TEXT PRIMARY KEY,
                    company_id TEXT NOT NULL,
                    job_title TEXT,
                    city TEXT,
                    region TEXT,
                    country TEXT,
                    work_type TEXT,
                    skills TEXT[] DEFAULT '{}'::TEXT[],
                    first_seen TIMESTAMP NOT NULL DEFAULT NOW(),
                    last_seen TIMESTAMP NOT NULL DEFAULT NOW(),
                    status TEXT DEFAULT 'open',
                    time_to_fill INTEGER
                )
            """)

            # Performance Indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_archive_company ON job_archive(company_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_archive_status ON job_archive(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_archive_last_seen ON job_archive(last_seen)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_companies_last_updated ON companies(last_updated)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_seeds_untested ON seeds(last_tested) WHERE last_tested IS NULL")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_time ON snapshots_6h(snapshot_time)")

    def upsert_company(self, data: Dict[str, Any]):
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("""
                INSERT INTO companies (
                    id, company_name, ats_type, token, job_count, 
                    remote_count, hybrid_count, onsite_count, locations, 
                    departments, normalized_locations, extracted_skills,
                    careers_url, last_updated
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (id) DO UPDATE SET
                    job_count = EXCLUDED.job_count,
                    remote_count = EXCLUDED.remote_count,
                    hybrid_count = EXCLUDED.hybrid_count,
                    onsite_count = EXCLUDED.onsite_count,
                    locations = EXCLUDED.locations,
                    departments = EXCLUDED.departments,
                    normalized_locations = EXCLUDED.normalized_locations,
                    extracted_skills = EXCLUDED.extracted_skills,
                    careers_url = EXCLUDED.careers_url,
                    last_updated = NOW()
            """, (
                data['id'], data['company_name'], data['ats_type'], data['token'],
                data['job_count'], data['remote_count'], data['hybrid_count'],
                data['onsite_count'], json.dumps(data.get('locations', [])),
                json.dumps(data.get('departments', [])), json.dumps(data.get('normalized_locations', {})),
                json.dumps(data.get('extracted_skills', {})), data.get('careers_url', '')
            ))

    def archive_jobs(self, company_id: str, jobs: List[Dict]):
        if not jobs:
            return
        now = datetime.utcnow()
        with self.get_cursor(dict_cursor=False) as cursor:
            for job in jobs:
                cursor.execute("""
                    INSERT INTO job_archive (
                        job_hash, company_id, job_title, city, region, country, 
                        work_type, skills, first_seen, last_seen, status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 
                              COALESCE((SELECT first_seen FROM job_archive WHERE job_hash = %s), %s), %s, 'open')
                    ON CONFLICT (job_hash) DO UPDATE SET
                        last_seen = EXCLUDED.last_seen,
                        status = 'open'
                """, (
                    job['hash'], company_id, job.get('title'), job.get('city'), 
                    job.get('region'), job.get('country'), job.get('work_type'), 
                    job.get('skills', []), job['hash'], now, now
                ))

    def mark_stale_jobs_closed(self, company_id: str, refresh_time: datetime) -> int:
        with self.get_cursor(dict_cursor=False) as cursor:
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
            closed_count = cursor.rowcount
            logger.info(f"Closed {closed_count} stale jobs for company {company_id}")
            return closed_count

    def get_time_to_fill_metrics(self) -> Dict[str, Any]:
        metrics = {}
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT AVG(time_to_fill) AS avg_ttf
                FROM job_archive 
                WHERE status = 'closed' AND time_to_fill IS NOT NULL AND time_to_fill > 0
            """)
            row = cursor.fetchone()
            metrics['overall_avg_ttf_days'] = round(row['avg_ttf'], 1) if row and row['avg_ttf'] else None
            
            cursor.execute("""
                SELECT work_type, AVG(time_to_fill) AS avg_ttf, COUNT(*) AS count
                FROM job_archive 
                WHERE status = 'closed' AND time_to_fill IS NOT NULL AND time_to_fill > 0
                GROUP BY work_type
                HAVING COUNT(*) >= 5
            """)
            metrics['avg_ttf_by_work_type'] = {row['work_type']: round(row['avg_ttf'], 1) for row in cursor.fetchall()}
            
            cursor.execute("""
                SELECT country, AVG(time_to_fill) AS avg_ttf, COUNT(*) AS closed_count
                FROM job_archive 
                WHERE status = 'closed' AND time_to_fill IS NOT NULL AND time_to_fill > 0 AND country IS NOT NULL
                GROUP BY country
                ORDER BY closed_count DESC
                LIMIT 10
            """)
            metrics['avg_ttf_by_top_countries'] = {row['country']: round(row['avg_ttf'], 1) for row in cursor.fetchall()}
        
        return metrics

    def get_advanced_analytics(self) -> Dict[str, Any]:
        metrics = self.get_time_to_fill_metrics()
        
        with self.get_cursor() as cursor:
            # Top skills in open jobs
            cursor.execute("""
                SELECT UNNEST(skills) AS skill, COUNT(*) AS count
                FROM job_archive
                WHERE status = 'open' AND skills IS NOT NULL AND ARRAY_LENGTH(skills, 1) > 0
                GROUP BY skill
                ORDER BY count DESC
                LIMIT 15
            """)
            metrics['top_skills'] = {row['skill']: row['count'] for row in cursor.fetchall()}
            
            # Top hiring regions (countries)
            cursor.execute("""
                SELECT country, COUNT(*) AS open_jobs
                FROM job_archive
                WHERE status = 'open' AND country IS NOT NULL
                GROUP BY country
                ORDER BY open_jobs DESC
                LIMIT 10
            """)
            metrics['top_hiring_regions'] = {row['country']: row['count'] for row in cursor.fetchall()}
            
            # Top hiring cities
            cursor.execute("""
                SELECT city, COUNT(*) AS open_jobs
                FROM job_archive
                WHERE status = 'open' AND city IS NOT NULL
                GROUP BY city
                ORDER BY open_jobs DESC
                LIMIT 10
            """)
            metrics['top_hiring_cities'] = {row['city']: row['count'] for row in cursor.fetchall()}
        
        return metrics

    def get_stats(self) -> Dict[str, Any]:
        stats = {}
        with self.get_cursor() as cursor:
            queries = {
                'total_seeds': "SELECT COUNT(*) AS count FROM seeds",
                'untested_seeds': "SELECT COUNT(*) AS count FROM seeds WHERE last_tested IS NULL",
                'total_jobs': "SELECT COALESCE(SUM(job_count), 0) AS sum FROM companies",
                'total_companies': "SELECT COUNT(*) AS count FROM companies",
                'total_closed_jobs': "SELECT COUNT(*) AS count FROM job_archive WHERE status = 'closed'",
                'remote_jobs': "SELECT COUNT(*) AS count FROM job_archive WHERE work_type = 'remote' AND status = 'open'"
            }
            for key, query in queries.items():
                cursor.execute(query)
                row = cursor.fetchone()
                stats[key] = row['sum'] if 'sum' in row else row['count'] if row else 0
        return stats

    def get_market_trends(self, days: int = 7) -> List[Dict]:
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    DATE(snapshot_time) AS date,
                    SUM(job_count) AS total_jobs,
                    SUM(remote_count) AS remote_jobs
                FROM snapshots_6h
                WHERE snapshot_time >= NOW() - INTERVAL '%s days'
                GROUP BY DATE(snapshot_time)
                ORDER BY date
            """, (days,))
            return [dict(row) for row in cursor.fetchall()]

    def get_job_count_changes(self, days: int = 7) -> Tuple[List[Dict], List[Dict]]:
        with self.get_cursor() as cursor:
            cursor.execute("""
                WITH snapshots AS (
                    SELECT company_id, job_count, snapshot_time,
                           ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY snapshot_time DESC) AS rn_latest,
                           ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY snapshot_time ASC) AS rn_oldest
                    FROM snapshots_6h
                    WHERE snapshot_time >= NOW() - INTERVAL '%s days'
                )
                SELECT 
                    c.company_name,
                    c.id AS company_id,
                    (latest.job_count - oldest.job_count) AS change_amount,
                    latest.job_count AS current_jobs
                FROM snapshots latest
                JOIN snapshots oldest ON latest.company_id = oldest.company_id
                JOIN companies c ON c.id = latest.company_id
                WHERE latest.rn_latest = 1 AND oldest.rn_oldest = 1
                  AND ABS(latest.job_count - oldest.job_count) >= 3
                ORDER BY ABS(change_amount) DESC
            """, (days,))
            changes = [dict(row) for row in cursor.fetchall()]
            
            surges = [c for c in changes if c['change_amount'] > 0][:15]
            declines = [c for c in changes if c['change_amount'] < 0][:15]
            
            return surges, declines

    def get_location_expansions(self, days: int = 30) -> List[Dict]:
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    c.company_name,
                    jsonb_object_keys(c.normalized_locations->'country') AS new_country,
                    c.job_count
                FROM companies c
                WHERE jsonb_typeof(c.normalized_locations->'country') = 'object'
                  AND EXISTS (
                    SELECT 1 FROM snapshots_6h s
                    WHERE s.company_id = c.id
                      AND s.snapshot_time >= NOW() - INTERVAL '%s days'
                      AND s.snapshot_time = (
                        SELECT MAX(snapshot_time) FROM snapshots_6h WHERE company_id = c.id
                      )
                      AND NOT (s.normalized_locations ? jsonb_object_keys(c.normalized_locations->'country'))
                  )
                LIMIT 20
            """, (days,))
            return [dict(row) for row in cursor.fetchall()]

    def insert_seeds(self, seeds: List[Tuple[str, str, str, int]]):
        if not seeds:
            return
        with self.get_cursor(dict_cursor=False) as cursor:
            args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s)", s).decode('utf-8') for s in seeds)
            cursor.execute(f"""
                INSERT INTO seeds (company_name, token_slug, source, tier)
                VALUES {args_str}
                ON CONFLICT (company_name) DO NOTHING
            """)

    def get_seeds_for_collection(self, limit: int) -> List[Tuple[int, str, str, str]]:
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("""
                SELECT id, company_name, token_slug, source FROM seeds
                WHERE enabled = TRUE AND is_hit = FALSE AND last_tested IS NULL
                ORDER BY tier ASC, id ASC
                LIMIT %s
            """, (limit,))
            return cursor.fetchall()
    
    def mark_seeds_tested(self, seed_ids: List[int], timestamp: datetime):
        if not seed_ids:
            return
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("""
                UPDATE seeds SET 
                    last_tested = %s,
                    total_tested = total_tested + 1
                WHERE id = ANY(%s)
            """, (timestamp, seed_ids))
            
    def mark_seed_hit(self, seed_id: int):
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("""
                UPDATE seeds SET 
                    is_hit = TRUE,
                    total_hits = total_hits + 1,
                    hit_rate = total_hits::float / NULLIF(total_tested, 0)
                WHERE id = %s
            """, (seed_id,))

    def get_companies_for_refresh(self, hours_since_update: int, limit: int) -> List[Dict[str, Any]]:
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM companies
                WHERE last_updated < NOW() - INTERVAL '%s hours'
                ORDER BY last_updated ASC
                LIMIT %s
            """, (hours_since_update, limit))
            return [dict(row) for row in cursor.fetchall()]

    def create_6h_snapshots(self):
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("""
                INSERT INTO snapshots_6h (company_id, job_count, remote_count, hybrid_count, onsite_count)
                SELECT id, job_count, remote_count, hybrid_count, onsite_count FROM companies
            """)
            cursor.execute("DELETE FROM snapshots_6h WHERE snapshot_time < NOW() - INTERVAL '90 days'")

    def create_monthly_snapshot(self):
        now = datetime.utcnow()
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("""
                INSERT INTO monthly_snapshots (company_id, year, month, job_count, remote_count, hybrid_count, onsite_count)
                SELECT id, %s, %s, job_count, remote_count, hybrid_count, onsite_count FROM companies
                ON CONFLICT (company_id, year, month) DO UPDATE SET 
                    job_count = EXCLUDED.job_count,
                    remote_count = EXCLUDED.remote_count,
                    hybrid_count = EXCLUDED.hybrid_count,
                    onsite_count = EXCLUDED.onsite_count
            """, (now.year, now.month))

    def add_manual_seed(self, company_name: str) -> bool:
        token_slug = _name_to_token(company_name)
        try:
            self.insert_seeds([(company_name, token_slug, 'manual', 1)])
            return True
        except Exception as e:
            logger.error(f"Failed to add manual seed {company_name}: {e}")
            return False

    # Optional: Source stats for dashboard
    def get_source_stats(self) -> List[Dict]:
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT source, 
                       COUNT(*) AS total_seeds,
                       SUM(CASE WHEN is_hit THEN 1 ELSE 0 END) AS hits,
                       AVG(hit_rate) AS avg_hit_rate
                FROM seeds
                GROUP BY source
                ORDER BY hits DESC
            """)
            return [dict(row) for row in cursor.fetchall()]

    def get_high_performing_sources(self, min_tested: int = 50, min_hit_rate: float = 0.05) -> List[Dict]:
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT source, hit_rate, total_hits, total_tested
                FROM seeds
                GROUP BY source
                HAVING COUNT(*) >= %s AND AVG(hit_rate) >= %s
                ORDER BY AVG(hit_rate) DESC
            """, (min_tested, min_hit_rate))
            return [dict(row) for row in cursor.fetchall()]

# Singleton instance
_db_instance = None

def get_db(max_retries: int = 15, delay: int = 2):
    global _db_instance
    if _db_instance is None:
        for attempt in range(max_retries):
            try:
                _db_instance = Database()
                logger.info("Database connection pool initialized successfully")
                return _db_instance
            except Exception as e:
                logger.warning(f"Database connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(delay)
                else:
                    raise
    return _db_instance
