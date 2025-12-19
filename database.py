"""
Enhanced Database Layer with Security, Performance, and Advanced Analytics
"""
import os
import json
import logging
import time
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from contextlib import contextmanager
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2 import pool
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def _name_to_token(name: str) -> str:
    """Convert company name to URL-friendly token"""
    token = name.lower()
    token = re.sub(r'\s+(inc|llc|ltd|co|corp|gmbh|sa)\.?$', '', token, flags=re.IGNORECASE)
    token = re.sub(r'[^a-z0-9\s-]', '', token)
    token = re.sub(r'[\s-]+', '-', token).strip('-')
    return token

class Database:
    """Enhanced database with connection pooling, advanced analytics, and monitoring"""
    
    def __init__(self, database_url: str = None):
        self.database_url = database_url or os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is required")
        
        # Fix Railway/Heroku postgres:// URLs
        if self.database_url.startswith('postgres://'):
            self.database_url = self.database_url.replace('postgres://', 'postgresql://', 1)
        
        self.conn_params = self._parse_database_url(self.database_url)
        
        # Enhanced connection pool with better settings
        pool_size = int(os.getenv('DB_POOL_SIZE', 15))
        max_overflow = int(os.getenv('DB_MAX_OVERFLOW', 25))
        
        self.pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=pool_size + max_overflow,
            **self.conn_params
        )
        
        self._init_schema()
        logger.info(f"Database initialized with pool size: {pool_size}, max overflow: {max_overflow}")
    
    def _parse_database_url(self, url: str) -> Dict[str, Any]:
        """Parse DATABASE_URL into connection parameters"""
        parsed = urlparse(url)
        return {
            'host': parsed.hostname,
            'port': parsed.port or 5432,
            'database': parsed.path[1:] if parsed.path else '',
            'user': parsed.username,
            'password': parsed.password,
            'sslmode': 'require',
            'connect_timeout': 10,
            'options': '-c statement_timeout=30000'  # 30 second query timeout
        }
    
    @contextmanager
    def get_cursor(self, dict_cursor: bool = True):
        """Thread-safe cursor context manager with automatic commit/rollback"""
        conn = None
        try:
            conn = self.pool.getconn()
            conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
            
            cursor_factory = RealDictCursor if dict_cursor else None
            with conn.cursor(cursor_factory=cursor_factory) as cursor:
                yield cursor
                conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database transaction failed: {e}", exc_info=True)
            raise
        finally:
            if conn:
                self.pool.putconn(conn)
    
    def _init_schema(self):
        """Initialize database schema with all tables and indexes"""
        with self.get_cursor(dict_cursor=False) as cursor:
            # Seeds Table (enhanced with hit tracking)
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
                    total_hits INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # Companies Table (enhanced with more tracking)
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
                    department_distribution JSONB DEFAULT '{}'::jsonb,
                    careers_url TEXT DEFAULT '',
                    first_discovered TIMESTAMP DEFAULT NOW(),
                    last_updated TIMESTAMP DEFAULT NOW(),
                    active BOOLEAN DEFAULT TRUE,
                    refresh_count INTEGER DEFAULT 0
                )
            """)

            # Enhanced Snapshots (6-hourly for trend detection)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS snapshots_6h (
                    id SERIAL PRIMARY KEY,
                    snapshot_time TIMESTAMP DEFAULT NOW(),
                    company_id TEXT NOT NULL,
                    job_count INTEGER,
                    remote_count INTEGER,
                    hybrid_count INTEGER,
                    onsite_count INTEGER,
                    normalized_locations JSONB DEFAULT '{}'::jsonb,
                    department_distribution JSONB DEFAULT '{}'::jsonb,
                    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
                )
            """)
            
            # Monthly Snapshots (long-term trends)
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
                    normalized_locations JSONB DEFAULT '{}'::jsonb,
                    department_distribution JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE (company_id, year, month),
                    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
                )
            """)

            # Enhanced Job Archive (with time-to-fill tracking)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_archive (
                    job_hash TEXT PRIMARY KEY,
                    company_id TEXT NOT NULL,
                    job_title TEXT,
                    department TEXT,
                    city TEXT,
                    region TEXT,
                    country TEXT,
                    work_type TEXT,
                    skills TEXT[] DEFAULT '{}'::TEXT[],
                    first_seen TIMESTAMP NOT NULL DEFAULT NOW(),
                    last_seen TIMESTAMP NOT NULL DEFAULT NOW(),
                    status TEXT DEFAULT 'open',
                    time_to_fill_days NUMERIC(10,2),
                    archived_at TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
                )
            """)

            # NEW: Intelligence Events Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS intelligence_events (
                    id SERIAL PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    company_id TEXT NOT NULL,
                    company_name TEXT NOT NULL,
                    event_data JSONB NOT NULL,
                    detected_at TIMESTAMP DEFAULT NOW(),
                    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
                )
            """)

            # Performance Indexes (optimized for common queries)
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_job_archive_company ON job_archive(company_id)",
                "CREATE INDEX IF NOT EXISTS idx_job_archive_status ON job_archive(status)",
                "CREATE INDEX IF NOT EXISTS idx_job_archive_last_seen ON job_archive(last_seen)",
                "CREATE INDEX IF NOT EXISTS idx_job_archive_ttf ON job_archive(time_to_fill_days) WHERE time_to_fill_days IS NOT NULL",
                "CREATE INDEX IF NOT EXISTS idx_companies_last_updated ON companies(last_updated)",
                "CREATE INDEX IF NOT EXISTS idx_companies_active ON companies(active) WHERE active = TRUE",
                "CREATE INDEX IF NOT EXISTS idx_seeds_untested ON seeds(last_tested) WHERE last_tested IS NULL",
                "CREATE INDEX IF NOT EXISTS idx_snapshots_time ON snapshots_6h(snapshot_time DESC)",
                "CREATE INDEX IF NOT EXISTS idx_snapshots_company_time ON snapshots_6h(company_id, snapshot_time DESC)",
                "CREATE INDEX IF NOT EXISTS idx_monthly_snapshots ON monthly_snapshots(year DESC, month DESC)",
                "CREATE INDEX IF NOT EXISTS idx_intelligence_events_type ON intelligence_events(event_type, detected_at DESC)",
                "CREATE INDEX IF NOT EXISTS idx_intelligence_events_company ON intelligence_events(company_id, detected_at DESC)",
                # GIN indexes for JSONB queries
                "CREATE INDEX IF NOT EXISTS idx_companies_skills_gin ON companies USING GIN(extracted_skills)",
                "CREATE INDEX IF NOT EXISTS idx_companies_locations_gin ON companies USING GIN(normalized_locations)",
                "CREATE INDEX IF NOT EXISTS idx_companies_departments_gin ON companies USING GIN(department_distribution)",
            ]
            
            for idx_sql in indexes:
                try:
                    cursor.execute(idx_sql)
                except Exception as e:
                    logger.warning(f"Index creation warning: {e}")

    def upsert_company(self, data: Dict[str, Any]):
        """Insert or update company with enhanced tracking"""
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("""
                INSERT INTO companies (
                    id, company_name, ats_type, token, job_count, 
                    remote_count, hybrid_count, onsite_count, locations, 
                    departments, normalized_locations, extracted_skills,
                    department_distribution, careers_url, last_updated
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (id) DO UPDATE SET
                    job_count = EXCLUDED.job_count,
                    remote_count = EXCLUDED.remote_count,
                    hybrid_count = EXCLUDED.hybrid_count,
                    onsite_count = EXCLUDED.onsite_count,
                    locations = EXCLUDED.locations,
                    departments = EXCLUDED.departments,
                    normalized_locations = EXCLUDED.normalized_locations,
                    extracted_skills = EXCLUDED.extracted_skills,
                    department_distribution = EXCLUDED.department_distribution,
                    careers_url = EXCLUDED.careers_url,
                    last_updated = NOW(),
                    refresh_count = companies.refresh_count + 1,
                    active = TRUE
            """, (
                data['id'], data['company_name'], data['ats_type'], data['token'],
                data['job_count'], data['remote_count'], data['hybrid_count'],
                data['onsite_count'], json.dumps(data.get('locations', [])),
                json.dumps(data.get('departments', [])), 
                json.dumps(data.get('normalized_locations', {})),
                json.dumps(data.get('extracted_skills', {})),
                json.dumps(data.get('department_distribution', {})),
                data.get('careers_url', '')
            ))

    def upsert_job_in_archive(self, job_data: Dict[str, Any]):
        """Insert or update job in archive with time-to-fill calculation"""
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("""
                INSERT INTO job_archive (
                    job_hash, company_id, job_title, department, city, region, country,
                    work_type, skills, first_seen, last_seen, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_hash) DO UPDATE SET
                    last_seen = EXCLUDED.last_seen,
                    status = CASE 
                        WHEN EXCLUDED.status = 'closed' THEN 'closed'
                        ELSE job_archive.status
                    END,
                    time_to_fill_days = CASE 
                        WHEN EXCLUDED.status = 'closed' AND job_archive.status = 'open' 
                        THEN EXTRACT(EPOCH FROM (EXCLUDED.last_seen - job_archive.first_seen))/86400.0
                        ELSE job_archive.time_to_fill_days
                    END
            """, (
                job_data['job_hash'], job_data['company_id'], job_data['job_title'],
                job_data.get('department'), job_data.get('city'), job_data.get('region'),
                job_data.get('country'), job_data.get('work_type'),
                job_data.get('skills', []), job_data['first_seen'],
                job_data['last_seen'], job_data.get('status', 'open')
            ))

    def mark_stale_jobs_closed(self, company_id: str, current_time: datetime) -> int:
        """Mark jobs as closed if not seen in last update"""
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("""
                UPDATE job_archive
                SET status = 'closed',
                    time_to_fill_days = EXTRACT(EPOCH FROM (%s - first_seen))/86400.0
                WHERE company_id = %s
                  AND status = 'open'
                  AND last_seen < %s - INTERVAL '1 hour'
                RETURNING job_hash
            """, (current_time, company_id, current_time))
            return cursor.rowcount

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive dashboard statistics"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT id) as total_companies,
                    SUM(job_count) as total_jobs,
                    SUM(remote_count) as remote_jobs,
                    SUM(hybrid_count) as hybrid_jobs,
                    SUM(onsite_count) as onsite_jobs,
                    COUNT(DISTINCT id) FILTER (WHERE ats_type = 'greenhouse') as greenhouse_count,
                    COUNT(DISTINCT id) FILTER (WHERE ats_type = 'lever') as lever_count,
                    COUNT(DISTINCT id) FILTER (WHERE ats_type = 'workday') as workday_count,
                    COUNT(DISTINCT id) FILTER (WHERE ats_type = 'ashby') as ashby_count,
                    AVG(job_count) as avg_jobs_per_company,
                    MAX(last_updated) as last_update
                FROM companies
                WHERE active = TRUE
            """)
            company_stats = dict(cursor.fetchone())

            cursor.execute("""
                SELECT COUNT(*) as untested_seeds
                FROM seeds
                WHERE last_tested IS NULL AND enabled = TRUE
            """)
            seed_stats = dict(cursor.fetchone())

            cursor.execute("""
                SELECT COUNT(*) as total_jobs_tracked,
                       COUNT(*) FILTER (WHERE status = 'open') as open_jobs,
                       COUNT(*) FILTER (WHERE status = 'closed') as closed_jobs,
                       AVG(time_to_fill_days) FILTER (WHERE status = 'closed') as avg_ttf
                FROM job_archive
                WHERE first_seen >= NOW() - INTERVAL '90 days'
            """)
            job_stats = dict(cursor.fetchone())

            return {**company_stats, **seed_stats, **job_stats}

    def get_market_trends(self, days: int = 30) -> List[Dict]:
        """Get daily job count trends"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    DATE_TRUNC('day', snapshot_time) as date,
                    SUM(job_count) as total_jobs,
                    SUM(remote_count) as remote_jobs,
                    COUNT(DISTINCT company_id) as active_companies
                FROM snapshots_6h
                WHERE snapshot_time >= NOW() - INTERVAL %s
                GROUP BY DATE_TRUNC('day', snapshot_time)
                ORDER BY date
            """, (f'{days} days',))
            return [dict(row) for row in cursor.fetchall()]

    def get_monthly_snapshots(self, limit: int = 12) -> List[Dict]:
        """Get monthly aggregated data for long-term trends"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    year, month,
                    COUNT(DISTINCT company_id) as total_companies,
                    SUM(job_count) as total_jobs,
                    SUM(remote_count) as remote_jobs,
                    AVG(job_count) as avg_jobs_per_company,
                    created_at
                FROM monthly_snapshots
                GROUP BY year, month, created_at
                ORDER BY year DESC, month DESC
                LIMIT %s
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def get_time_to_fill_metrics(self) -> Dict[str, Any]:
        """Calculate comprehensive time-to-fill metrics"""
        with self.get_cursor() as cursor:
            # Overall TTF
            cursor.execute("""
                SELECT 
                    AVG(time_to_fill_days) as overall_avg_ttf_days,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY time_to_fill_days) as median_ttf_days,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY time_to_fill_days) as p25_ttf_days,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY time_to_fill_days) as p75_ttf_days,
                    MIN(time_to_fill_days) as min_ttf_days,
                    MAX(time_to_fill_days) as max_ttf_days,
                    COUNT(*) as sample_size
                FROM job_archive
                WHERE status = 'closed'
                  AND time_to_fill_days IS NOT NULL
                  AND time_to_fill_days > 0
                  AND time_to_fill_days < 365
                  AND first_seen >= NOW() - INTERVAL '90 days'
            """)
            overall = dict(cursor.fetchone())

            # TTF by work type
            cursor.execute("""
                SELECT 
                    work_type,
                    AVG(time_to_fill_days) as avg_ttf_days,
                    COUNT(*) as count
                FROM job_archive
                WHERE status = 'closed'
                  AND time_to_fill_days IS NOT NULL
                  AND work_type IS NOT NULL
                  AND first_seen >= NOW() - INTERVAL '90 days'
                GROUP BY work_type
            """)
            by_work_type = {row['work_type']: row['avg_ttf_days'] for row in cursor.fetchall()}

            # TTF by department
            cursor.execute("""
                SELECT 
                    department,
                    AVG(time_to_fill_days) as avg_ttf_days,
                    COUNT(*) as count
                FROM job_archive
                WHERE status = 'closed'
                  AND time_to_fill_days IS NOT NULL
                  AND department IS NOT NULL
                  AND first_seen >= NOW() - INTERVAL '90 days'
                GROUP BY department
                ORDER BY count DESC
                LIMIT 10
            """)
            by_department = {row['department']: row['avg_ttf_days'] for row in cursor.fetchall()}

            return {
                **overall,
                'by_work_type': by_work_type,
                'by_department': by_department
            }

    def get_advanced_analytics(self) -> Dict[str, Any]:
        """Comprehensive analytics for the analytics dashboard"""
        analytics = {}
        
        # 1. Time-to-Fill metrics
        analytics['time_to_fill'] = self.get_time_to_fill_metrics()
        
        # 2. Top skills across all companies
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    key as skill,
                    SUM(value::int) as total_demand
                FROM companies, jsonb_each_text(extracted_skills)
                WHERE active = TRUE
                GROUP BY key
                ORDER BY total_demand DESC
                LIMIT 20
            """)
            analytics['top_skills'] = {row['skill']: row['total_demand'] for row in cursor.fetchall()}
        
        # 3. Top hiring regions
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    key as country,
                    SUM(value::int) as total_jobs
                FROM companies, jsonb_each_text(normalized_locations -> 'country')
                WHERE active = TRUE
                GROUP BY key
                ORDER BY total_jobs DESC
                LIMIT 10
            """)
            analytics['top_hiring_regions'] = {row['country']: row['total_jobs'] for row in cursor.fetchall()}
        
        # 4. Department distribution
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    key as department,
                    SUM(value::int) as total_jobs
                FROM companies, jsonb_each_text(department_distribution)
                WHERE active = TRUE
                GROUP BY key
                ORDER BY total_jobs DESC
            """)
            analytics['department_distribution'] = {row['department']: row['total_jobs'] for row in cursor.fetchall()}
        
        # 5. Work type distribution
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    SUM(remote_count) as remote,
                    SUM(hybrid_count) as hybrid,
                    SUM(onsite_count) as onsite,
                    SUM(job_count) as total
                FROM companies
                WHERE active = TRUE
            """)
            row = cursor.fetchone()
            total = row['total'] or 1
            analytics['work_type_distribution'] = {
                'remote': row['remote'] or 0,
                'hybrid': row['hybrid'] or 0,
                'onsite': row['onsite'] or 0,
                'total': row['total'] or 0,
                'remote_percent': round((row['remote'] or 0) / total * 100, 2),
                'hybrid_percent': round((row['hybrid'] or 0) / total * 100, 2),
                'onsite_percent': round((row['onsite'] or 0) / total * 100, 2),
            }
        
        # 6. Fastest growing companies
        with self.get_cursor() as cursor:
            cursor.execute("""
                WITH recent_changes AS (
                    SELECT 
                        company_id,
                        MAX(job_count) - MIN(job_count) as job_change,
                        MAX(snapshot_time) - MIN(snapshot_time) as time_span
                    FROM snapshots_6h
                    WHERE snapshot_time >= NOW() - INTERVAL '14 days'
                    GROUP BY company_id
                    HAVING MAX(job_count) - MIN(job_count) > 0
                )
                SELECT 
                    c.company_name,
                    c.ats_type,
                    rc.job_change,
                    c.job_count as current_jobs,
                    ROUND((rc.job_change::numeric / EXTRACT(EPOCH FROM rc.time_span) * 86400), 2) as jobs_per_day
                FROM recent_changes rc
                JOIN companies c ON c.id = rc.company_id
                WHERE c.active = TRUE
                ORDER BY rc.job_change DESC
                LIMIT 20
            """)
            analytics['fastest_growing'] = [dict(row) for row in cursor.fetchall()]
        
        # 7. ATS platform distribution
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    ats_type,
                    COUNT(*) as company_count,
                    SUM(job_count) as total_jobs,
                    AVG(job_count) as avg_jobs
                FROM companies
                WHERE active = TRUE
                GROUP BY ats_type
                ORDER BY company_count DESC
            """)
            analytics['ats_distribution'] = [dict(row) for row in cursor.fetchall()]
        
        # 8. Recent intelligence events
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    event_type,
                    COUNT(*) as event_count,
                    MAX(detected_at) as last_detected
                FROM intelligence_events
                WHERE detected_at >= NOW() - INTERVAL '7 days'
                GROUP BY event_type
                ORDER BY event_count DESC
            """)
            analytics['recent_events'] = [dict(row) for row in cursor.fetchall()]
        
        return analytics

    def get_job_count_changes(self, days: int = 7) -> Tuple[List[Dict], List[Dict]]:
        """Get companies with significant job count changes"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                WITH snapshot_comparison AS (
                    SELECT 
                        company_id,
                        FIRST_VALUE(job_count) OVER (PARTITION BY company_id ORDER BY snapshot_time DESC) as latest_count,
                        FIRST_VALUE(job_count) OVER (PARTITION BY company_id ORDER BY snapshot_time ASC) as earliest_count,
                        MAX(snapshot_time) OVER (PARTITION BY company_id) as latest_time,
                        MIN(snapshot_time) OVER (PARTITION BY company_id) as earliest_time
                    FROM snapshots_6h
                    WHERE snapshot_time >= NOW() - INTERVAL %s
                )
                SELECT DISTINCT ON (company_id)
                    c.company_name,
                    c.id AS company_id,
                    sc.latest_count AS current_jobs,
                    sc.earliest_count AS previous_jobs,
                    (sc.latest_count - sc.earliest_count) AS change_amount,
                    CASE 
                        WHEN sc.earliest_count = 0 THEN 100.0
                        ELSE ROUND(((sc.latest_count - sc.earliest_count)::numeric / sc.earliest_count * 100), 2)
                    END as change_percent
                FROM snapshot_comparison sc
                JOIN companies c ON c.id = sc.company_id
                WHERE ABS(sc.latest_count - sc.earliest_count) >= 5
                  AND c.active = TRUE
                ORDER BY company_id, ABS(sc.latest_count - sc.earliest_count) DESC
            """, (f'{days} days',))
            
            changes = [dict(row) for row in cursor.fetchall()]
            
            # Separate into surges and declines
            surges = sorted([c for c in changes if c['change_amount'] > 0], 
                          key=lambda x: x['change_amount'], reverse=True)[:20]
            declines = sorted([c for c in changes if c['change_amount'] < 0], 
                            key=lambda x: abs(c['change_amount']), reverse=True)[:20]
            
            return surges, declines

    def get_location_expansions(self, days: int = 30) -> List[Dict]:
        """Detect new location expansions"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    c.company_name,
                    c.id as company_id,
                    jsonb_object_keys(c.normalized_locations->'country') AS country,
                    c.job_count,
                    c.last_updated
                FROM companies c
                WHERE c.active = TRUE
                  AND c.last_updated >= NOW() - INTERVAL %s
                  AND jsonb_typeof(c.normalized_locations->'country') = 'object'
                ORDER BY c.last_updated DESC
                LIMIT 30
            """, (f'{days} days',))
            return [dict(row) for row in cursor.fetchall()]

    def record_intelligence_event(self, event_type: str, company_id: str, 
                                 company_name: str, event_data: Dict):
        """Record an intelligence event for tracking and notifications"""
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("""
                INSERT INTO intelligence_events (event_type, company_id, company_name, event_data)
                VALUES (%s, %s, %s, %s)
            """, (event_type, company_id, company_name, json.dumps(event_data)))

    def insert_seeds(self, seeds: List[Tuple[str, str, str, int]]):
        """Bulk insert seeds efficiently"""
        if not seeds:
            return 0
        
        with self.get_cursor(dict_cursor=False) as cursor:
            execute_values(
                cursor,
                """
                INSERT INTO seeds (company_name, token_slug, source, tier)
                VALUES %s
                ON CONFLICT (company_name) DO NOTHING
                """,
                seeds,
                page_size=1000
            )
            return cursor.rowcount

    def get_seeds(self, limit: int = 100) -> List[Dict]:
        """Get seeds with their stats"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM seeds
                ORDER BY 
                    CASE WHEN last_tested IS NULL THEN 0 ELSE 1 END,
                    tier ASC,
                    created_at DESC
                LIMIT %s
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def get_seeds_for_collection(self, limit: int) -> List[Tuple[int, str, str, str]]:
        """Get untested seeds for collection"""
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("""
                SELECT id, company_name, token_slug, source 
                FROM seeds
                WHERE enabled = TRUE 
                  AND is_hit = FALSE 
                  AND last_tested IS NULL
                ORDER BY tier ASC, id ASC
                LIMIT %s
            """, (limit,))
            return cursor.fetchall()
    
    def mark_seeds_tested(self, seed_ids: List[int], timestamp: datetime):
        """Mark seeds as tested"""
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
        """Mark seed as successful hit"""
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("""
                UPDATE seeds SET 
                    is_hit = TRUE,
                    total_hits = total_hits + 1,
                    hit_rate = (total_hits + 1)::float / NULLIF(total_tested, 0)
                WHERE id = %s
            """, (seed_id,))

    def add_manual_seed(self, company_name: str) -> bool:
        """Add a manual seed from the UI"""
        token_slug = _name_to_token(company_name)
        try:
            self.insert_seeds([(company_name, token_slug, 'manual', 1)])
            return True
        except Exception as e:
            logger.error(f"Failed to add manual seed {company_name}: {e}")
            return False

    def get_companies_for_refresh(self, hours_since_update: int, limit: int) -> List[Dict[str, Any]]:
        """Get companies that need refreshing"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT * FROM companies
                WHERE active = TRUE
                  AND last_updated < NOW() - INTERVAL %s
                ORDER BY last_updated ASC
                LIMIT %s
            """, (f'{hours_since_update} hours', limit))
            return [dict(row) for row in cursor.fetchall()]

    def create_6h_snapshots(self) -> int:
        """Create 6-hourly snapshots for all companies"""
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("""
                INSERT INTO snapshots_6h (
                    company_id, job_count, remote_count, hybrid_count, 
                    onsite_count, normalized_locations, department_distribution
                )
                SELECT 
                    id, job_count, remote_count, hybrid_count, 
                    onsite_count, normalized_locations, department_distribution
                FROM companies
                WHERE active = TRUE
            """)
            snapshot_count = cursor.rowcount
            
            # Cleanup old snapshots
            cursor.execute("""
                DELETE FROM snapshots_6h 
                WHERE snapshot_time < NOW() - INTERVAL '90 days'
            """)
            
            logger.info(f"Created {snapshot_count} snapshots, cleaned up old data")
            return snapshot_count

    def create_monthly_snapshot(self) -> int:
        """Create monthly snapshot (idempotent for current month)"""
        now = datetime.utcnow()
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("""
                INSERT INTO monthly_snapshots (
                    company_id, year, month, job_count, remote_count, 
                    hybrid_count, onsite_count, normalized_locations, 
                    department_distribution
                )
                SELECT 
                    id, %s, %s, job_count, remote_count, hybrid_count, 
                    onsite_count, normalized_locations, department_distribution
                FROM companies
                WHERE active = TRUE
                ON CONFLICT (company_id, year, month) DO UPDATE SET 
                    job_count = EXCLUDED.job_count,
                    remote_count = EXCLUDED.remote_count,
                    hybrid_count = EXCLUDED.hybrid_count,
                    onsite_count = EXCLUDED.onsite_count,
                    normalized_locations = EXCLUDED.normalized_locations,
                    department_distribution = EXCLUDED.department_distribution,
                    created_at = NOW()
            """, (now.year, now.month))
            return cursor.rowcount

    def acquire_advisory_lock(self, lock_name: str, timeout: int = 0) -> bool:
        """Acquire PostgreSQL advisory lock for distributed coordination"""
        lock_id = hash(lock_name) % (2**31)
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
            return cursor.fetchone()[0]

    def release_advisory_lock(self, lock_name: str):
        """Release PostgreSQL advisory lock"""
        lock_id = hash(lock_name) % (2**31)
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))

    def get_source_stats(self) -> List[Dict]:
        """Get seed source performance statistics"""
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    source, 
                    COUNT(*) AS total_seeds,
                    SUM(CASE WHEN is_hit THEN 1 ELSE 0 END) AS hits,
                    ROUND(AVG(hit_rate), 4) AS avg_hit_rate,
                    SUM(total_tested) as total_tests
                FROM seeds
                GROUP BY source
                ORDER BY hits DESC
            """)
            return [dict(row) for row in cursor.fetchall()]

    def close(self):
        """Close all connections in the pool"""
        if self.pool:
            self.pool.closeall()
            logger.info("Database connection pool closed")

# Singleton instance
_db_instance = None

def get_db(max_retries: int = 15, delay: int = 2) -> Database:
    """Get or create database singleton with retry logic"""
    global _db_instance
    if _db_instance is None:
        for attempt in range(max_retries):
            try:
                _db_instance = Database()
                logger.info("✅ Database connection pool initialized successfully")
                return _db_instance
            except Exception as e:
                logger.warning(f"⚠️ Database connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(delay)
                else:
                    logger.error("❌ Failed to initialize database after all retries")
                    raise
    return _db_instance
