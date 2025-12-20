"""Database Interface for Job Intelligence Platform"""

import os
import logging
import json
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.pool import ThreadedConnectionPool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, database_url: str = None):
        self.database_url = database_url or os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable not set")
        
        pool_size = int(os.getenv('DB_POOL_SIZE', 15))
        max_overflow = int(os.getenv('DB_MAX_OVERFLOW', 25))
        
        try:
            self.pool = ThreadedConnectionPool(
                minconn=5,
                maxconn=pool_size + max_overflow,
                dsn=self.database_url
            )
            logger.info(f"Database initialized with pool size: {pool_size}, max overflow: {max_overflow}")
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
        
        self._create_tables()
        logger.info("✅ Database connection pool initialized successfully")
    
    @contextmanager
    def get_connection(self):
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)
    
    def _create_tables(self):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Companies
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS companies (
                        id SERIAL PRIMARY KEY,
                        company_name VARCHAR(255) NOT NULL UNIQUE,
                        company_name_token VARCHAR(255) UNIQUE,
                        ats_type VARCHAR(50),
                        board_url TEXT,
                        job_count INTEGER DEFAULT 0,
                        last_scraped TIMESTAMP,
                        created_at TIMESTAMP DEFAULT NOW(),
                        metadata JSONB
                    )
                """)
                
                # Job archive - FIXED with all columns
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS job_archive (
                        id SERIAL PRIMARY KEY,
                        company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
                        job_id VARCHAR(255),
                        title TEXT,
                        location TEXT,
                        department TEXT,
                        work_type VARCHAR(50),
                        job_url TEXT,
                        posted_date DATE,
                        salary_min INTEGER,
                        salary_max INTEGER,
                        salary_currency VARCHAR(10),
                        status VARCHAR(20) DEFAULT 'active',
                        first_seen TIMESTAMP DEFAULT NOW(),
                        last_seen TIMESTAMP DEFAULT NOW(),
                        closed_at TIMESTAMP,
                        metadata JSONB,
                        UNIQUE(company_id, job_id)
                    )
                """)
                
                cur.execute("CREATE INDEX IF NOT EXISTS idx_job_archive_status ON job_archive(status)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_job_archive_company_status ON job_archive(company_id, status)")
                
                # Seeds
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS seed_companies (
                        id SERIAL PRIMARY KEY,
                        company_name VARCHAR(255) NOT NULL,
                        company_name_token VARCHAR(255) UNIQUE,
                        source VARCHAR(100),
                        tier INTEGER DEFAULT 4,
                        website_url TEXT,
                        times_tested INTEGER DEFAULT 0,
                        times_successful INTEGER DEFAULT 0,
                        last_tested_at TIMESTAMP,
                        success_rate DECIMAL(5,2) DEFAULT 0,
                        is_blacklisted BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                
                # Snapshots - FIXED with all columns
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS snapshots_6h (
                        id SERIAL PRIMARY KEY,
                        company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
                        snapshot_time TIMESTAMP DEFAULT NOW(),
                        job_count INTEGER,
                        active_jobs INTEGER,
                        locations_count INTEGER,
                        departments_count INTEGER,
                        metadata JSONB
                    )
                """)
                
                cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_company_time ON snapshots_6h(company_id, snapshot_time DESC)")
                
                # Monthly snapshots
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS snapshots_monthly (
                        id SERIAL PRIMARY KEY,
                        snapshot_date DATE NOT NULL UNIQUE,
                        total_companies INTEGER,
                        total_jobs INTEGER,
                        avg_jobs_per_company DECIMAL(10,2),
                        top_locations JSONB,
                        top_departments JSONB,
                        ats_distribution JSONB,
                        metadata JSONB,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                
                # Intelligence events - FIXED with metadata
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS intelligence_events (
                        id SERIAL PRIMARY KEY,
                        company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
                        event_type VARCHAR(50),
                        severity VARCHAR(20) DEFAULT 'info',
                        metadata JSONB,
                        detected_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                
                cur.execute("CREATE INDEX IF NOT EXISTS idx_intel_events_type_time ON intelligence_events(event_type, detected_at DESC)")
                
                conn.commit()
    
    def _name_to_token(self, name: str) -> str:
        import re
        token = name.lower()
        token = re.sub(r'\s+(inc|llc|ltd|co|corp|corporation|gmbh|sa|ag|plc)\.?$', '', token, flags=re.IGNORECASE)
        token = re.sub(r'[^a-z0-9\s-]', '', token)
        token = re.sub(r'[\s-]+', '-', token).strip('-')
        return token
    
    def acquire_advisory_lock(self, lock_name: str, timeout: int = 0) -> bool:
        try:
            lock_id = hash(lock_name) % (2**31)
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    if timeout > 0:
                        cur.execute("SELECT pg_try_advisory_lock(%s)", (lock_id,))
                    else:
                        cur.execute("SELECT pg_advisory_lock(%s)", (lock_id,))
                    result = cur.fetchone()[0] if timeout > 0 else True
                    conn.commit()
                    if result:
                        logger.debug(f"Acquired advisory lock: {lock_name}")
                    return result
        except Exception as e:
            logger.error(f"Error acquiring advisory lock: {e}")
            return False
    
    def release_advisory_lock(self, lock_name: str) -> bool:
        try:
            lock_id = hash(lock_name) % (2**31)
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (lock_id,))
                    result = cur.fetchone()[0]
                    conn.commit()
                    if result:
                        logger.debug(f"Released advisory lock: {lock_name}")
                    return result
        except Exception as e:
            logger.error(f"Error releasing advisory lock: {e}")
            return False
    
    def add_company(self, company_name: str, ats_type: str, board_url: str, job_count: int = 0, metadata: Dict = None) -> Optional[int]:
        try:
            token = self._name_to_token(company_name)
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO companies (company_name, company_name_token, ats_type, board_url, job_count, last_scraped, metadata)
                        VALUES (%s, %s, %s, %s, %s, NOW(), %s)
                        ON CONFLICT (company_name) 
                        DO UPDATE SET 
                            ats_type = EXCLUDED.ats_type,
                            board_url = EXCLUDED.board_url,
                            job_count = EXCLUDED.job_count,
                            last_scraped = NOW(),
                            metadata = EXCLUDED.metadata
                        RETURNING id
                    """, (company_name, token, ats_type, board_url, job_count, json.dumps(metadata or {})))
                    company_id = cur.fetchone()[0]
                    conn.commit()
                    logger.info(f"Added/updated company: {company_name} (ID: {company_id})")
                    return company_id
        except Exception as e:
            logger.error(f"Error adding company: {e}")
            return None
    
    def get_company_id(self, company_name: str) -> Optional[int]:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM companies WHERE company_name = %s", (company_name,))
                    result = cur.fetchone()
                    return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting company ID: {e}")
            return None
    
    def update_company_job_count(self, company_id: int, job_count: int):
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE companies SET job_count = %s, last_scraped = NOW() WHERE id = %s", (job_count, company_id))
                    conn.commit()
        except Exception as e:
            logger.error(f"Error updating job count: {e}")
    
    def get_companies_for_refresh(self, hours_since_update: int = 6, limit: int = 500) -> List[Dict]:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT id, company_name, ats_type, board_url, job_count
                        FROM companies
                        WHERE last_scraped < NOW() - INTERVAL '%s hours' OR last_scraped IS NULL
                        ORDER BY last_scraped ASC NULLS FIRST
                        LIMIT %s
                    """, (hours_since_update, limit))
                    columns = [desc[0] for desc in cur.description]
                    return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error getting companies for refresh: {e}")
            return []
    
    def archive_jobs(self, company_id: int, jobs: List[Dict]) -> Tuple[int, int, int]:
        if not jobs:
            return 0, 0, 0
        try:
            new_count = 0
            updated_count = 0
            closed_count = 0
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT job_id FROM job_archive WHERE company_id = %s AND status = 'active'", (company_id,))
                    current_job_ids = {row[0] for row in cur.fetchall()}
                    new_job_ids = {job['id'] for job in jobs}
                    closed_ids = current_job_ids - new_job_ids
                    if closed_ids:
                        cur.execute("UPDATE job_archive SET status = 'closed', closed_at = NOW() WHERE company_id = %s AND job_id = ANY(%s) AND status = 'active'", (company_id, list(closed_ids)))
                        closed_count = cur.rowcount
                    for job in jobs:
                        cur.execute("""
                            INSERT INTO job_archive 
                            (company_id, job_id, title, location, department, work_type, job_url, posted_date, salary_min, salary_max, salary_currency, status, metadata)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'active', %s)
                            ON CONFLICT (company_id, job_id) 
                            DO UPDATE SET
                                title = EXCLUDED.title,
                                location = EXCLUDED.location,
                                department = EXCLUDED.department,
                                work_type = EXCLUDED.work_type,
                                job_url = EXCLUDED.job_url,
                                posted_date = EXCLUDED.posted_date,
                                salary_min = EXCLUDED.salary_min,
                                salary_max = EXCLUDED.salary_max,
                                salary_currency = EXCLUDED.salary_currency,
                                last_seen = NOW(),
                                status = 'active',
                                metadata = EXCLUDED.metadata
                            RETURNING (xmax = 0) AS inserted
                        """, (
                            company_id, job['id'], job.get('title'), job.get('location'),
                            job.get('department'), job.get('work_type'), job.get('url'),
                            job.get('posted_date'), job.get('salary_min'), job.get('salary_max'),
                            job.get('salary_currency'), json.dumps(job.get('metadata', {}))
                        ))
                        was_inserted = cur.fetchone()[0]
                        if was_inserted:
                            new_count += 1
                        else:
                            updated_count += 1
                    conn.commit()
                    logger.info(f"Job archive: +{new_count} new, ~{updated_count} updated, -{closed_count} closed")
                    return new_count, updated_count, closed_count
        except Exception as e:
            logger.error(f"Error archiving jobs: {e}")
            return 0, 0, 0
    
    def insert_seeds(self, seeds: List[Tuple[str, str, str, int]]) -> int:
        if not seeds:
            return 0
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    execute_batch(cur, """
                        INSERT INTO seed_companies (company_name, company_name_token, source, tier)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (company_name_token) DO NOTHING
                    """, seeds, page_size=1000)
                    inserted = cur.rowcount
                    conn.commit()
                    return inserted
        except Exception as e:
            logger.error(f"Error inserting seeds: {e}")
            return 0
    
    def add_manual_seed(self, company_name: str, website_url: str = None) -> bool:
        try:
            token = self._name_to_token(company_name)
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM seed_companies WHERE company_name_token = %s OR company_name ILIKE %s", (token, company_name))
                    if cur.fetchone():
                        logger.info(f"Seed already exists: {company_name}")
                        return False
                    cur.execute("SELECT 1 FROM companies WHERE company_name ILIKE %s", (company_name,))
                    if cur.fetchone():
                        logger.info(f"Company already tracked: {company_name}")
                        return False
                    cur.execute("""
                        INSERT INTO seed_companies (company_name, company_name_token, source, tier, website_url)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (company_name_token) DO NOTHING
                    """, (company_name, token, 'manual', 0, website_url))
                    conn.commit()
                    logger.info(f"Added manual seed: {company_name}")
                    return True
        except Exception as e:
            logger.error(f"Error adding manual seed: {e}")
            return False
    
    def get_seeds(self, limit: int = 100, prioritize_quality: bool = True) -> List[Dict]:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    if prioritize_quality:
                        cur.execute("""
                            SELECT company_name, company_name_token, source, tier, times_tested, success_rate, website_url
                            FROM seed_companies
                            WHERE is_blacklisted = FALSE
                            ORDER BY 
                                CASE WHEN times_tested = 0 THEN 0 ELSE 1 END,
                                success_rate DESC NULLS LAST,
                                created_at DESC
                            LIMIT %s
                        """, (limit,))
                    else:
                        cur.execute("""
                            SELECT company_name, company_name_token, source, tier, times_tested, success_rate, website_url
                            FROM seed_companies
                            WHERE is_blacklisted = FALSE
                            ORDER BY created_at DESC
                            LIMIT %s
                        """, (limit,))
                    columns = [desc[0] for desc in cur.description]
                    return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error getting seeds: {e}")
            return []
    
    def increment_seed_tested(self, company_name: str):
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE seed_companies 
                        SET times_tested = times_tested + 1,
                            last_tested_at = NOW(),
                            success_rate = CASE 
                                WHEN times_tested + 1 > 0 
                                THEN (times_successful::DECIMAL / (times_tested + 1) * 100)
                                ELSE 0 
                            END
                        WHERE company_name ILIKE %s
                    """, (company_name,))
                    conn.commit()
        except Exception as e:
            logger.debug(f"Error updating seed tested count: {e}")
    
    def increment_seed_success(self, company_name: str):
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE seed_companies 
                        SET times_successful = times_successful + 1,
                            success_rate = CASE 
                                WHEN times_tested > 0 
                                THEN ((times_successful + 1)::DECIMAL / times_tested * 100)
                                ELSE 0 
                            END
                        WHERE company_name ILIKE %s
                    """, (company_name,))
                    conn.commit()
        except Exception as e:
            logger.debug(f"Error updating seed success count: {e}")
    
    def blacklist_poor_seeds(self, min_tests: int = 3, max_success_rate: float = 5.0) -> int:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE seed_companies
                        SET is_blacklisted = TRUE
                        WHERE times_tested >= %s 
                        AND success_rate < %s
                        AND is_blacklisted = FALSE
                        RETURNING company_name
                    """, (min_tests, max_success_rate))
                    blacklisted = cur.fetchall()
                    conn.commit()
                    if blacklisted:
                        logger.info(f"Blacklisted {len(blacklisted)} poor-performing seeds")
                    return len(blacklisted)
        except Exception as e:
            logger.error(f"Error blacklisting seeds: {e}")
            return 0
    
    def create_company_snapshots(self) -> int:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO snapshots_6h (company_id, job_count, active_jobs, locations_count, departments_count)
                        SELECT 
                            c.id,
                            c.job_count,
                            COUNT(DISTINCT CASE WHEN j.status = 'active' THEN j.id END) as active_jobs,
                            COUNT(DISTINCT j.location) as locations_count,
                            COUNT(DISTINCT j.department) as departments_count
                        FROM companies c
                        LEFT JOIN job_archive j ON c.id = j.company_id
                        GROUP BY c.id
                    """)
                    count = cur.rowcount
                    conn.commit()
                    logger.info(f"Created {count} company snapshots")
                    return count
        except Exception as e:
            logger.error(f"Error creating snapshots: {e}")
            return 0
    
    def create_monthly_snapshot(self) -> bool:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(DISTINCT c.id) as total_companies, COALESCE(SUM(c.job_count), 0) as total_jobs, COALESCE(AVG(c.job_count), 0) as avg_jobs FROM companies c")
                    total_companies, total_jobs, avg_jobs = cur.fetchone()
                    cur.execute("SELECT location, COUNT(*) as count FROM job_archive WHERE status = 'active' AND location IS NOT NULL GROUP BY location ORDER BY count DESC LIMIT 20")
                    top_locations = [{'location': row[0], 'count': row[1]} for row in cur.fetchall()]
                    cur.execute("SELECT department, COUNT(*) as count FROM job_archive WHERE status = 'active' AND department IS NOT NULL GROUP BY department ORDER BY count DESC LIMIT 20")
                    top_departments = [{'department': row[0], 'count': row[1]} for row in cur.fetchall()]
                    cur.execute("SELECT ats_type, COUNT(*) as count FROM companies GROUP BY ats_type ORDER BY count DESC")
                    ats_distribution = [{'ats': row[0], 'count': row[1]} for row in cur.fetchall()]
                    cur.execute("""
                        INSERT INTO snapshots_monthly 
                        (snapshot_date, total_companies, total_jobs, avg_jobs_per_company, top_locations, top_departments, ats_distribution)
                        VALUES (CURRENT_DATE, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (snapshot_date) DO UPDATE SET
                            total_companies = EXCLUDED.total_companies,
                            total_jobs = EXCLUDED.total_jobs,
                            avg_jobs_per_company = EXCLUDED.avg_jobs_per_company,
                            top_locations = EXCLUDED.top_locations,
                            top_departments = EXCLUDED.top_departments,
                            ats_distribution = EXCLUDED.ats_distribution
                    """, (total_companies, total_jobs, avg_jobs, json.dumps(top_locations), json.dumps(top_departments), json.dumps(ats_distribution)))
                    conn.commit()
                    logger.info("Created monthly snapshot")
                    return True
        except Exception as e:
            logger.error(f"Error creating monthly snapshot: {e}")
            return False
    
    def get_job_count_changes(self, days: int = 7, threshold_percent: float = 10.0) -> Tuple[List[Dict], List[Dict]]:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        WITH recent_snapshots AS (
                            SELECT DISTINCT ON (company_id)
                                company_id, job_count as current_count, snapshot_time
                            FROM snapshots_6h
                            WHERE snapshot_time >= NOW() - INTERVAL '1 day'
                            ORDER BY company_id, snapshot_time DESC
                        ),
                        old_snapshots AS (
                            SELECT DISTINCT ON (company_id)
                                company_id, job_count as old_count
                            FROM snapshots_6h
                            WHERE snapshot_time >= NOW() - INTERVAL '%s days'
                              AND snapshot_time < NOW() - INTERVAL '%s days'
                            ORDER BY company_id, snapshot_time DESC
                        )
                        SELECT 
                            c.company_name, c.id as company_id, o.old_count, r.current_count,
                            (r.current_count - o.old_count) as job_change,
                            ROUND(((r.current_count - o.old_count)::DECIMAL / NULLIF(o.old_count, 0) * 100), 1) as percent_change
                        FROM recent_snapshots r
                        JOIN old_snapshots o ON r.company_id = o.company_id
                        JOIN companies c ON c.id = r.company_id
                        WHERE o.old_count > 0
                          AND ABS((r.current_count - o.old_count)::DECIMAL / o.old_count * 100) >= %s
                        ORDER BY ABS(r.current_count - o.old_count) DESC
                    """, (days, days - 1, threshold_percent))
                    columns = [desc[0] for desc in cur.description]
                    all_changes = [dict(zip(columns, row)) for row in cur.fetchall()]
                    surges = [c for c in all_changes if c['job_change'] > 0]
                    declines = [c for c in all_changes if c['job_change'] < 0]
                    return surges, declines
        except Exception as e:
            logger.error(f"Error getting job count changes: {e}")
            return [], []
    
    def get_location_expansions(self, days: int = 30) -> List[Dict]:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            c.company_name,
                            ie.metadata->>'location' as new_location,
                            ie.metadata->>'job_count' as job_count,
                            ie.detected_at
                        FROM intelligence_events ie
                        JOIN companies c ON ie.company_id = c.id
                        WHERE ie.event_type = 'location_expansion'
                          AND ie.detected_at >= NOW() - INTERVAL '%s days'
                        ORDER BY ie.detected_at DESC
                        LIMIT 50
                    """, (days,))
                    columns = [desc[0] for desc in cur.description]
                    return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error getting location expansions: {e}")
            return []
    
    def track_location_expansion(self, company_id: int, new_location: str, job_count: int = 1):
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM snapshots_6h WHERE company_id = %s", (company_id,))
                    snapshot_count = cur.fetchone()[0]
                    if snapshot_count == 0:
                        logger.debug(f"Skipping location expansion for company {company_id} (first scan)")
                        return
                    cur.execute("SELECT id FROM job_archive WHERE company_id = %s AND location ILIKE %s AND first_seen < NOW() - INTERVAL '1 day'", (company_id, f'%{new_location}%'))
                    if cur.fetchone():
                        return
                    cur.execute("INSERT INTO intelligence_events (company_id, event_type, metadata, detected_at) VALUES (%s, 'location_expansion', %s, NOW())", (company_id, json.dumps({'location': new_location, 'job_count': job_count})))
                    conn.commit()
                    logger.info(f"📍 Location expansion detected: {new_location}")
        except Exception as e:
            logger.error(f"Error tracking location expansion: {e}")
    
    def get_time_to_fill_metrics(self) -> Dict:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            COUNT(*) as sample_size,
                            AVG(EXTRACT(EPOCH FROM (closed_at - first_seen)) / 86400) as avg_ttf_days,
                            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (closed_at - first_seen)) / 86400) as median_ttf_days,
                            MIN(EXTRACT(EPOCH FROM (closed_at - first_seen)) / 86400) as min_ttf_days,
                            MAX(EXTRACT(EPOCH FROM (closed_at - first_seen)) / 86400) as max_ttf_days
                        FROM job_archive
                        WHERE status = 'closed'
                          AND closed_at IS NOT NULL
                          AND closed_at > first_seen
                          AND closed_at > NOW() - INTERVAL '6 months'
                    """)
                    row = cur.fetchone()
                    if row and row[0] > 0:
                        return {
                            'sample_size': row[0],
                            'overall_avg_ttf_days': round(row[1], 1) if row[1] else None,
                            'median_ttf_days': round(row[2], 1) if row[2] else None,
                            'min_ttf_days': round(row[3], 1) if row[3] else None,
                            'max_ttf_days': round(row[4], 1) if row[4] else None
                        }
                    return {}
        except Exception as e:
            logger.error(f"Error calculating TTF metrics: {e}")
            return {}
    
    def get_stats(self) -> Dict[str, Any]:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            (SELECT COUNT(*) FROM companies) as total_companies,
                            (SELECT COALESCE(SUM(job_count), 0) FROM companies) as total_jobs,
                            (SELECT COUNT(*) FROM seed_companies WHERE is_blacklisted = FALSE) as total_seeds,
                            (SELECT COUNT(*) FROM job_archive WHERE status = 'closed') as closed_jobs
                    """)
                    row = cur.fetchone()
                    return {
                        'total_companies': row[0],
                        'total_jobs': row[1],
                        'total_seeds': row[2],
                        'closed_jobs': row[3]
                    }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {'total_companies': 0, 'total_jobs': 0, 'total_seeds': 0, 'closed_jobs': 0}
    
    def get_market_trends(self, days: int = 7) -> List[Dict]:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            DATE_TRUNC('day', snapshot_time) as date,
                            COUNT(DISTINCT company_id) as companies,
                            SUM(job_count) as total_jobs,
                            AVG(job_count) as avg_jobs_per_company
                        FROM snapshots_6h
                        WHERE snapshot_time >= NOW() - INTERVAL '%s days'
                        GROUP BY DATE_TRUNC('day', snapshot_time)
                        ORDER BY date
                    """, (days,))
                    columns = [desc[0] for desc in cur.description]
                    return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error getting market trends: {e}")
            return []
    
    def get_monthly_snapshots(self, months: int = 12) -> List[Dict]:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT *
                        FROM snapshots_monthly
                        WHERE snapshot_date >= CURRENT_DATE - INTERVAL '%s months'
                        ORDER BY snapshot_date DESC
                    """, (months,))
                    columns = [desc[0] for desc in cur.description]
                    return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error getting monthly snapshots: {e}")
            return []
    
    def get_advanced_analytics(self) -> Dict:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT company_name, job_count FROM companies ORDER BY job_count DESC LIMIT 20")
                    top_companies = [{'company': row[0], 'jobs': row[1]} for row in cur.fetchall()]
                    cur.execute("SELECT location, COUNT(*) as count FROM job_archive WHERE status = 'active' AND location IS NOT NULL GROUP BY location ORDER BY count DESC LIMIT 20")
                    top_locations = [{'location': row[0], 'count': row[1]} for row in cur.fetchall()]
                    cur.execute("SELECT department, COUNT(*) as count FROM job_archive WHERE status = 'active' AND department IS NOT NULL GROUP BY department ORDER BY count DESC LIMIT 20")
                    top_departments = [{'department': row[0], 'count': row[1]} for row in cur.fetchall()]
                    cur.execute("SELECT ats_type, COUNT(*) as count, SUM(job_count) as total_jobs FROM companies GROUP BY ats_type ORDER BY count DESC")
                    ats_distribution = [{'ats': row[0], 'companies': row[1], 'jobs': row[2]} for row in cur.fetchall()]
                    return {
                        'top_companies': top_companies,
                        'top_locations': top_locations,
                        'top_departments': top_departments,
                        'ats_distribution': ats_distribution
                    }
        except Exception as e:
            logger.error(f"Error getting advanced analytics: {e}")
            return {}

_db_instance = None

def get_db() -> Database:
    global _db_instance
    if _db_instance is None:
        _db_instance = Database()
    return _db_instance
