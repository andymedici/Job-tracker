"""
Database Interface for Job Intelligence Platform
Handles PostgreSQL connection, queries, and data management
"""

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
    """PostgreSQL database interface with connection pooling"""
    
    def __init__(self, database_url: str = None):
        """Initialize database connection pool"""
        self.database_url = database_url or os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable not set")
        
        # Initialize connection pool
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
        
        # Create tables if they don't exist and apply migrations
        self._create_tables()
        logger.info("✅ Database connection pool initialized successfully")
    
    @contextmanager
    def get_connection(self):
        """Get a connection from the pool"""
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)
    
    def _create_tables(self):
        """Create database tables and apply schema migrations"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                
                # =============================================================
                # 1. CREATE TABLES (Used for fresh installs)
                # =============================================================
                
                # Companies table
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
                
                # Job archive table
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
                        status VARCHAR(20) DEFAULT 'active',
                        first_seen TIMESTAMP DEFAULT NOW(),
                        last_seen TIMESTAMP DEFAULT NOW(),
                        closed_at TIMESTAMP,
                        metadata JSONB,
                        UNIQUE(company_id, job_id)
                    )
                """)
                
                # Create index on status for faster queries
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_job_archive_status 
                    ON job_archive(status)
                """)
                
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_job_archive_company_status 
                    ON job_archive(company_id, status)
                """)
                
                # Seed companies table
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
                        created_at TIMESTAMP DEFAULT NOW(),
                        UNIQUE(company_name_token)
                    )
                """)
                
                # 6-hour snapshots table
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
                
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_snapshots_company_time 
                    ON snapshots_6h(company_id, snapshot_time DESC)
                """)
                
                # Monthly snapshots table
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
                
                # Intelligence events table
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
                
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_intel_events_type_time 
                    ON intelligence_events(event_type, detected_at DESC)
                """)
                
                
                # =============================================================
                # 2. SCHEMA MIGRATIONS (ALTER TABLE IF MISSING)
                # This ensures missing columns are added to an existing DB.
                # =============================================================
                logger.info("Starting schema migration checks...")

                # Define the columns that were throwing errors in your logs:
                migration_statements = [
                    # Fix 1: Missing 'company_name_token' in companies table
                    ("companies", "company_name_token", "VARCHAR(255) UNIQUE"),
                    
                    # Fix 2: Missing 'closed_at' in job_archive table (already there, but kept for robustness)
                    ("job_archive", "closed_at", "TIMESTAMP NULL"),
                    
                    # Fix 3: Missing 'active_jobs' in snapshots_6h table
                    ("snapshots_6h", "active_jobs", "INTEGER"),
                ]

                for table, column, definition in migration_statements:
                    try:
                        # Check if column exists before attempting to add it (safer approach)
                        cur.execute(f"""
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_name = '{table}' AND column_name = '{column}';
                        """)
                        if not cur.fetchone():
                            cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
                            logger.warning(f"✅ Migrated: Added column '{column}' to table '{table}'")
                        else:
                            logger.debug(f"Column '{column}' already exists in '{table}'. Skipping.")

                    except psycopg2.ProgrammingError as e:
                        logger.error(f"Migration error for {table}.{column}: {e}. Attempting rollback of current sub-transaction.")
                        conn.rollback() # Rollback the failed ALTER if needed

                # 4. Ensure necessary indexes for new columns exist
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_job_archive_closed_at ON job_archive(closed_at)
                """)
                logger.debug("Checked for idx_job_archive_closed_at index.")

                conn.commit()
    
    def _name_to_token(self, name: str) -> str:
        """Convert company name to URL-friendly token"""
        import re
        token = name.lower()
        token = re.sub(r'\s+(inc|llc|ltd|co|corp|corporation|gmbh|sa|ag|plc)\.?$', '', token, flags=re.IGNORECASE)
        token = re.sub(r'[^a-z0-9\s-]', '', token)
        token = re.sub(r'[\s-]+', '-', token).strip('-')
        return token
    
    # ========================================================================
    # Distributed Locking (for scheduled jobs)
    # ========================================================================
    
    def acquire_advisory_lock(self, lock_name: str, timeout: int = 0) -> bool:
        """Acquire a PostgreSQL advisory lock"""
        try:
            # Convert lock name to integer hash
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
        """Release a PostgreSQL advisory lock"""
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
    
    # ========================================================================
    # Company Management
    # ========================================================================
    
    def add_company(self, company_name: str, ats_type: str, board_url: str, 
                    job_count: int = 0, metadata: Dict = None) -> Optional[int]:
        """Add a new company to the database"""
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
        """Get company ID by name"""
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
        """Update company job count"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE companies 
                        SET job_count = %s, last_scraped = NOW()
                        WHERE id = %s
                    """, (job_count, company_id))
                    conn.commit()
        except Exception as e:
            logger.error(f"Error updating job count: {e}")
    
    def get_companies_for_refresh(self, hours_since_update: int = 6, limit: int = 500) -> List[Dict]:
        """Get companies that need refreshing"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT id, company_name, ats_type, board_url, job_count
                        FROM companies
                        WHERE last_scraped < NOW() - INTERVAL '%s hours'
                            OR last_scraped IS NULL
                        ORDER BY last_scraped ASC NULLS FIRST
                        LIMIT %s
                    """, (hours_since_update, limit))
                    
                    columns = [desc[0] for desc in cur.description]
                    return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error getting companies for refresh: {e}")
            return []
    
    # ========================================================================
    # Job Archive Management
    # ========================================================================
    
    def archive_jobs(self, company_id: int, jobs: List[Dict]) -> Tuple[int, int, int]:
        """Archive jobs and track status changes"""
        if not jobs:
            return 0, 0, 0
        
        try:
            new_count = 0
            updated_count = 0
            closed_count = 0
            
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Get current job IDs for this company
                    cur.execute("""
                        SELECT job_id FROM job_archive 
                        WHERE company_id = %s AND status = 'active'
                    """, (company_id,))
                    
                    current_job_ids = {row[0] for row in cur.fetchall()}
                    new_job_ids = {job['id'] for job in jobs}
                    
                    # Mark removed jobs as closed
                    closed_ids = current_job_ids - new_job_ids
                    if closed_ids:
                        cur.execute("""
                            UPDATE job_archive
                            SET status = 'closed', closed_at = NOW()
                            WHERE company_id = %s AND job_id = ANY(%s) AND status = 'active'
                        """, (company_id, list(closed_ids)))
                        closed_count = cur.rowcount
                    
                    # Insert or update jobs
                    for job in jobs:
                        cur.execute("""
                            INSERT INTO job_archive 
                            (company_id, job_id, title, location, department, work_type, job_url, posted_date, status, metadata)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'active', %s)
                            ON CONFLICT (company_id, job_id) 
                            DO UPDATE SET
                                title = EXCLUDED.title,
                                location = EXCLUDED.location,
                                department = EXCLUDED.department,
                                work_type = EXCLUDED.work_type,
                                job_url = EXCLUDED.job_url,
                                posted_date = EXCLUDED.posted_date,
                                last_seen = NOW(),
                                status = 'active',
                                metadata = EXCLUDED.metadata
                            RETURNING (xmax = 0) AS inserted
                        """, (
                            company_id,
                            job['id'],
                            job.get('title'),
                            job.get('location'),
                            job.get('department'),
                            job.get('work_type'),
                            job.get('url'),
                            job.get('posted_date'),
                            json.dumps(job.get('metadata', {}))
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
    
    # ========================================================================
    # Seed Company Management
    # ========================================================================
    
    def insert_seeds(self, seeds: List[Tuple[str, str, str, int]]) -> int:
        """Bulk insert seed companies"""
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
        """Add a manual seed company"""
        try:
            token = self._name_to_token(company_name)
            
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Check if already exists
                    cur.execute("""
                        SELECT 1 FROM seed_companies 
                        WHERE company_name_token = %s OR company_name ILIKE %s
                    """, (token, company_name))
                    
                    if cur.fetchone():
                        logger.info(f"Seed already exists: {company_name}")
                        return False
                    
                    # Check if already tracked
                    cur.execute("SELECT 1 FROM companies WHERE company_name ILIKE %s", (company_name,))
                    if cur.fetchone():
                        logger.info(f"Company already tracked: {company_name}")
                        return False
                    
                    # Insert new seed
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
        """Get seed companies, optionally prioritized by success rate"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    if prioritize_quality:
                        # Prioritize: untested seeds, then high success rate, then rest
                        cur.execute("""
                            SELECT company_name, company_name_token, source, tier, 
                                   times_tested, success_rate, website_url
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
                            SELECT company_name, company_name_token, source, tier,
                                   times_tested, success_rate, website_url
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
        """Increment times_tested for a seed"""
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
        """Increment times_successful for a seed"""
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
        """Blacklist seeds with poor success rates"""
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
    
    # ========================================================================
    # Snapshots & Time-Series Data
    # ========================================================================
    
    def create_company_snapshots(self) -> int:
        """Create 6-hour snapshots for all companies"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # NOTE: This query now relies on 'active_jobs' existing in snapshots_6h
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
            # The log showed ERROR:database:Error creating snapshots: column "active_jobs" of relation "snapshots_6h" does not exist
            # This should be fixed by the migration logic in _create_tables()
            logger.error(f"Error creating snapshots: {e}")
            return 0
    
    def create_monthly_snapshot(self) -> bool:
        """Create monthly aggregate snapshot"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Get aggregate stats
                    cur.execute("""
                        SELECT 
                            COUNT(DISTINCT c.id) as total_companies,
                            COALESCE(SUM(c.job_count), 0) as total_jobs,
                            COALESCE(AVG(c.job_count), 0) as avg_jobs
                        FROM companies c
                    """)
                    
                    total_companies, total_jobs, avg_jobs = cur.fetchone()
                    
                    # Top locations
                    cur.execute("""
                        SELECT location, COUNT(*) as count
                        FROM job_archive
                        WHERE status = 'active' AND location IS NOT NULL
                        GROUP BY location
                        ORDER BY count DESC
                        LIMIT 20
                    """)
                    
                    top_locations = [{'location': row[0], 'count': row[1]} for row in cur.fetchall()]
                    
                    # Top departments
                    cur.execute("""
                        SELECT department, COUNT(*) as count
                        FROM job_archive
                        WHERE status = 'active' AND department IS NOT NULL
                        GROUP BY department
                        ORDER BY count DESC
                        LIMIT 20
                    """)
                    
                    top_departments = [{'department': row[0], 'count': row[1]} for row in cur.fetchall()]
                    
                    # ATS distribution
                    cur.execute("""
                        SELECT ats_type, COUNT(*) as count
                        FROM companies
                        GROUP BY ats_type
                        ORDER BY count DESC
                    """)
                    
                    ats_distribution = [{'ats': row[0], 'count': row[1]} for row in cur.fetchall()]
                    
                    # Insert monthly snapshot
                    cur.execute("""
                        INSERT INTO snapshots_monthly 
                        (snapshot_date, total_companies, total_jobs, avg_jobs_per_company, 
                         top_locations, top_departments, ats_distribution)
                        VALUES (CURRENT_DATE, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (snapshot_date) DO UPDATE SET
                            total_companies = EXCLUDED.total_companies,
                            total_jobs = EXCLUDED.total_jobs,
                            avg_jobs_per_company = EXCLUDED.avg_jobs_per_company,
                            top_locations = EXCLUDED.top_locations,
                            top_departments = EXCLUDED.top_departments,
                            ats_distribution = EXCLUDED.ats_distribution
                    """, (
                        total_companies,
                        total_jobs,
                        avg_jobs,
                        json.dumps(top_locations),
                        json.dumps(top_departments),
                        json.dumps(ats_distribution)
                    ))
                    
                    conn.commit()
                    logger.info("Created monthly snapshot")
                    return True
        except Exception as e:
            logger.error(f"Error creating monthly snapshot: {e}")
            return False
    
    # ========================================================================
    # Intelligence & Analytics
    # ========================================================================
    
    def get_job_count_changes(self, days: int = 7, threshold_percent: float = 10.0) -> Tuple[List[Dict], List[Dict]]:
        """Get companies with significant job count changes (surges and declines)"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # NOTE: This query relies on 'active_jobs' existing in snapshots_6h
                    cur.execute("""
                        WITH recent_snapshots AS (
                            SELECT DISTINCT ON (company_id)
                                company_id,
                                job_count as current_count,
                                snapshot_time
                            FROM snapshots_6h
                            WHERE snapshot_time >= NOW() - INTERVAL '1 day'
                            ORDER BY company_id, snapshot_time DESC
                        ),
                        old_snapshots AS (
                            SELECT DISTINCT ON (company_id)
                                company_id,
                                job_count as old_count
                            FROM snapshots_6h
                            WHERE snapshot_time >= NOW() - INTERVAL '%s days'
                              AND snapshot_time < NOW() - INTERVAL '%s days'
                            ORDER BY company_id, snapshot_time DESC
                        )
                        SELECT 
                            c.company_name,
                            c.id as company_id,
                            o.old_count,
                            r.current_count,
                            (r.current_count - o.old_count) as job_change,
                            ROUND(((r.current_count - o.old_count)::DECIMAL / NULLIF(o.old_count, 0) * 100), 1) as percent_change
                        FROM recent_snapshots r
                        JOIN old_snapshots o ON r.company_id = o.company_id
                        JOIN companies c ON c.id = r.company_id
                        WHERE o.old_count > 0
                          AND ABS((r.current_count - o.old_count)::DECIMAL / o.old_count * 100) >= %s
                        ORDER BY ABS(r.current_count - o.old_count) DESC
                    """, (days, days, threshold_percent))
                    
                    # NOTE: The log output error LINE 6:             FROM jobs j
                    # This suggests another query was running later that referred to 'jobs' instead of 'job_archive'.
                    # This specific function seems okay, but check for other 'jobs' references in your main application logic.
                    
                    columns = [desc[0] for desc in cur.description]
                    results = [dict(zip(columns, row)) for row in cur.fetchall()]

                    surges = [r for r in results if r['job_change'] > 0]
                    freezes = [r for r in results if r['job_change'] < 0]

                    return surges, freezes
        except Exception as e:
            logger.error(f"Error getting job count changes: {e}")
            return [], []


    def calculate_ttf_metrics(self) -> List[Dict]:
        """Calculate Time To Fill (TTF) metrics for recently closed jobs"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # NOTE: This query relies on 'closed_at' existing in job_archive
                    cur.execute("""
                        SELECT
                            c.company_name,
                            c.id as company_id,
                            AVG(EXTRACT(EPOCH FROM (j.closed_at - j.first_seen))) / 86400 AS avg_ttf_days,
                            COUNT(j.id) as closed_count
                        FROM job_archive j
                        JOIN companies c ON j.company_id = c.id
                        WHERE j.status = 'closed'
                        AND j.closed_at >= NOW() - INTERVAL '30 days'
                        AND j.first_seen IS NOT NULL 
                        AND j.closed_at IS NOT NULL
                        GROUP BY c.company_name, c.id
                        HAVING COUNT(j.id) > 5
                        ORDER BY avg_ttf_days ASC
                    """)
                    
                    columns = [desc[0] for desc in cur.description]
                    return [dict(zip(columns, row)) for row in cur.fetchall()]

        except Exception as e:
            # The log showed ERROR:database:Error calculating TTF metrics: column "closed_at" does not exist
            # This should be fixed by the migration logic in _create_tables()
            logger.error(f"Error calculating TTF metrics: {e}")
            return []

# ... (rest of your existing database.py code) ...

# Global instance of the Database class
# This will initialize the pool and create tables when the module is imported.
DB_CLIENT = Database() 

# Function wrapper required by main.py's import statement
def get_db() -> Database:
    """Returns the globally initialized Database client instance."""
    return DB_CLIENT

# Context manager wrapper for database connections (if needed in main.py)
# This is equivalent to your existing get_connection method, but exposed globally.
get_connection = DB_CLIENT.get_connection

# ========================================================================
# EOF
# ========================================================================
