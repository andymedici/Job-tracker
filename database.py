"""
Database Module for PostgreSQL
==============================
Handles all database operations with PostgreSQL for Railway deployment.

Railway automatically provides DATABASE_URL environment variable when you
add a PostgreSQL plugin to your project.

Includes:
- Connection pooling
- Schema management
- Seed company priority system
- Source hit rate tracking
"""

import os
import json
import logging
import time # <-- ADDED for retry sleep
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from contextlib import contextmanager
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Database:
    """PostgreSQL database manager for Railway deployment."""
    
    def __init__(self, database_url: str = None):
        self.database_url = database_url or os.getenv('DATABASE_URL')
        
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is required")
        
        # Parse the URL for connection parameters
        self.conn_params = self._parse_database_url(self.database_url)
        
        # Create connection pool
        self.pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            **self.conn_params
        )
        
        # Initialize schema
        self._init_schema()
    
    def _parse_database_url(self, url: str) -> Dict[str, Any]:
        """Parse DATABASE_URL into connection parameters."""
        parsed = urlparse(url)
        
        return {
            'host': parsed.hostname,
            'port': parsed.port or 5432,
            'database': parsed.path[1:],  # Remove leading slash
            'user': parsed.username,
            'password': parsed.password,
            'sslmode': 'require'  # Railway requires SSL
        }
    
    @contextmanager
    def get_connection(self):
        """Get a connection from the pool."""
        conn = self.pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            self.pool.putconn(conn)
    
    @contextmanager
    def get_cursor(self, dict_cursor: bool = True):
        """Get a cursor with automatic connection handling."""
        with self.get_connection() as conn:
            cursor_factory = RealDictCursor if dict_cursor else None
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
            finally:
                cursor.close()
    
    def _init_schema(self):
        """Initialize database schema."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Main companies table
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
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Individual jobs table
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
            
            # Monthly snapshots for trends
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS monthly_snapshots (
                    id SERIAL PRIMARY KEY,
                    company_id TEXT NOT NULL,
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    job_count INTEGER,
                    remote_count INTEGER,
                    hybrid_count INTEGER,
                    onsite_count INTEGER,
                    snapshot_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(company_id, year, month)
                )
            """)
            
            # Seed companies with priority system
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS seed_companies (
                    name TEXT PRIMARY KEY,
                    source TEXT,
                    source_tier INTEGER DEFAULT 2,
                    priority INTEGER DEFAULT 50,
                    tested_count INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    last_tested TIMESTAMP,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Job history archive (for location and job count change detection)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_history_archive (
                    id SERIAL PRIMARY KEY,
                    company_id TEXT NOT NULL,
                    job_count INTEGER,
                    locations_json JSONB,
                    departments_json JSONB,
                    archive_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(company_id, archive_date)
                )
            """)
            
            # Location expansion alerts
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS location_expansions (
                    id SERIAL PRIMARY KEY,
                    company_id TEXT NOT NULL,
                    new_location TEXT NOT NULL,
                    job_count_at_detection INTEGER,
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(company_id, new_location)
                )
            """)
            
            # Job count change alerts
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_count_changes (
                    id SERIAL PRIMARY KEY,
                    company_id TEXT NOT NULL,
                    previous_count INTEGER,
                    current_count INTEGER,
                    change_percent DECIMAL,
                    change_type TEXT, -- 'surge', 'decline', 'new'
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Source statistics (for seed expander performance)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS source_stats (
                    source TEXT PRIMARY KEY,
                    seeds_discovered INTEGER DEFAULT 0,
                    seeds_tested INTEGER DEFAULT 0,
                    hit_rate DECIMAL DEFAULT 0.0,
                    enabled BOOLEAN DEFAULT TRUE,
                    last_run TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Market-wide hourly totals (for trend charts)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS market_snapshots (
                    id SERIAL PRIMARY KEY,
                    snapshot_time TIMESTAMP NOT NULL UNIQUE DEFAULT CURRENT_TIMESTAMP,
                    total_companies INTEGER,
                    total_jobs INTEGER,
                    total_remote INTEGER,
                    total_hybrid INTEGER,
                    total_onsite INTEGER,
                    greenhouse_companies INTEGER,
                    greenhouse_jobs INTEGER,
                    lever_companies INTEGER,
                    lever_jobs INTEGER
                )
            """)
            
            # Weekly aggregated stats
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS weekly_stats (
                    id SERIAL PRIMARY KEY,
                    week_start DATE NOT NULL UNIQUE,
                    total_companies INTEGER,
                    total_jobs INTEGER,
                    remote_jobs INTEGER,
                    hybrid_jobs INTEGER,
                    onsite_jobs INTEGER,
                    new_companies INTEGER
                )
            """)
            
            conn.commit()


    # ==================== PUBLIC API ====================
    
    def upsert_company(self, company_data: Dict):
        """Insert or update a company record."""
        # ... (implementation omitted for brevity, no changes needed)
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO companies (
                    id, ats_type, token, company_name, job_count, remote_count, hybrid_count, onsite_count, last_job_count, locations, departments, first_seen, last_seen, last_updated
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                ON CONFLICT (id) DO UPDATE SET
                    job_count = EXCLUDED.job_count,
                    remote_count = EXCLUDED.remote_count,
                    hybrid_count = EXCLUDED.hybrid_count,
                    onsite_count = EXCLUDED.onsite_count,
                    locations = EXCLUDED.locations,
                    departments = EXCLUDED.departments,
                    last_seen = CURRENT_TIMESTAMP,
                    last_updated = CURRENT_TIMESTAMP
            """, (
                company_data['id'],
                company_data['ats_type'],
                company_data['token'],
                company_data['company_name'],
                company_data.get('job_count', 0),
                company_data.get('remote_count', 0),
                company_data.get('hybrid_count', 0),
                company_data.get('onsite_count', 0),
                company_data.get('job_count', 0), # last_job_count is initially the same as job_count
                json.dumps(company_data.get('locations', [])),
                json.dumps(company_data.get('departments', []))
            ))

    def update_company_job_count(self, company_id: str, new_count: int):
        """Update job count and last_job_count."""
        # ... (implementation omitted for brevity, no changes needed)
        with self.get_cursor() as cursor:
            # First, fetch the current job_count
            cursor.execute("SELECT job_count FROM companies WHERE id = %s", (company_id,))
            row = cursor.fetchone()
            if row:
                last_count = row['job_count']
                cursor.execute("""
                    UPDATE companies 
                    SET 
                        job_count = %s,
                        last_job_count = %s,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (new_count, last_count, company_id))

    def delete_stale_jobs(self, company_id: str, last_seen_before: datetime):
        """Delete jobs for a company that haven't been seen since last_seen_before."""
        # ... (implementation omitted for brevity, no changes needed)
        with self.get_cursor() as cursor:
            cursor.execute("""
                DELETE FROM jobs 
                WHERE company_id = %s AND last_seen < %s
            """, (company_id, last_seen_before))

    def upsert_job(self, job_data: Dict):
        """Insert or update a job record."""
        # ... (implementation omitted for brevity, no changes needed)
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO jobs (
                    id, company_id, title, location, department, work_type, url, first_seen, last_seen
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    location = EXCLUDED.location,
                    department = EXCLUDED.department,
                    work_type = EXCLUDED.work_type,
                    url = EXCLUDED.url,
                    last_seen = CURRENT_TIMESTAMP
            """, (
                job_data['id'],
                job_data['company_id'],
                job_data['title'],
                job_data.get('location'),
                job_data.get('department'),
                job_data.get('work_type'),
                job_data.get('url')
            ))

    def get_seeds_for_collection(self, limit: int = 500) -> List[Dict]:
        """
        Selects seeds for collection based on priority and recency.
        Prioritizes: 1. Untested, 2. High Priority, 3. Oldest last_tested.
        """
        # ... (implementation omitted for brevity, no changes needed)
        with self.get_cursor() as cursor:
            cursor.execute("""
                SELECT name, source, source_tier, priority 
                FROM seed_companies
                ORDER BY 
                    CASE WHEN tested_count = 0 THEN 0 ELSE 1 END, -- Untested first
                    priority DESC,                           -- Then by priority
                    last_tested ASC                          -- Then by staleness
                LIMIT %s
            """, (limit,))
            return list(cursor.fetchall())
            
    def update_seed_stats(self, name: str, success: bool):
        """Update seed company's tested and success counts."""
        # ... (implementation omitted for brevity, no changes needed)
        try:
            with self.get_cursor() as cursor:
                if success:
                    cursor.execute("""
                        UPDATE seed_companies 
                        SET tested_count = tested_count + 1, 
                            success_count = success_count + 1,
                            last_tested = CURRENT_TIMESTAMP
                        WHERE name = %s
                    """, (name,))
                else:
                    cursor.execute("""
                        UPDATE seed_companies 
                        SET tested_count = tested_count + 1,
                            last_tested = CURRENT_TIMESTAMP
                        WHERE name = %s
                    """, (name,))
        except Exception as e:
            logger.error(f"Error updating seed stats for {name}: {e}")
            
    def upsert_seed_companies(self, companies: List[str], source: str, tier: int, priority: Optional[int] = None) -> int:
        """Insert new seed companies with priority."""
        # ... (implementation omitted for brevity, no changes needed)
        # Default priorities by tier
        if priority is None:
            priority = {1: 80, 2: 50, 3: 30}.get(tier, 50)
            
        added = 0
        try:
            with self.get_cursor() as cursor:
                for company in companies:
                    try:
                        cursor.execute("""
                            INSERT INTO seed_companies (name, source, source_tier, priority)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (name) DO NOTHING
                        """, (company, source, tier, priority))
                        added += cursor.rowcount
                    except Exception as e:
                        logger.error(f"Error upserting seed {company}: {e}")
            self.update_source_stats(source, added, 0, 0.0)
        except Exception as e:
            logger.error(f"Error upserting batch for source {source}: {e}")
            
        return added

    def update_source_stats(self, source: str, discovered: int, tested: int, hit_rate: float):
        """Update statistics for a seed source."""
        # ... (implementation omitted for brevity, no changes needed)
        try:
            with self.get_cursor() as cursor:
                # Use a transaction to ensure integrity
                cursor.execute("""
                    INSERT INTO source_stats (source, seeds_discovered, seeds_tested, hit_rate)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (source) DO UPDATE SET
                        seeds_discovered = source_stats.seeds_discovered + EXCLUDED.seeds_discovered,
                        seeds_tested = source_stats.seeds_tested + EXCLUDED.seeds_tested,
                        hit_rate = EXCLUDED.hit_rate,
                        last_run = CURRENT_TIMESTAMP
                """, (source, discovered, tested, hit_rate))
        except Exception as e:
            logger.error(f"Error updating source stats for {source}: {e}")
            
    def get_source_stats(self) -> List[Dict]:
        """Get source statistics."""
        # ... (implementation omitted for brevity, no changes needed)
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT source, seeds_discovered, seeds_tested, hit_rate, enabled, last_run
                    FROM source_stats
                    ORDER BY hit_rate DESC
                """)
                return list(cursor.fetchall())
        except:
            return []

    def get_high_performing_sources(self, min_tested: int, min_hit_rate: float) -> List[str]:
        """Get sources with good hit rates."""
        # ... (implementation omitted for brevity, no changes needed)
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT source FROM source_stats
                    WHERE seeds_tested >= %s AND hit_rate >= %s AND enabled = TRUE
                    ORDER BY hit_rate DESC
                """, (min_tested, min_hit_rate))
                return [row['source'] for row in cursor]
        except:
            return []
    
    def disable_low_performing_source(self, source: str):
        """Disable a source with poor hit rate."""
        # ... (implementation omitted for brevity, no changes needed)
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    UPDATE source_stats SET enabled = FALSE WHERE source = %s
                """, (source,))
                logger.info(f"Disabled low-performing source: {source}")
        except Exception as e:
            logger.error(f"Error disabling source: {e}")
            
    def create_monthly_snapshot(self):
        """Create a new monthly job snapshot for all companies."""
        # ... (implementation omitted for brevity, no changes needed)
        now = datetime.utcnow()
        year = now.year
        month = now.month
        
        with self.get_cursor() as cursor:
            # Insert the latest job count for all companies
            cursor.execute("""
                INSERT INTO monthly_snapshots (company_id, year, month, job_count, remote_count, hybrid_count, onsite_count)
                SELECT id, %s, %s, job_count, remote_count, hybrid_count, onsite_count FROM companies
                ON CONFLICT (company_id, year, month) DO UPDATE SET
                    job_count = EXCLUDED.job_count,
                    remote_count = EXCLUDED.remote_count,
                    hybrid_count = EXCLUDED.hybrid_count,
                    onsite_count = EXCLUDED.onsite_count
            """, (year, month))
            
            # Also create a market-wide hourly snapshot
            self.create_market_snapshot()
            
    def create_market_snapshot(self):
        """Create an hourly market trend snapshot."""
        # ... (implementation omitted for brevity, no changes needed)
        now = datetime.utcnow()
        snapshot_time = now.replace(minute=0, second=0, microsecond=0)
        
        with self.get_cursor() as cursor:
            # Calculate total stats
            cursor.execute("""
                SELECT 
                    COUNT(id) as total_companies, 
                    COALESCE(SUM(job_count), 0) as total_jobs,
                    COALESCE(SUM(remote_count), 0) as total_remote,
                    COALESCE(SUM(hybrid_count), 0) as total_hybrid,
                    COALESCE(SUM(onsite_count), 0) as total_onsite
                FROM companies
            """)
            total_stats = cursor.fetchone()
            
            # Calculate ATS-specific stats
            cursor.execute("""
                SELECT ats_type, COUNT(id) as count, COALESCE(SUM(job_count), 0) as jobs 
                FROM companies GROUP BY ats_type
            """)
            ats_stats = {row['ats_type']: row for row in cursor}
            
            greenhouse_companies = ats_stats.get('greenhouse', {}).get('count', 0)
            greenhouse_jobs = ats_stats.get('greenhouse', {}).get('jobs', 0)
            lever_companies = ats_stats.get('lever', {}).get('count', 0)
            lever_jobs = ats_stats.get('lever', {}).get('jobs', 0)
            
            cursor.execute("""
                INSERT INTO market_snapshots (snapshot_time, total_companies, total_jobs, total_remote, total_hybrid, total_onsite, greenhouse_companies, greenhouse_jobs, lever_companies, lever_jobs)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (snapshot_time) DO UPDATE SET
                    total_companies = EXCLUDED.total_companies,
                    total_jobs = EXCLUDED.total_jobs,
                    total_remote = EXCLUDED.total_remote,
                    total_hybrid = EXCLUDED.total_hybrid,
                    total_onsite = EXCLUDED.total_onsite,
                    greenhouse_companies = EXCLUDED.greenhouse_companies,
                    greenhouse_jobs = EXCLUDED.greenhouse_jobs,
                    lever_companies = EXCLUDED.lever_companies,
                    lever_jobs = EXCLUDED.lever_jobs
            """, (
                snapshot_time,
                total_stats['total_companies'],
                total_stats['total_jobs'],
                total_stats['total_remote'],
                total_stats['total_hybrid'],
                total_stats['total_onsite'],
                greenhouse_companies,
                greenhouse_jobs,
                lever_companies,
                lever_jobs
            ))
            
    def get_stats(self) -> Dict:
        """Get database statistics."""
        # ... (implementation omitted for brevity, no changes needed)
        try:
            with self.get_cursor() as cursor:
                stats = {}
                
                # Company counts by ATS type
                cursor.execute("""
                    SELECT ats_type, COUNT(*) as count, COALESCE(SUM(job_count), 0) as jobs 
                    FROM companies 
                    GROUP BY ats_type
                """)
                for row in cursor:
                    stats[f"{row['ats_type']}_companies"] = row['count']
                    stats[f"{row['ats_type']}_jobs"] = row['jobs']
                    
                # Total counts
                stats['total_companies'] = stats.get('greenhouse_companies', 0) + stats.get('lever_companies', 0)
                stats['total_jobs'] = stats.get('greenhouse_jobs', 0) + stats.get('lever_jobs', 0)
                
                # Work type breakdown
                cursor.execute("""
                    SELECT 
                        COALESCE(SUM(remote_count), 0) as total_remote,
                        COALESCE(SUM(hybrid_count), 0) as total_hybrid,
                        COALESCE(SUM(onsite_count), 0) as total_onsite
                    FROM companies
                """)
                work_type_stats = cursor.fetchone()
                stats.update(work_type_stats)
                
                # Seed stats
                cursor.execute("""
                    SELECT COUNT(*) as total_seeds, COALESCE(SUM(tested_count), 0) as seeds_tested 
                    FROM seed_companies
                """)
                seed_stats = cursor.fetchone()
                stats.update(seed_stats)
                
                # Market Trend (Last 7 days)
                cursor.execute("""
                    SELECT total_jobs FROM market_snapshots
                    WHERE snapshot_time >= (NOW() - INTERVAL '7 days')
                    ORDER BY snapshot_time DESC
                """)
                jobs_history = [row['total_jobs'] for row in cursor]
                stats['job_trend'] = jobs_history
                
                return stats
        except Exception as e:
            logger.error(f"Error fetching stats: {e}")
            return {}

    def get_company_details(self, company_id: str) -> Optional[Dict]:
        """Get all details for a single company."""
        # ... (implementation omitted for brevity, no changes needed)
        try:
            with self.get_cursor() as cursor:
                # Get company core data
                cursor.execute("SELECT * FROM companies WHERE id = %s", (company_id,))
                company = cursor.fetchone()
                if not company:
                    return None
                    
                # Get job list
                cursor.execute("""
                    SELECT title, location, department, work_type, url 
                    FROM jobs 
                    WHERE company_id = %s 
                    ORDER BY title ASC
                """, (company_id,))
                company['jobs'] = list(cursor.fetchall())
                
                # Get monthly snapshots
                cursor.execute("""
                    SELECT year, month, job_count, remote_count, hybrid_count, onsite_count, snapshot_date FROM monthly_snapshots
                    WHERE company_id = %s 
                    ORDER BY year DESC, month DESC 
                    LIMIT 24
                """, (company_id,))
                company['snapshots'] = list(cursor.fetchall())
                
                # Get location expansions
                cursor.execute("""
                    SELECT new_location, detected_at, job_count_at_detection FROM location_expansions 
                    WHERE company_id = %s 
                    ORDER BY detected_at DESC
                """, (company_id,))
                company['expansions'] = list(cursor.fetchall())
                
                # Get job count changes
                cursor.execute("""
                    SELECT previous_count, current_count, change_percent, change_type, detected_at 
                    FROM job_count_changes 
                    WHERE company_id = %s 
                    ORDER BY detected_at DESC
                    LIMIT 10
                """, (company_id,))
                company['changes'] = list(cursor.fetchall())
                
                return dict(company)
        except Exception as e:
            logger.error(f"Error fetching company details for {company_id}: {e}")
            return None

    def get_market_trends(self, days: int = 30) -> List[Dict]:
        """Get market trends for charting."""
        # ... (implementation omitted for brevity, no changes needed)
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT snapshot_time, total_jobs, total_companies, total_remote, total_hybrid, total_onsite
                    FROM market_snapshots
                    WHERE snapshot_time >= (NOW() - INTERVAL '30 days')
                    ORDER BY snapshot_time ASC
                """, (days,))
                return list(cursor.fetchall())
        except Exception as e:
            logger.error(f"Error fetching market trends: {e}")
            return []
            
    def test_connection(self):
        """Test the connection by running a simple query."""
        with self.get_cursor(dict_cursor=False) as cursor:
            cursor.execute("SELECT 1")
            return cursor.fetchone()[0] == 1
            
    def close(self):
        """Close all connections."""
        if self.pool:
            self.pool.closeall()


# Singleton instance
_db_instance: Optional[Database] = None


def get_db(max_retries: int = 15, delay: int = 2) -> Database:
    """
    Get or create database instance with connection retry logic for deployment resilience.
    
    This function waits for the database service to be ready, preventing
    a fatal error on initial container startup.
    """
    global _db_instance
    
    if _db_instance is None:
        logger.info("Attempting to initialize database connection pool...")
        
        # FIX: Added retry logic
        for attempt in range(max_retries):
            try:
                _db_instance = Database()
                logger.info("Database connection pool initialized successfully.")
                return _db_instance
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Database connection failed (Attempt {attempt + 1}/{max_retries}). Retrying in {delay}s. Error: {e}")
                    time.sleep(delay)
                else:
                    logger.error(f"FATAL: Database connection failed after {max_retries} attempts.")
                    raise ValueError("Could not connect to the database. Check DATABASE_URL and service health.") from e

    return _db_instance


def init_db(database_url: str = None) -> Database:
    """Initialize database with optional URL override."""
    # This function is now deprecated in favor of the retry logic in get_db().
    raise NotImplementedError("Use get_db() to initialize the database connection for resilient deployment.")
