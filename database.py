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
    
    def _parse_database_url(self, url: str) -> Dict[str, str]:
        """Parses a standard DATABASE_URL string into psycopg2 connection parameters."""
        result = urlparse(url)
        return {
            "database": result.path[1:],
            "user": result.username,
            "password": result.password,
            "host": result.hostname,
            "port": result.port
        }

    @contextmanager
    def get_cursor(self, commit: bool = True):
        """Context manager to get a database connection and cursor."""
        conn = None
        try:
            conn = self.pool.getconn()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            yield cursor
            if commit:
                conn.commit()
        except Exception as e:
            logger.error(f"Database operation error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self.pool.putconn(conn)

    def _init_schema(self):
        """Initializes the database schema."""
        with self.get_cursor() as cursor:
            # Table for job board companies (normalized data)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    id TEXT PRIMARY KEY,
                    company_name TEXT NOT NULL,
                    ats_type TEXT NOT NULL,
                    token TEXT NOT NULL,
                    job_count INTEGER,
                    remote_count INTEGER,
                    hybrid_count INTEGER,
                    onsite_count INTEGER,
                    locations JSONB,
                    departments JSONB,
                    last_collected TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    first_collected TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    UNIQUE (ats_type, token)
                );
            """)

            # Table for all discovered company names (seeds)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS seed_companies (
                    id SERIAL PRIMARY KEY,
                    company_name TEXT UNIQUE NOT NULL,
                    token_slug TEXT UNIQUE NOT NULL,  -- UPGRADE: New column for pre-calculated slug
                    source TEXT NOT NULL,
                    discovered_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    last_tested TIMESTAMP WITH TIME ZONE,
                    tested_count INTEGER DEFAULT 0,
                    hit_count INTEGER DEFAULT 0,
                    priority INTEGER DEFAULT 100
                );
            """)

            # Table for job details (historical and current)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_details (
                    id TEXT PRIMARY KEY,
                    company_id TEXT REFERENCES companies(id),
                    title TEXT NOT NULL,
                    location TEXT,
                    department TEXT,
                    raw_content TEXT,
                    created_at TIMESTAMP WITH TIME ZONE,
                    collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """)
            
            # Table for monthly snapshots (for historical analysis)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS monthly_snapshots (
                    id SERIAL PRIMARY KEY,
                    snapshot_month DATE UNIQUE NOT NULL,
                    total_companies INTEGER,
                    total_jobs INTEGER,
                    avg_remote_pct NUMERIC,
                    avg_hybrid_pct NUMERIC,
                    avg_onsite_pct NUMERIC,
                    data JSONB
                );
            """)

            # Table for tracking source performance
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS source_stats (
                    source TEXT PRIMARY KEY,
                    seeds_discovered INTEGER DEFAULT 0,
                    seeds_tested INTEGER DEFAULT 0,
                    hit_count INTEGER DEFAULT 0,
                    hit_rate NUMERIC DEFAULT 0.0,
                    enabled BOOLEAN DEFAULT TRUE,
                    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """)
            
            # Table for market intelligence alerts (expansions, surges)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS market_alerts (
                    id SERIAL PRIMARY KEY,
                    alert_type TEXT NOT NULL, -- 'expansion', 'surge', 'decline'
                    company_id TEXT REFERENCES companies(id),
                    company_name TEXT NOT NULL,
                    data JSONB NOT NULL,
                    detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """)
            
            # Ensure indexes exist for performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_companies_ats_token ON companies (ats_type, token);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_details_company_id ON job_details (company_id);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_seed_companies_priority ON seed_companies (priority DESC);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_seed_companies_tested_count ON seed_companies (tested_count);")
            
            logger.info("Database schema initialized/updated successfully.")

    def upsert_company(self, data: Dict):
        """Inserts or updates a company and its current job data."""
        # Clean up locations and departments from potential sets/tuples to lists
        locations = list(data.get('locations', []))
        departments = list(data.get('departments', []))
        
        # Upsert company data
        with self.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO companies (id, company_name, ats_type, token, job_count, remote_count, hybrid_count, onsite_count, locations, departments, last_collected)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (id) DO UPDATE 
                SET 
                    company_name = EXCLUDED.company_name,
                    job_count = EXCLUDED.job_count,
                    remote_count = EXCLUDED.remote_count,
                    hybrid_count = EXCLUDED.hybrid_count,
                    onsite_count = EXCLUDED.onsite_count,
                    locations = EXCLUDED.locations,
                    departments = EXCLUDED.departments,
                    last_collected = NOW();
            """, (
                data['id'], data['company_name'], data['ats_type'], data['token'],
                data['job_count'], data.get('remote_count', 0), data.get('hybrid_count', 0), data.get('onsite_count', 0),
                json.dumps(locations), json.dumps(departments)
            ))
            
            # Delete old job details for this company before inserting new ones
            cursor.execute("DELETE FROM job_details WHERE company_id = %s;", (data['id'],))

            # Insert new job details
            if data.get('jobs'):
                jobs_data = [
                    (
                        job['id'],
                        data['id'],
                        job['title'],
                        job.get('location'),
                        job.get('department'),
                        job.get('raw_content'),
                        job.get('created_at', datetime.utcnow()) # Assuming job has 'created_at' or default to now
                    )
                    for job in data['jobs']
                ]
                # Use execute_batch for performance
                psycopg2.extras.execute_batch(
                    cursor,
                    """
                        INSERT INTO job_details (id, company_id, title, location, department, raw_content, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """,
                    jobs_data
                )
        
        logger.info(f"Upserted company: {data['company_name']} ({data['job_count']} jobs)")

    def upsert_seed_company(self, name: str, token_slug: str, source: str, priority: int = 100):
        """Inserts or updates a discovered company name with its pre-calculated token slug."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO seed_companies (company_name, token_slug, source, priority) 
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (company_name) DO UPDATE 
                    SET 
                        source = EXCLUDED.source, -- Keep the original source or update to latest
                        priority = GREATEST(seed_companies.priority, EXCLUDED.priority),
                        token_slug = EXCLUDED.token_slug; -- Update the slug if it changes
                """, (name, token_slug, source, priority))
                
                # Update source stats
                cursor.execute("""
                    INSERT INTO source_stats (source, seeds_discovered) VALUES (%s, 1)
                    ON CONFLICT (source) DO UPDATE SET seeds_discovered = source_stats.seeds_discovered + 1, last_updated = NOW();
                """, (source,))
                
                return True
        except Exception as e:
            # Often triggered by a unique constraint violation on token_slug if the slugging logic is too simple
            # and produces the same slug for two different company names. This is expected.
            if 'unique constraint' not in str(e):
                logger.error(f"Error upserting seed company {name}: {e}")
            return False

    def mark_seeds_tested(self, ids: List[int], last_tested: datetime):
        """Marks a batch of seed companies as tested."""
        if not ids:
            return
        
        with self.get_cursor() as cursor:
            # Convert list of IDs to a format psycopg2 can use (tuple)
            id_tuple = tuple(ids)
            
            # Mark the seeds as tested
            cursor.execute("""
                UPDATE seed_companies 
                SET 
                    tested_count = tested_count + 1, 
                    last_tested = %s
                WHERE id IN %s;
            """, (last_tested, id_tuple))

    def mark_seed_hit(self, id: int):
        """Marks a seed company as a successful hit."""
        with self.get_cursor() as cursor:
            cursor.execute("""
                UPDATE seed_companies 
                SET hit_count = hit_count + 1
                WHERE id = %s;
            """, (id,))

    def get_seeds_for_collection(self, limit: int) -> List[Tuple[int, str, str]]:
        """
        Gets a batch of high-priority, low-tested seed companies.
        Returns a list of (id, company_name, token_slug)
        """
        try:
            with self.get_cursor() as cursor:
                # Prioritize:
                # 1. Seeds with high priority that have never been tested (tested_count = 0)
                # 2. Seeds with high priority that have the lowest tested_count (stale data)
                # 3. Seeds not tested in the last 7 days (to prevent re-testing too frequently)
                cursor.execute("""
                    SELECT 
                        id, 
                        company_name, 
                        token_slug  -- UPGRADE: Retrieve the slug
                    FROM seed_companies
                    WHERE 
                        (last_tested IS NULL OR last_tested < NOW() - INTERVAL '7 days') 
                        AND tested_count < 5 -- Cap max tests to prevent endless retries on dead seeds
                    ORDER BY 
                        CASE WHEN tested_count = 0 THEN 0 ELSE 1 END, -- Untested first
                        priority DESC,                               -- High priority first
                        tested_count ASC                             -- Lowest tested count first
                    LIMIT %s;
                """, (limit,))
                # Return the ID, name, and pre-calculated slug
                return [(row['id'], row['company_name'], row['token_slug']) for row in cursor]
        except Exception as e:
            logger.error(f"Error getting seeds for collection: {e}")
            return []

    # UPGRADE: New function for dashboard stats
    def get_untested_seed_count(self) -> int:
        """Get the total number of seed companies that have never been tested."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM seed_companies WHERE tested_count = 0;")
                return cursor.fetchone()['count']
        except Exception as e:
            logger.error(f"Error getting untested seed count: {e}")
            return 0

    def update_source_stats(self, source: str, tested_count: int, hit_count: int):
        """Updates the performance stats for a given seed source."""
        with self.get_cursor() as cursor:
            # This logic assumes the source was inserted during seed upsert.
            cursor.execute("""
                UPDATE source_stats 
                SET 
                    seeds_tested = seeds_tested + %s, 
                    hit_count = hit_count + %s,
                    hit_rate = (CAST(hit_count AS NUMERIC) + %s) / (CAST(seeds_tested AS NUMERIC) + %s),
                    last_updated = NOW()
                WHERE source = %s;
            """, (tested_count, hit_count, hit_count, tested_count, source))

    def get_source_stats(self) -> List[Dict]:
        """Returns all source performance statistics."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        source, 
                        seeds_discovered, 
                        seeds_tested, 
                        hit_count, 
                        hit_rate, 
                        enabled, 
                        last_updated
                    FROM source_stats
                    ORDER BY hit_rate DESC, seeds_tested DESC;
                """)
                return cursor.fetchall()
        except:
            return []
            
    def get_high_performing_sources(self, min_tested: int = 50, min_hit_rate: float = 0.01) -> List[str]:
        """Get sources with good hit rates."""
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
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    UPDATE source_stats SET enabled = FALSE WHERE source = %s
                """,[source])
                logger.info(f"Disabled low-performing source: {source}")
        except Exception as e:
            logger.error(f"Error disabling source: {e}")

    def create_monthly_snapshot(self):
        """Creates an aggregate snapshot of current data for historical analysis."""
        snapshot_month = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0).date()
        
        with self.get_cursor() as cursor:
            # 1. Get current stats
            stats = self.get_stats()
            total_companies = stats.get('total_companies', 0)
            total_jobs = stats.get('total_jobs', 0)
            
            # 2. Calculate aggregation metrics
            if total_companies > 0:
                cursor.execute("""
                    SELECT 
                        AVG(remote_count) AS avg_remote, 
                        AVG(hybrid_count) AS avg_hybrid, 
                        AVG(onsite_count) AS avg_onsite
                    FROM companies
                    WHERE job_count > 0;
                """)
                agg_row = cursor.fetchone()
                
                avg_remote_pct = (agg_row['avg_remote'] / total_jobs) * 100 if total_jobs > 0 and agg_row['avg_remote'] is not None else 0
                avg_hybrid_pct = (agg_row['avg_hybrid'] / total_jobs) * 100 if total_jobs > 0 and agg_row['avg_hybrid'] is not None else 0
                avg_onsite_pct = (agg_row['avg_onsite'] / total_jobs) * 100 if total_jobs > 0 and agg_row['avg_onsite'] is not None else 0
            else:
                avg_remote_pct = avg_hybrid_pct = avg_onsite_pct = 0.0

            # 3. Get detailed data (Company ID, Jobs, Location Breakdown)
            cursor.execute("""
                SELECT 
                    id, company_name, ats_type, job_count, remote_count, hybrid_count, onsite_count, locations, departments
                FROM companies
                WHERE job_count > 0;
            """)
            detailed_data = cursor.fetchall()
            
            # 4. Insert/Update snapshot
            cursor.execute("""
                INSERT INTO monthly_snapshots (snapshot_month, total_companies, total_jobs, avg_remote_pct, avg_hybrid_pct, avg_onsite_pct, data)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (snapshot_month) DO UPDATE 
                SET 
                    total_companies = EXCLUDED.total_companies,
                    total_jobs = EXCLUDED.total_jobs,
                    avg_remote_pct = EXCLUDED.avg_remote_pct,
                    avg_hybrid_pct = EXCLUDED.avg_hybrid_pct,
                    avg_onsite_pct = EXCLUDED.avg_onsite_pct,
                    data = EXCLUDED.data;
            """, (
                snapshot_month,
                total_companies,
                total_jobs,
                avg_remote_pct,
                avg_hybrid_pct,
                avg_onsite_pct,
                json.dumps([dict(row) for row in detailed_data])
            ))
            logger.info(f"Monthly snapshot created for {snapshot_month}. Companies: {total_companies}")

    def purge_old_job_details(self, days_to_keep: int):
        """Removes job details older than a specified number of days."""
        threshold = datetime.utcnow() - timedelta(days=days_to_keep)
        with self.get_cursor() as cursor:
            cursor.execute("DELETE FROM job_details WHERE collected_at < %s;", (threshold,))
            logger.info(f"Purged old job details older than {days_to_keep} days.")
            
    def purge_stale_companies(self, days_stale: int):
        """Removes companies that haven't been successfully collected in a specified number of days."""
        threshold = datetime.utcnow() - timedelta(days=days_stale)
        with self.get_cursor() as cursor:
            # Select IDs of companies to delete
            cursor.execute("SELECT id FROM companies WHERE last_collected < %s;", (threshold,))
            company_ids_to_delete = [row['id'] for row in cursor.fetchall()]
            
            if company_ids_to_delete:
                # Delete related job details first (due to foreign key)
                cursor.execute("DELETE FROM job_details WHERE company_id IN %s;", (tuple(company_ids_to_delete),))
                
                # Delete the stale companies
                cursor.execute("DELETE FROM companies WHERE id IN %s;", (tuple(company_ids_to_delete),))
                
                logger.info(f"Purged {len(company_ids_to_delete)} stale companies older than {days_stale} days.")


    def close(self):
        """Close all connections."""
        if self.pool:
            self.pool.closeall()


# Singleton instance
_db_instance: Optional[Database] = None


def get_db() -> Database:
    """Get or create database instance."""
    global _db_instance
    if _db_instance is None:
        _db_instance = Database()
    return _db_instance


def init_db(database_url: str = None) -> Database:
    """Initialize database with optional URL override."""
    global _db_instance
    _db_instance = Database(database_url)
    return _db_instance
