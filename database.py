"""
Database Module for PostgreSQL
==============================
Handles all database operations with PostgreSQL for Railway deployment.

Railway automatically provides DATABASE_URL environment variable when you
add a PostgreSQL plugin to your project.
"""

import os
import json
import logging
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
                    locations JSONB DEFAULT '[]'::jsonb,
                    departments JSONB DEFAULT '[]'::jsonb,
                    source TEXT,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_job_count INTEGER DEFAULT 0,
                    UNIQUE(ats_type, token)
                )
            """)
            
            # Jobs table for detailed tracking
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    company_id TEXT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
                    title TEXT NOT NULL,
                    location TEXT,
                    department TEXT,
                    work_type TEXT,
                    url TEXT,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Monthly snapshots for historical tracking
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
            
            # Seed companies for discovery
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS seed_companies (
                    name TEXT PRIMARY KEY,
                    source TEXT,
                    category TEXT,
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    tested BOOLEAN DEFAULT FALSE,
                    has_greenhouse BOOLEAN,
                    has_lever BOOLEAN
                )
            """)
            
            # Failed lookups cache (negative caching)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS failed_lookups (
                    token TEXT NOT NULL,
                    ats_type TEXT NOT NULL,
                    failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    reason TEXT,
                    PRIMARY KEY (token, ats_type)
                )
            """)
            
            # Location expansions tracking
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS location_expansions (
                    id SERIAL PRIMARY KEY,
                    company_id TEXT NOT NULL,
                    company_name TEXT,
                    ats_type TEXT,
                    new_location TEXT NOT NULL,
                    previous_locations JSONB,
                    job_count_at_detection INTEGER,
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    notified BOOLEAN DEFAULT FALSE
                )
            """)
            
            # Job count changes tracking
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_count_changes (
                    id SERIAL PRIMARY KEY,
                    company_id TEXT NOT NULL,
                    company_name TEXT,
                    ats_type TEXT,
                    previous_count INTEGER,
                    current_count INTEGER,
                    change_percent REAL,
                    change_type TEXT,
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    notified BOOLEAN DEFAULT FALSE
                )
            """)
            
            # Historical job data archive
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS job_history_archive (
                    id SERIAL PRIMARY KEY,
                    company_id TEXT NOT NULL,
                    archive_date DATE NOT NULL,
                    job_count INTEGER,
                    remote_count INTEGER,
                    hybrid_count INTEGER,
                    onsite_count INTEGER,
                    locations_json JSONB,
                    departments_json JSONB,
                    UNIQUE(company_id, archive_date)
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
                    greenhouse_companies INTEGER,
                    lever_companies INTEGER,
                    new_companies_this_week INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Collection runs tracking
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS collection_runs (
                    id SERIAL PRIMARY KEY,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    companies_tested INTEGER DEFAULT 0,
                    greenhouse_found INTEGER DEFAULT 0,
                    lever_found INTEGER DEFAULT 0,
                    total_jobs INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'running'
                )
            """)
            
            # Create indexes for performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_companies_ats ON companies(ats_type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_companies_jobs ON companies(job_count DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_companies_last_seen ON companies(last_seen)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_company ON monthly_snapshots(company_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_failed_lookups_date ON failed_lookups(failed_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_loc_exp_date ON location_expansions(detected_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_changes_date ON job_count_changes(detected_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_archive_company ON job_history_archive(company_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_weekly_stats_week ON weekly_stats(week_start)")
            
            conn.commit()
            logger.info("PostgreSQL schema initialized successfully")
    
    def upsert_company(self, board) -> bool:
        """Insert or update a company."""
        try:
            company_id = f"{board.ats_type}:{board.token}"
            
            with self.get_cursor() as cursor:
                # Get previous job count for change detection
                cursor.execute(
                    "SELECT job_count FROM companies WHERE id = %s", 
                    (company_id,)
                )
                row = cursor.fetchone()
                last_job_count = row['job_count'] if row else 0
                
                cursor.execute("""
                    INSERT INTO companies (
                        id, ats_type, token, company_name, job_count,
                        remote_count, hybrid_count, onsite_count,
                        locations, departments, source, last_seen, last_job_count
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        company_name = EXCLUDED.company_name,
                        job_count = EXCLUDED.job_count,
                        remote_count = EXCLUDED.remote_count,
                        hybrid_count = EXCLUDED.hybrid_count,
                        onsite_count = EXCLUDED.onsite_count,
                        locations = EXCLUDED.locations,
                        departments = EXCLUDED.departments,
                        last_seen = EXCLUDED.last_seen,
                        last_job_count = companies.job_count
                """, (
                    company_id, board.ats_type, board.token, board.company_name,
                    board.job_count, board.remote_count, board.hybrid_count, board.onsite_count,
                    json.dumps(board.locations), json.dumps(board.departments),
                    board.source, datetime.utcnow(), last_job_count
                ))
                
                return True
        except Exception as e:
            logger.error(f"Error upserting company {board.token}: {e}")
            return False
    
    def create_monthly_snapshot(self, board):
        """Create monthly snapshot for trend tracking."""
        try:
            now = datetime.utcnow()
            company_id = f"{board.ats_type}:{board.token}"
            
            with self.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO monthly_snapshots (
                        company_id, year, month, job_count,
                        remote_count, hybrid_count, onsite_count
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (company_id, year, month) DO UPDATE SET
                        job_count = EXCLUDED.job_count,
                        remote_count = EXCLUDED.remote_count,
                        hybrid_count = EXCLUDED.hybrid_count,
                        onsite_count = EXCLUDED.onsite_count,
                        snapshot_date = CURRENT_TIMESTAMP
                """, (
                    company_id, now.year, now.month,
                    board.job_count, board.remote_count, board.hybrid_count, board.onsite_count
                ))
        except Exception as e:
            logger.error(f"Error creating snapshot: {e}")
    
    def is_recently_failed(self, token: str, ats_type: str, days: int = 7) -> bool:
        """Check if token was recently tested and failed."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT 1 FROM failed_lookups 
                    WHERE token = %s AND ats_type = %s
                    AND failed_at > NOW() - INTERVAL '%s days'
                """, (token, ats_type, days))
                return cursor.fetchone() is not None
        except:
            return False
    
    def record_failed_lookup(self, token: str, ats_type: str, reason: str = ""):
        """Record a failed lookup for caching."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO failed_lookups (token, ats_type, reason, failed_at)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (token, ats_type) DO UPDATE SET
                        failed_at = CURRENT_TIMESTAMP,
                        reason = EXCLUDED.reason
                """, (token, ats_type, reason))
        except Exception as e:
            logger.error(f"Error recording failed lookup: {e}")
    
    def get_stats(self) -> Dict:
        """Get database statistics."""
        try:
            with self.get_cursor() as cursor:
                stats = {}
                
                # Company counts by ATS type
                cursor.execute("""
                    SELECT ats_type, COUNT(*) as count, COALESCE(SUM(job_count), 0) as jobs
                    FROM companies GROUP BY ats_type
                """)
                for row in cursor:
                    stats[f"{row['ats_type']}_companies"] = row['count']
                    stats[f"{row['ats_type']}_jobs"] = int(row['jobs'])
                
                # Recent updates
                cursor.execute("""
                    SELECT COUNT(*) as count FROM companies 
                    WHERE last_seen > NOW() - INTERVAL '1 day'
                """)
                stats['updated_last_24h'] = cursor.fetchone()['count']
                
                # Total stats
                cursor.execute("SELECT COUNT(*) as count, COALESCE(SUM(job_count), 0) as jobs FROM companies")
                row = cursor.fetchone()
                stats['total_companies'] = row['count']
                stats['total_jobs'] = int(row['jobs'])
                
                return stats
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {'total_companies': 0, 'total_jobs': 0, 'error': str(e)}
    
    def get_seed_companies(self, limit: int = 2000) -> List[str]:
        """Get seed companies that haven't been tested."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT name FROM seed_companies 
                    WHERE tested = FALSE 
                    ORDER BY discovered_at DESC
                    LIMIT %s
                """, (limit,))
                return [row['name'] for row in cursor]
        except:
            return []
    
    def save_seed_companies(self, companies: List[str], source: str) -> int:
        """Save seed companies to database."""
        added = 0
        try:
            with self.get_cursor() as cursor:
                for company in companies:
                    if company and len(company) >= 2:
                        try:
                            cursor.execute("""
                                INSERT INTO seed_companies (name, source)
                                VALUES (%s, %s)
                                ON CONFLICT (name) DO NOTHING
                            """, (company.strip(), source))
                            if cursor.rowcount > 0:
                                added += 1
                        except:
                            pass
            logger.info(f"Saved {added} new seed companies from {source}")
        except Exception as e:
            logger.error(f"Error saving seed companies: {e}")
        return added
    
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
