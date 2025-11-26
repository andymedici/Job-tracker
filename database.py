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
                    category TEXT,
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    tested BOOLEAN DEFAULT FALSE,
                    tested_at TIMESTAMP,
                    has_greenhouse BOOLEAN,
                    has_lever BOOLEAN
                )
            """)
            
            # Source hit rate tracking
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS source_stats (
                    source TEXT PRIMARY KEY,
                    tier INTEGER DEFAULT 2,
                    total_seeds INTEGER DEFAULT 0,
                    seeds_tested INTEGER DEFAULT 0,
                    seeds_found INTEGER DEFAULT 0,
                    greenhouse_found INTEGER DEFAULT 0,
                    lever_found INTEGER DEFAULT 0,
                    hit_rate REAL DEFAULT 0.0,
                    last_fetch TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    enabled BOOLEAN DEFAULT TRUE
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
            
            # Historical job data archive (DAILY - one per day)
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
            
            # Granular snapshots (every 6 hours for short-term analysis)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS snapshots_6h (
                    id SERIAL PRIMARY KEY,
                    company_id TEXT NOT NULL,
                    snapshot_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    job_count INTEGER,
                    remote_count INTEGER,
                    hybrid_count INTEGER,
                    onsite_count INTEGER,
                    UNIQUE(company_id, snapshot_time)
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
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_seeds_priority ON seed_companies(priority DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_seeds_tested ON seed_companies(tested)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_seeds_source ON seed_companies(source)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_location_exp_company ON location_expansions(company_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_location_exp_date ON location_expansions(detected_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_changes_company ON job_count_changes(company_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_job_changes_date ON job_count_changes(detected_at)")
            
            conn.commit()
            logger.info("Database schema initialized")
    
    def upsert_company(self, company_data: Dict) -> bool:
        """Insert or update a company."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO companies (
                        id, ats_type, token, company_name, job_count,
                        remote_count, hybrid_count, onsite_count, last_job_count,
                        locations, departments, first_seen, last_seen, last_updated
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        company_name = EXCLUDED.company_name,
                        last_job_count = companies.job_count,
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
                    company_data.get('job_count', 0),
                    json.dumps(company_data.get('locations', [])),
                    json.dumps(company_data.get('departments', []))
                ))
                return True
        except Exception as e:
            logger.error(f"Error upserting company: {e}")
            return False
    
    def create_monthly_snapshot(self):
        """Create monthly snapshots for all companies."""
        now = datetime.utcnow()
        year, month = now.year, now.month
        
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO monthly_snapshots 
                    (company_id, year, month, job_count, remote_count, hybrid_count, onsite_count)
                    SELECT id, %s, %s, job_count, remote_count, hybrid_count, onsite_count
                    FROM companies
                    ON CONFLICT (company_id, year, month) DO UPDATE SET
                        job_count = EXCLUDED.job_count,
                        remote_count = EXCLUDED.remote_count,
                        hybrid_count = EXCLUDED.hybrid_count,
                        onsite_count = EXCLUDED.onsite_count,
                        snapshot_date = CURRENT_TIMESTAMP
                """, (year, month))
                logger.info(f"Created monthly snapshots for {year}-{month:02d}")
        except Exception as e:
            logger.error(f"Error creating monthly snapshot: {e}")
    
    def create_6h_snapshots(self):
        """Create 6-hourly snapshots for granular trend tracking."""
        try:
            with self.get_cursor() as cursor:
                # Round to nearest 6-hour block
                now = datetime.utcnow()
                hour_block = (now.hour // 6) * 6
                snapshot_time = now.replace(hour=hour_block, minute=0, second=0, microsecond=0)
                
                # Insert snapshots for all companies
                cursor.execute("""
                    INSERT INTO snapshots_6h 
                    (company_id, snapshot_time, job_count, remote_count, hybrid_count, onsite_count)
                    SELECT id, %s, job_count, remote_count, hybrid_count, onsite_count
                    FROM companies
                    ON CONFLICT (company_id, snapshot_time) DO UPDATE SET
                        job_count = EXCLUDED.job_count,
                        remote_count = EXCLUDED.remote_count,
                        hybrid_count = EXCLUDED.hybrid_count,
                        onsite_count = EXCLUDED.onsite_count
                """, (snapshot_time,))
                
                count = cursor.rowcount
                logger.info(f"📸 Created 6h snapshots for {count} companies at {snapshot_time}")
                
                # Clean up old 6h snapshots (keep 7 days = 28 snapshots per company)
                cutoff = now - timedelta(days=7)
                cursor.execute("DELETE FROM snapshots_6h WHERE snapshot_time < %s", (cutoff,))
                deleted = cursor.rowcount
                if deleted > 0:
                    logger.info(f"🗑️ Cleaned up {deleted} old 6h snapshots")
                    
        except Exception as e:
            logger.error(f"Error creating 6h snapshots: {e}")
    
    def create_market_snapshot(self):
        """Create a market-wide snapshot for overall trend tracking."""
        try:
            with self.get_cursor() as cursor:
                # Get current market totals
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_companies,
                        COALESCE(SUM(job_count), 0) as total_jobs,
                        COALESCE(SUM(remote_count), 0) as total_remote,
                        COALESCE(SUM(hybrid_count), 0) as total_hybrid,
                        COALESCE(SUM(onsite_count), 0) as total_onsite,
                        COUNT(*) FILTER (WHERE ats_type = 'greenhouse') as greenhouse_companies,
                        COALESCE(SUM(job_count) FILTER (WHERE ats_type = 'greenhouse'), 0) as greenhouse_jobs,
                        COUNT(*) FILTER (WHERE ats_type = 'lever') as lever_companies,
                        COALESCE(SUM(job_count) FILTER (WHERE ats_type = 'lever'), 0) as lever_jobs
                    FROM companies
                """)
                stats = dict(cursor.fetchone())
                
                # Round to nearest hour
                now = datetime.utcnow()
                snapshot_time = now.replace(minute=0, second=0, microsecond=0)
                
                cursor.execute("""
                    INSERT INTO market_snapshots 
                    (snapshot_time, total_companies, total_jobs, total_remote, total_hybrid, total_onsite,
                     greenhouse_companies, greenhouse_jobs, lever_companies, lever_jobs)
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
                """, (snapshot_time, stats['total_companies'], stats['total_jobs'],
                      stats['total_remote'], stats['total_hybrid'], stats['total_onsite'],
                      stats['greenhouse_companies'], stats['greenhouse_jobs'],
                      stats['lever_companies'], stats['lever_jobs']))
                
                logger.info(f"📊 Market snapshot: {stats['total_companies']} companies, {stats['total_jobs']} jobs")
                
                # Clean up old market snapshots (keep 30 days)
                cutoff = now - timedelta(days=30)
                cursor.execute("DELETE FROM market_snapshots WHERE snapshot_time < %s", (cutoff,))
                
        except Exception as e:
            logger.error(f"Error creating market snapshot: {e}")
    
    def get_market_trends(self, days: int = 7) -> List[Dict]:
        """Get market-wide trends for charting."""
        try:
            with self.get_cursor() as cursor:
                cutoff = datetime.utcnow() - timedelta(days=days)
                cursor.execute("""
                    SELECT snapshot_time, total_companies, total_jobs, 
                           total_remote, total_hybrid, total_onsite,
                           greenhouse_jobs, lever_jobs
                    FROM market_snapshots
                    WHERE snapshot_time > %s
                    ORDER BY snapshot_time ASC
                """, (cutoff,))
                return [dict(row) for row in cursor]
        except Exception as e:
            logger.error(f"Error getting market trends: {e}")
            return []
    
    def get_company_trends(self, company_id: str, days: int = 7) -> List[Dict]:
        """Get trend data for a specific company."""
        try:
            with self.get_cursor() as cursor:
                cutoff = datetime.utcnow() - timedelta(days=days)
                cursor.execute("""
                    SELECT snapshot_time, job_count, remote_count, hybrid_count, onsite_count
                    FROM snapshots_6h
                    WHERE company_id = %s AND snapshot_time > %s
                    ORDER BY snapshot_time ASC
                """, (company_id, cutoff))
                return [dict(row) for row in cursor]
        except Exception as e:
            logger.error(f"Error getting company trends: {e}")
            return []

    def is_recently_failed(self, token: str, ats_type: str, days: int = 7) -> bool:
        """Check if a token failed recently (negative cache)."""
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
    
    def record_failed_lookup(self, token: str, ats_type: str, reason: str = None):
        """Record a failed lookup for negative caching."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO failed_lookups (token, ats_type, reason)
                    VALUES (%s, %s, %s)
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
                
                # Seed stats
                cursor.execute("SELECT COUNT(*) as total, SUM(CASE WHEN tested THEN 1 ELSE 0 END) as tested FROM seed_companies")
                seed_row = cursor.fetchone()
                stats['total_seeds'] = seed_row['total'] or 0
                stats['seeds_tested'] = int(seed_row['tested'] or 0)
                
                return stats
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {'total_companies': 0, 'total_jobs': 0, 'error': str(e)}
    
    def get_seed_companies(self, limit: int = 2000, by_priority: bool = True) -> List[str]:
        """Get seed companies that haven't been tested, optionally by priority."""
        try:
            with self.get_cursor() as cursor:
                if by_priority:
                    cursor.execute("""
                        SELECT name FROM seed_companies 
                        WHERE tested = FALSE 
                        ORDER BY priority DESC, discovered_at DESC
                        LIMIT %s
                    """, (limit,))
                else:
                    cursor.execute("""
                        SELECT name FROM seed_companies 
                        WHERE tested = FALSE 
                        ORDER BY discovered_at DESC
                        LIMIT %s
                    """, (limit,))
                return [row['name'] for row in cursor]
        except:
            return []
    
    def get_companies_for_refresh(self, hours_since_update: int = 6, limit: int = 500) -> List[Dict]:
        """Get existing companies that need refreshing based on last update time."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT id, ats_type, token, company_name, job_count, 
                           remote_count, hybrid_count, onsite_count
                    FROM companies 
                    WHERE last_updated < NOW() - INTERVAL '%s hours'
                    ORDER BY last_updated ASC
                    LIMIT %s
                """, (hours_since_update, limit))
                return [dict(row) for row in cursor]
        except Exception as e:
            logger.error(f"Error getting companies for refresh: {e}")
            return []
    
    def get_all_tracked_companies(self) -> List[Dict]:
        """Get all tracked companies with their ATS info for refresh."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT id, ats_type, token, company_name
                    FROM companies 
                    ORDER BY job_count DESC
                """)
                return [dict(row) for row in cursor]
        except Exception as e:
            logger.error(f"Error getting all companies: {e}")
            return []
    
    def save_seed_companies(self, companies: List[str], source: str, 
                           tier: int = 2, priority: int = None) -> int:
        """Save seed companies to database with priority."""
        # Default priorities by tier
        if priority is None:
            priority = {1: 80, 2: 50, 3: 30}.get(tier, 50)
        
        added = 0
        try:
            with self.get_cursor() as cursor:
                for company in companies:
                    if company and len(company) >= 2:
                        try:
                            cursor.execute("""
                                INSERT INTO seed_companies (name, source, source_tier, priority)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (name) DO UPDATE SET
                                    priority = GREATEST(seed_companies.priority, EXCLUDED.priority)
                            """, (company.strip(), source, tier, priority))
                            if cursor.rowcount > 0:
                                added += 1
                        except:
                            pass
                
                # Update source stats
                cursor.execute("""
                    INSERT INTO source_stats (source, tier, total_seeds, last_fetch, last_updated)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (source) DO UPDATE SET
                        total_seeds = source_stats.total_seeds + %s,
                        last_fetch = CURRENT_TIMESTAMP,
                        last_updated = CURRENT_TIMESTAMP
                """, (source, tier, added, added))
                
            logger.info(f"Saved {added} new seed companies from {source} (tier {tier}, priority {priority})")
        except Exception as e:
            logger.error(f"Error saving seed companies: {e}")
        return added
    
    def mark_seed_tested(self, name: str, has_greenhouse: bool = False, has_lever: bool = False):
        """Mark a seed company as tested and record results."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    UPDATE seed_companies 
                    SET tested = TRUE, 
                        tested_at = CURRENT_TIMESTAMP,
                        has_greenhouse = %s, 
                        has_lever = %s
                    WHERE name = %s
                    RETURNING source
                """, (has_greenhouse, has_lever, name))
                
                row = cursor.fetchone()
                if row and row['source']:
                    # Update source stats
                    found = 1 if (has_greenhouse or has_lever) else 0
                    gh_found = 1 if has_greenhouse else 0
                    lv_found = 1 if has_lever else 0
                    
                    cursor.execute("""
                        UPDATE source_stats 
                        SET seeds_tested = seeds_tested + 1,
                            seeds_found = seeds_found + %s,
                            greenhouse_found = greenhouse_found + %s,
                            lever_found = lever_found + %s,
                            hit_rate = CASE 
                                WHEN seeds_tested + 1 > 0 
                                THEN (seeds_found + %s)::REAL / (seeds_tested + 1) 
                                ELSE 0 
                            END,
                            last_updated = CURRENT_TIMESTAMP
                        WHERE source = %s
                    """, (found, gh_found, lv_found, found, row['source']))
        except Exception as e:
            logger.error(f"Error marking seed tested: {e}")
    
    def get_source_stats(self) -> List[Dict]:
        """Get hit rate statistics for all sources."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("""
                    SELECT source, tier, total_seeds, seeds_tested, seeds_found,
                           greenhouse_found, lever_found, hit_rate, enabled, last_fetch
                    FROM source_stats
                    ORDER BY tier ASC, hit_rate DESC
                """)
                return [dict(row) for row in cursor]
        except:
            return []
    
    def get_high_performing_sources(self, min_tested: int = 100, min_hit_rate: float = 0.01) -> List[str]:
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
                """, (source,))
                logger.info(f"Disabled low-performing source: {source}")
        except Exception as e:
            logger.error(f"Error disabling source: {e}")
    
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
