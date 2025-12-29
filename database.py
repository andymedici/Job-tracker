"""Database Interface for Job Intelligence Platform - Production Grade with Smart Seed Rotation + Cleanup + Trends"""

import os
import logging
import json
import re
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def infer_work_type(title: str, location: str, description: str = None) -> Optional[str]:
    """Infer work type from job title, location, and description"""
    text = f"{title} {location} {description or ''}".lower()
    
    # Check for remote indicators
    remote_keywords = ['remote', 'work from home', 'wfh', 'distributed', 'anywhere', 'virtual', 'telecommute']
    hybrid_keywords = ['hybrid', 'flexible', 'remote-friendly', 'office/remote', 'remote or office']
    onsite_keywords = ['onsite', 'on-site', 'in-office', 'office-based', 'on site', 'in office']
    
    remote_score = sum(1 for kw in remote_keywords if kw in text)
    hybrid_score = sum(1 for kw in hybrid_keywords if kw in text)
    onsite_score = sum(1 for kw in onsite_keywords if kw in text)
    
    # Location-based inference
    if location:
        location_lower = location.lower()
        if 'remote' in location_lower or 'anywhere' in location_lower or 'worldwide' in location_lower:
            remote_score += 2
        elif 'hybrid' in location_lower:
            hybrid_score += 2
        elif any(city in location_lower for city in ['new york', 'san francisco', 'seattle', 'boston', 'austin', 'chicago', 'denver', 'atlanta', 'dallas']):
            # If specific city mentioned without remote, likely onsite
            if remote_score == 0 and hybrid_score == 0:
                onsite_score += 1
    
    # Determine work type
    if remote_score > hybrid_score and remote_score > onsite_score:
        return 'Remote'
    elif hybrid_score > remote_score and hybrid_score > onsite_score:
        return 'Hybrid'
    elif onsite_score > 0 or (remote_score == 0 and hybrid_score == 0):
        return 'Onsite'
    
    return None

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
                
                # Job archive
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
                cur.execute("CREATE INDEX IF NOT EXISTS idx_job_archive_title ON job_archive(title)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_job_archive_location ON job_archive(location)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_job_archive_work_type ON job_archive(work_type)")
                
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
                
                # Indexes for seed rotation performance
                cur.execute("CREATE INDEX IF NOT EXISTS idx_seeds_times_tested ON seed_companies(times_tested)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_seeds_success_rate ON seed_companies(success_rate)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_seeds_last_tested ON seed_companies(last_tested_at)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_seeds_blacklisted ON seed_companies(is_blacklisted)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_seeds_tier ON seed_companies(tier)")
                
                # Snapshots
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
                
                # Intelligence events
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
        token = name.lower()
        token = re.sub(r'\s+(inc|llc|ltd|co|corp|corporation|gmbh|sa|ag|plc)\.?$', '', token, flags=re.IGNORECASE)
        token = re.sub(r'[^a-z0-9\s-]', '', token)
        token = re.sub(r'[\s-]+', '-', token).strip('-')
        return token
    
    def _extract_skills_from_text(self, text: str) -> Dict[str, int]:
        """Extract skills from job title/description"""
        if not text:
            return {}
        
        skills = {}
        
        # Comprehensive skill patterns
        skill_patterns = {
            # Programming Languages
            'Python': r'\bPython\b',
            'JavaScript': r'\bJavaScript\b|\bJS\b',
            'TypeScript': r'\bTypeScript\b|\bTS\b',
            'Java': r'\bJava\b(?!Script)',
            'Go': r'\bGo\b|\bGolang\b',
            'Rust': r'\bRust\b',
            'C++': r'\bC\+\+\b',
            'C#': r'\bC#\b',
            'Ruby': r'\bRuby\b',
            'PHP': r'\bPHP\b',
            'Swift': r'\bSwift\b',
            'Kotlin': r'\bKotlin\b',
            
            # Frontend
            'React': r'\bReact\b|\bReactJS\b',
            'Vue': r'\bVue\.js\b|\bVue\b',
            'Angular': r'\bAngular\b',
            'Next.js': r'\bNext\.js\b',
            'Svelte': r'\bSvelte\b',
            'HTML': r'\bHTML\b',
            'CSS': r'\bCSS\b',
            'Tailwind': r'\bTailwind\b',
            
            # Backend
            'Node.js': r'\bNode\.?js\b',
            'Django': r'\bDjango\b',
            'Flask': r'\bFlask\b',
            'FastAPI': r'\bFastAPI\b',
            'Spring': r'\bSpring\b',
            'Express': r'\bExpress\b',
            
            # Cloud
            'AWS': r'\bAWS\b',
            'Azure': r'\bAzure\b',
            'GCP': r'\bGCP\b|\bGoogle Cloud\b',
            'Docker': r'\bDocker\b',
            'Kubernetes': r'\bKubernetes\b|\bK8s\b',
            'Terraform': r'\bTerraform\b',
            
            # Databases
            'SQL': r'\bSQL\b',
            'PostgreSQL': r'\bPostgreSQL\b|\bPostgres\b',
            'MySQL': r'\bMySQL\b',
            'MongoDB': r'\bMongoDB\b',
            'Redis': r'\bRedis\b',
            'Elasticsearch': r'\bElasticsearch\b',
            
            # Data & AI
            'Machine Learning': r'\bMachine Learning\b|\bML\b',
            'AI': r'\bAI\b|\bArtificial Intelligence\b',
            'Data Science': r'\bData Science\b',
            'TensorFlow': r'\bTensorFlow\b',
            'PyTorch': r'\bPyTorch\b',
            'Spark': r'\bSpark\b',
            
            # DevOps
            'CI/CD': r'\bCI/CD\b',
            'Jenkins': r'\bJenkins\b',
            'Git': r'\bGit\b(?!Hub|Lab)',
            'Linux': r'\bLinux\b',
            
            # Roles (for categorization)
            'Full Stack': r'\bFull[- ]?Stack\b',
            'Frontend': r'\bFront[- ]?end\b|\bFront[- ]?End\b',
            'Backend': r'\bBack[- ]?end\b|\bBack[- ]?End\b',
            'DevOps': r'\bDevOps\b',
            'Data Engineer': r'\bData Engineer\b',
            'Mobile': r'\bMobile\b|\biOS\b|\bAndroid\b',
        }
        
        text_lower = text.lower()
        for skill, pattern in skill_patterns.items():
            if re.search(pattern, text, re.IGNORECASE):
                skills[skill] = skills.get(skill, 0) + 1
        
        return skills
    
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
            
            # Track new locations for expansion detection
            new_locations = set()
            
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Get existing locations for this company
                    cur.execute("""
                        SELECT DISTINCT location 
                        FROM job_archive 
                        WHERE company_id = %s 
                        AND location IS NOT NULL
                    """, (company_id,))
                    existing_locations = {row[0].lower() for row in cur.fetchall()}
                    
                    cur.execute("SELECT job_id FROM job_archive WHERE company_id = %s AND status = 'active'", (company_id,))
                    current_job_ids = {row[0] for row in cur.fetchall()}
                    new_job_ids = {job['id'] for job in jobs}
                    closed_ids = current_job_ids - new_job_ids
                    
                    if closed_ids:
                        cur.execute("UPDATE job_archive SET status = 'closed', closed_at = NOW() WHERE company_id = %s AND job_id = ANY(%s) AND status = 'active'", (company_id, list(closed_ids)))
                        closed_count = cur.rowcount
                    
                    for job in jobs:
                        # Infer work_type if not provided
                        work_type = job.get('work_type')
                        if not work_type or work_type.strip() == '':
    
    def backfill_work_types(self) -> int:
        """Backfill work_type for existing jobs that don't have it set"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Get jobs without work_type
                    cur.execute("""
                        SELECT id, title, location, metadata
                        FROM job_archive
                        WHERE status = 'active'
                        AND (work_type IS NULL OR work_type = '')
                        LIMIT 10000
                    """)
                    jobs = cur.fetchall()
                    
                    updated = 0
                    for job_id, title, location, metadata in jobs:
                        # Extract description from metadata if available
                        description = None
                        if metadata and isinstance(metadata, dict):
                            description = metadata.get('description', '')
                        
                        # Infer work type
                        work_type = infer_work_type(title or '', location or '', description)
                        
                        if work_type:
                            cur.execute("""
                                UPDATE job_archive
                                SET work_type = %s
                                WHERE id = %s
                            """, (work_type, job_id))
                            updated += 1
                    
                    conn.commit()
                    logger.info(f"Backfilled work_type for {updated} jobs")
                    return updated
        except Exception as e:
            logger.error(f"Error backfilling work types: {e}")
            return 0
    
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
    
    # ========================================================================
    # SMART SEED ROTATION LOGIC
    # ========================================================================
    
    def get_seeds(self, limit: int = 100, prioritize_quality: bool = True) -> List[Dict]:
        """
        Get seeds for testing with intelligent rotation
        
        Priority order:
        1. Never tested seeds from tier 1 (highest quality)
        2. Never tested seeds from tier 2
        3. Never tested seeds from any tier
        4. Successful seeds tested 7+ days ago (re-test winners)
        5. Successful seeds tested 14+ days ago
        6. Low-tested seeds (1-2 times) from tier 1/2
        7. Everything else not blacklisted
        
        Uses RANDOM() within each priority group to ensure variety
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    if prioritize_quality:
                        query = """
                            WITH prioritized_seeds AS (
                                SELECT 
                                    company_name,
                                    company_name_token,
                                    source,
                                    tier,
                                    times_tested,
                                    times_successful,
                                    success_rate,
                                    last_tested_at,
                                    is_blacklisted,
                                    CASE
                                        -- Priority 1: Never tested tier 1 (premium sources)
                                        WHEN times_tested = 0 AND tier = 1 THEN 1
                                        
                                        -- Priority 2: Never tested tier 2 (public companies)
                                        WHEN times_tested = 0 AND tier = 2 THEN 2
                                        
                                        -- Priority 3: Never tested other tiers
                                        WHEN times_tested = 0 THEN 3
                                        
                                        -- Priority 4: High success seeds tested a week ago
                                        WHEN success_rate >= 50.0 AND 
                                             last_tested_at < NOW() - INTERVAL '7 days' THEN 4
                                        
                                        -- Priority 5: Good success seeds tested 2 weeks ago
                                        WHEN success_rate >= 30.0 AND 
                                             last_tested_at < NOW() - INTERVAL '14 days' THEN 5
                                        
                                        -- Priority 6: Low-tested tier 1 seeds (give them another chance)
                                        WHEN times_tested <= 2 AND tier = 1 THEN 6
                                        
                                        -- Priority 7: Low-tested tier 2 seeds
                                        WHEN times_tested <= 2 AND tier = 2 THEN 7
                                        
                                        -- Priority 8: Everything else
                                        ELSE 8
                                    END as priority,
                                    -- Randomize within each priority group
                                    RANDOM() as random_sort
                                FROM seed_companies
                                WHERE is_blacklisted = false
                            )
                            SELECT 
                                company_name,
                                company_name_token,
                                source,
                                tier,
                                times_tested,
                                times_successful,
                                success_rate,
                                last_tested_at
                            FROM prioritized_seeds
                            ORDER BY priority ASC, random_sort
                            LIMIT %s
                        """
                    else:
                        # Simple random selection of non-blacklisted seeds
                        query = """
                            SELECT 
                                company_name,
                                company_name_token,
                                source,
                                tier,
                                times_tested,
                                times_successful,
                                success_rate,
                                last_tested_at
                            FROM seed_companies
                            WHERE is_blacklisted = false
                            ORDER BY RANDOM()
                            LIMIT %s
                        """
                    
                    cur.execute(query, (limit,))
                    columns = [desc[0] for desc in cur.description]
                    seeds = [dict(zip(columns, row)) for row in cur.fetchall()]
                    
                    if seeds:
                        logger.info(f"🌱 Retrieved {len(seeds)} seeds for testing")
                        # Log rotation stats
                        never_tested = sum(1 for s in seeds if s['times_tested'] == 0)
                        retesting = len(seeds) - never_tested
                        logger.info(f"   - {never_tested} never tested, {retesting} re-testing")
                    
                    return seeds
        except Exception as e:
            logger.error(f"Error getting seeds: {e}")
            return []
    
    def increment_seed_tested(self, company_name: str):
        """Increment times_tested counter and update last_tested_at"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE seed_companies 
                        SET times_tested = times_tested + 1,
                            last_tested_at = NOW(),
                            success_rate = CASE 
                                WHEN times_tested + 1 > 0 
                                THEN ROUND((times_successful::DECIMAL / (times_tested + 1) * 100), 2)
                                ELSE 0 
                            END
                        WHERE company_name ILIKE %s
                    """, (company_name,))
                    conn.commit()
        except Exception as e:
            logger.debug(f"Error updating seed tested count: {e}")
    
    def increment_seed_success(self, company_name: str):
        """Increment success counter and recalculate success rate"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE seed_companies 
                        SET times_successful = times_successful + 1,
                            success_rate = CASE 
                                WHEN times_tested > 0 
                                THEN ROUND(((times_successful + 1)::DECIMAL / times_tested * 100), 2)
                                ELSE 100.0
                            END
                        WHERE company_name ILIKE %s
                    """, (company_name,))
                    conn.commit()
        except Exception as e:
            logger.debug(f"Error updating seed success count: {e}")
    
    def blacklist_poor_seeds(self, min_tests: int = 3, max_success_rate: float = 5.0) -> int:
        """Blacklist seeds that have been tested multiple times but never/rarely succeeded"""
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
                        logger.info(f"🚫 Blacklisted {len(blacklisted)} poor-performing seeds (tested {min_tests}+ times, <{max_success_rate}% success)")
                    return len(blacklisted)
        except Exception as e:
            logger.error(f"Error blacklisting seeds: {e}")
            return 0
    
    def cleanup_garbage_seeds(self) -> int:
        """Remove obviously bad seeds from database"""
        logger.info("🗑️ Starting garbage seed cleanup...")
        
        garbage_patterns = [
            '%log out%', '%logout%', '%login%', '%sign in%', '%sign out%', '%signin%',
            '%staff locations%', '%remote%jobs%', '%work from%', '%careers%page%',
            '%track awesome%', '%!%', '%[%', '%]%', '%{%', '%}%',
            'awsgoogle%', '%&%&%', '%|%|%', '%menu%', '%navigation%',
            '%apply%now%', '%search%', '%filter%', '%view%all%',
            '%table%contents%', '%external%links%', '%see%also%',
            '%references%', '%jump%to%', '%back%to%', '%click%here%',
            '%readme%', '%contributing%', '%license%', '%changelog%',
            '%skip%to%', '%scroll%to%', '%page%', '%previous%', '%next%',
            '%load%more%', '%show%all%', '%edit%', '%delete%', '%remove%',
        ]
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    total_deleted = 0
                    
                    # Delete by pattern matching
                    for pattern in garbage_patterns:
                        cur.execute("""
                            DELETE FROM seed_companies
                            WHERE company_name ILIKE %s
                        """, (pattern,))
                        deleted = cur.rowcount
                        if deleted > 0:
                            logger.info(f"   Deleted {deleted} seeds matching pattern: {pattern}")
                        total_deleted += deleted
                    
                    # Delete seeds with < 3 letters
                    cur.execute("""
                        DELETE FROM seed_companies
                        WHERE LENGTH(REGEXP_REPLACE(company_name, '[^a-zA-Z]', '', 'g')) < 3
                    """)
                    deleted = cur.rowcount
                    if deleted > 0:
                        logger.info(f"   Deleted {deleted} seeds with < 3 letters")
                    total_deleted += deleted
                    
                    # Delete seeds with 10+ words (likely concatenated garbage)
                    cur.execute("""
                        DELETE FROM seed_companies
                        WHERE array_length(string_to_array(company_name, ' '), 1) > 10
                    """)
                    deleted = cur.rowcount
                    if deleted > 0:
                        logger.info(f"   Deleted {deleted} seeds with 10+ words")
                    total_deleted += deleted
                    
                    # Delete seeds with excessive special characters (>30%)
                    cur.execute("""
                        DELETE FROM seed_companies
                        WHERE LENGTH(REGEXP_REPLACE(company_name, '[^a-zA-Z0-9 ]', '', 'g'))::DECIMAL / 
                              GREATEST(LENGTH(company_name), 1) < 0.7
                    """)
                    deleted = cur.rowcount
                    if deleted > 0:
                        logger.info(f"   Deleted {deleted} seeds with excessive special characters")
                    total_deleted += deleted
                    
                    # Delete seeds that are just numbers (using PostgreSQL regex)
                    cur.execute("""
                        DELETE FROM seed_companies
                        WHERE company_name ~ '^[0-9[:space:]_.\\-]+$'
                    """)
                    deleted = cur.rowcount
                    if deleted > 0:
                        logger.info(f"   Deleted {deleted} numeric-only seeds")
                    total_deleted += deleted
                    
                    # Delete seeds starting with special characters
                    cur.execute("""
                        DELETE FROM seed_companies
                        WHERE company_name ~ '^[^a-zA-Z0-9]'
                    """)
                    deleted = cur.rowcount
                    if deleted > 0:
                        logger.info(f"   Deleted {deleted} seeds starting with special chars")
                    total_deleted += deleted
                    
                    conn.commit()
                    logger.info(f"✅ Cleanup complete: Deleted {total_deleted} garbage seeds")
                    return total_deleted
        except Exception as e:
            logger.error(f"Error cleaning garbage seeds: {e}")
            return 0
    
    def get_seed_stats(self) -> Dict:
        """Get comprehensive seed statistics for dashboard"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            COUNT(*) as total_seeds,
                            COUNT(*) FILTER (WHERE times_tested = 0) as never_tested,
                            COUNT(*) FILTER (WHERE times_tested > 0 AND times_successful > 0) as successful,
                            COUNT(*) FILTER (WHERE is_blacklisted = true) as blacklisted,
                            COUNT(*) FILTER (WHERE tier = 1) as tier1_seeds,
                            COUNT(*) FILTER (WHERE tier = 2) as tier2_seeds,
                            ROUND(AVG(times_tested), 2) as avg_tests,
                            ROUND(AVG(success_rate) FILTER (WHERE times_tested > 0), 2) as avg_success_rate
                        FROM seed_companies
                    """)
                    row = cur.fetchone()
                    if row:
                        return {
                            'total_seeds': row[0] or 0,
                            'never_tested': row[1] or 0,
                            'successful': row[2] or 0,
                            'blacklisted': row[3] or 0,
                            'tier1_seeds': row[4] or 0,
                            'tier2_seeds': row[5] or 0,
                            'avg_tests': float(row[6]) if row[6] else 0.0,
                            'avg_success_rate': float(row[7]) if row[7] else 0.0
                        }
                    return {}
        except Exception as e:
            logger.error(f"Error getting seed stats: {e}")
            return {}
    
    # ========================================================================
    # END SMART SEED ROTATION LOGIC
    # ========================================================================
    
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
        """Get comprehensive time-to-fill metrics"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Overall metrics
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
                          AND closed_at > NOW() - INTERVAL '90 days'
                    """)
                    overall = cur.fetchone()
                    
                    # By work type
                    cur.execute("""
                        SELECT 
                            work_type,
                            AVG(EXTRACT(EPOCH FROM (closed_at - first_seen)) / 86400) as avg_days
                        FROM job_archive
                        WHERE status = 'closed'
                          AND closed_at IS NOT NULL
                          AND closed_at > first_seen
                          AND work_type IS NOT NULL
                          AND closed_at > NOW() - INTERVAL '90 days'
                        GROUP BY work_type
                    """)
                    by_work_type = {row['work_type']: round(row['avg_days'], 1) for row in cur.fetchall()}
                    
                    # By department
                    cur.execute("""
                        SELECT 
                            department,
                            AVG(EXTRACT(EPOCH FROM (closed_at - first_seen)) / 86400) as avg_days
                        FROM job_archive
                        WHERE status = 'closed'
                          AND closed_at IS NOT NULL
                          AND closed_at > first_seen
                          AND department IS NOT NULL
                          AND closed_at > NOW() - INTERVAL '90 days'
                        GROUP BY department
                        ORDER BY avg_days DESC
                        LIMIT 10
                    """)
                    by_department = {row['department']: round(row['avg_days'], 1) for row in cur.fetchall()}
                    
                    if overall and overall['sample_size'] > 0:
                        return {
                            'sample_size': overall['sample_size'],
                            'overall_avg_ttf_days': round(overall['avg_ttf_days'], 1) if overall['avg_ttf_days'] else 0,
                            'median_ttf_days': round(overall['median_ttf_days'], 1) if overall['median_ttf_days'] else 0,
                            'min_ttf_days': round(overall['min_ttf_days'], 1) if overall['min_ttf_days'] else 0,
                            'max_ttf_days': round(overall['max_ttf_days'], 1) if overall['max_ttf_days'] else 0,
                            'by_work_type': by_work_type,
                            'by_department': by_department
                        }
                    return {
                        'sample_size': 0,
                        'overall_avg_ttf_days': 0,
                        'median_ttf_days': 0,
                        'min_ttf_days': 0,
                        'max_ttf_days': 0,
                        'by_work_type': {},
                        'by_department': {}
                    }
        except Exception as e:
            logger.error(f"Error calculating TTF metrics: {e}")
            return {
                'sample_size': 0,
                'overall_avg_ttf_days': 0,
                'median_ttf_days': 0,
                'min_ttf_days': 0,
                'max_ttf_days': 0,
                'by_work_type': {},
                'by_department': {}
            }
    
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
        """Get comprehensive advanced analytics with skills extraction"""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Top companies
                    cur.execute("""
                        SELECT company_name, job_count, ats_type
                        FROM companies 
                        ORDER BY job_count DESC 
                        LIMIT 20
                    """)
                    top_companies = [dict(row) for row in cur.fetchall()]
                    
                    # Top hiring regions
                    cur.execute("""
                        SELECT location, COUNT(*) as count
                        FROM job_archive 
                        WHERE status = 'active' AND location IS NOT NULL
                        GROUP BY location 
                        ORDER BY count DESC 
                        LIMIT 20
                    """)
                    top_hiring_regions = {row['location']: row['count'] for row in cur.fetchall()}
                    
                    # Department distribution
                    cur.execute("""
                        SELECT department, COUNT(*) as count
                        FROM job_archive 
                        WHERE status = 'active' AND department IS NOT NULL
                        GROUP BY department 
                        ORDER BY count DESC 
                        LIMIT 20
                    """)
                    department_distribution = {row['department']: row['count'] for row in cur.fetchall()}
                    
                    # ATS distribution
                    cur.execute("""
                        SELECT ats_type, COUNT(*) as companies, SUM(job_count) as jobs
                        FROM companies 
                        GROUP BY ats_type 
                        ORDER BY companies DESC
                    """)
                    ats_distribution = [dict(row) for row in cur.fetchall()]
                    
                    # Work type distribution - FIXED VERSION
                    cur.execute("""
                        SELECT 
                            COUNT(*) FILTER (WHERE LOWER(work_type) LIKE '%remote%') as remote,
                            COUNT(*) FILTER (WHERE LOWER(work_type) LIKE '%hybrid%') as hybrid,
                            COUNT(*) FILTER (WHERE LOWER(work_type) LIKE '%onsite%' OR LOWER(work_type) LIKE '%on-site%' OR LOWER(work_type) LIKE '%office%') as onsite,
                            COUNT(*) as total
                        FROM job_archive
                        WHERE status = 'active'
                    """)
                    work_type_row = cur.fetchone()
                    
                    # Calculate percentages
                    total = work_type_row['total'] if work_type_row and work_type_row['total'] > 0 else 1
                    work_type_distribution = {
                        'remote': int(work_type_row['remote']) if work_type_row else 0,
                        'hybrid': int(work_type_row['hybrid']) if work_type_row else 0,
                        'onsite': int(work_type_row['onsite']) if work_type_row else 0,
                        'remote_percent': round((work_type_row['remote'] or 0) / total * 100, 1) if work_type_row else 0,
                        'hybrid_percent': round((work_type_row['hybrid'] or 0) / total * 100, 1) if work_type_row else 0,
                        'onsite_percent': round((work_type_row['onsite'] or 0) / total * 100, 1) if work_type_row else 0,
                    }
                    
                    logger.info(f"Work type distribution: {work_type_distribution}")
                    
                    # Salary insights
                    cur.execute("""
                        SELECT 
                            MIN(salary_min) as min_salary,
                            MAX(salary_max) as max_salary,
                            AVG((salary_min + salary_max) / 2) as median_salary,
                            COUNT(*) as with_salary
                        FROM job_archive
                        WHERE status = 'active' 
                          AND salary_min IS NOT NULL 
                          AND salary_max IS NOT NULL
                    """)
                    salary_row = cur.fetchone()
                    salary_insights = {
                        'min': int(salary_row['min_salary']) if salary_row['min_salary'] else None,
                        'max': int(salary_row['max_salary']) if salary_row['max_salary'] else None,
                        'median': int(salary_row['median_salary']) if salary_row['median_salary'] else None,
                        'jobs_with_salary': salary_row['with_salary'] or 0
                    }
                    
                    # Skills extraction from job titles
                    cur.execute("""
                        SELECT title
                        FROM job_archive
                        WHERE status = 'active' AND title IS NOT NULL
                    """)
                    jobs = cur.fetchall()
                    
                    all_skills = {}
                    for job in jobs:
                        skills = self._extract_skills_from_text(job['title'])
                        for skill, count in skills.items():
                            all_skills[skill] = all_skills.get(skill, 0) + count
                    
                    top_skills = dict(sorted(all_skills.items(), key=lambda x: x[1], reverse=True)[:30])
                    
                    # Fastest growing companies (last 14 days)
                    cur.execute("""
                        WITH recent AS (
                            SELECT DISTINCT ON (company_id)
                                company_id,
                                job_count as current_jobs,
                                snapshot_time
                            FROM snapshots_6h
                            WHERE snapshot_time >= NOW() - INTERVAL '1 day'
                            ORDER BY company_id, snapshot_time DESC
                        ),
                        old AS (
                            SELECT DISTINCT ON (company_id)
                                company_id,
                                job_count as old_jobs
                            FROM snapshots_6h
                            WHERE snapshot_time BETWEEN NOW() - INTERVAL '14 days' AND NOW() - INTERVAL '13 days'
                            ORDER BY company_id, snapshot_time DESC
                        )
                        SELECT 
                            c.company_name,
                            c.ats_type,
                            r.current_jobs,
                            (r.current_jobs - COALESCE(o.old_jobs, 0)) as job_change,
                            ROUND((r.current_jobs - COALESCE(o.old_jobs, 0))::DECIMAL / 14, 1) as jobs_per_day
                        FROM recent r
                        JOIN companies c ON r.company_id = c.id
                        LEFT JOIN old o ON r.company_id = o.company_id
                        WHERE (r.current_jobs - COALESCE(o.old_jobs, 0)) > 0
                        ORDER BY job_change DESC
                        LIMIT 10
                    """)
                    fastest_growing = [dict(row) for row in cur.fetchall()]
                    
                    # Time to fill
                    time_to_fill = self.get_time_to_fill_metrics()
                    
                    # Recent events
                    cur.execute("""
                        SELECT 
                            event_type,
                            COUNT(*) as event_count,
                            MAX(detected_at) as last_detected
                        FROM intelligence_events
                        WHERE detected_at >= NOW() - INTERVAL '30 days'
                        GROUP BY event_type
                        ORDER BY event_count DESC
                    """)
                    recent_events = [dict(row) for row in cur.fetchall()]
                    
                    return {
                        'top_companies': top_companies,
                        'top_hiring_regions': top_hiring_regions,
                        'department_distribution': department_distribution,
                        'ats_distribution': ats_distribution,
                        'work_type_distribution': work_type_distribution,
                        'salary_insights': salary_insights,
                        'top_skills': top_skills,
                        'fastest_growing': fastest_growing,
                        'time_to_fill': time_to_fill,
                        'recent_events': recent_events
                    }
        except Exception as e:
            logger.error(f"Error getting advanced analytics: {e}", exc_info=True)
            return {}

    # ========================================================================
    # TRENDS & RETENTION METRICS - NEW METHODS
    # ========================================================================
    
    def get_salary_trends(self, days=90):
        """Get salary trends over time"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        DATE_TRUNC('week', first_seen) as week,
                        AVG((salary_min + salary_max) / 2)::DECIMAL(10,2) as avg_salary,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (salary_min + salary_max) / 2)::DECIMAL(10,2) as median_salary,
                        COUNT(*) as job_count
                    FROM job_archive
                    WHERE first_seen > NOW() - INTERVAL %s
                    AND salary_min IS NOT NULL
                    AND salary_max IS NOT NULL
                    AND status = 'active'
                    GROUP BY week
                    ORDER BY week
                """, (f'{days} days',))
                
                return [dict(row) for row in cur.fetchall()]

    def get_skills_trends(self, days=90):
        """Get skills demand trends over time"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        DATE_TRUNC('week', first_seen) as week,
                        title,
                        COUNT(*) as job_count
                    FROM job_archive
                    WHERE first_seen > NOW() - INTERVAL %s
                    AND status = 'active'
                    GROUP BY week, title
                    ORDER BY week, job_count DESC
                """, (f'{days} days',))
                
                weekly_data = {}
                skills_to_track = [
                    'python', 'javascript', 'react', 'java', 'typescript',
                    'node', 'aws', 'kubernetes', 'docker', 'sql',
                    'go', 'rust', 'vue', 'angular', 'graphql',
                    'mongodb', 'postgresql', 'redis', 'kafka', 'spark',
                    'machine learning', 'ai', 'data science', 'devops',
                    'tensorflow', 'pytorch', 'backend', 'frontend', 'fullstack'
                ]
                
                for row in cur.fetchall():
                    week = row['week'].isoformat()
                    title = row['title'].lower()
                    count = row['job_count']
                    
                    if week not in weekly_data:
                        weekly_data[week] = {skill: 0 for skill in skills_to_track}
                    
                    for skill in skills_to_track:
                        if skill in title:
                            weekly_data[week][skill] += count
                
                return weekly_data

    def get_company_growth_trend(self, company_id, days=90):
        """Get job count trend for a specific company"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        DATE_TRUNC('day', snapshot_time) as date,
                        AVG(job_count)::INTEGER as avg_jobs,
                        MAX(job_count) as max_jobs,
                        MIN(job_count) as min_jobs,
                        AVG(active_jobs)::INTEGER as avg_active_jobs
                    FROM snapshots_6h
                    WHERE company_id = %s
                    AND snapshot_time > NOW() - INTERVAL %s
                    GROUP BY DATE_TRUNC('day', snapshot_time)
                    ORDER BY date
                """, (company_id, f'{days} days'))
                
                return [dict(row) for row in cur.fetchall()]

    def get_market_trends(self, days=90):
        """Get overall market hiring trends"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        DATE_TRUNC('day', snapshot_time) as date,
                        SUM(job_count)::INTEGER as total_jobs,
                        COUNT(DISTINCT company_id) as active_companies,
                        AVG(job_count)::DECIMAL(10,2) as avg_jobs_per_company
                    FROM snapshots_6h
                    WHERE snapshot_time > NOW() - INTERVAL %s
                    GROUP BY DATE_TRUNC('day', snapshot_time)
                    ORDER BY date
                """, (f'{days} days',))
                
                return [dict(row) for row in cur.fetchall()]

    def get_department_growth_trends(self, days=90):
        """Get hiring trends by department"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        DATE_TRUNC('week', first_seen) as week,
                        department,
                        COUNT(*) as new_jobs
                    FROM job_archive
                    WHERE first_seen > NOW() - INTERVAL %s
                    AND department IS NOT NULL
                    AND status = 'active'
                    GROUP BY week, department
                    ORDER BY week, new_jobs DESC
                """, (f'{days} days',))
                
                weekly_data = {}
                for row in cur.fetchall():
                    week = row['week'].isoformat()
                    dept = row['department']
                    count = row['new_jobs']
                    
                    if week not in weekly_data:
                        weekly_data[week] = {}
                    
                    weekly_data[week][dept] = count
                
                return weekly_data

    def get_retention_metrics(self):
        """Get job retention and refill metrics"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        AVG(EXTRACT(EPOCH FROM (COALESCE(closed_at, NOW()) - first_seen)) / 86400)::DECIMAL(10,2) as avg_days_open,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (COALESCE(closed_at, NOW()) - first_seen)) / 86400)::DECIMAL(10,2) as median_days_open,
                        COUNT(*) FILTER (WHERE closed_at IS NOT NULL) as closed_jobs,
                        COUNT(*) FILTER (WHERE closed_at IS NULL) as open_jobs
                    FROM job_archive
                    WHERE first_seen > NOW() - INTERVAL '90 days'
                """)
                
                retention = cur.fetchone()
                
                # Check for repeat postings
                cur.execute("""
                    WITH job_pairs AS (
                        SELECT 
                            j1.company_id,
                            j1.title,
                            j1.closed_at as first_closed,
                            j2.first_seen as reposted,
                            EXTRACT(EPOCH FROM (j2.first_seen - j1.closed_at)) / 86400 as days_between
                        FROM job_archive j1
                        JOIN job_archive j2 ON 
                            j1.company_id = j2.company_id 
                            AND j1.title = j2.title
                            AND j1.id != j2.id
                        WHERE j1.closed_at IS NOT NULL
                        AND j2.first_seen > j1.closed_at
                        AND j2.first_seen < j1.closed_at + INTERVAL '90 days'
                        AND j1.first_seen > NOW() - INTERVAL '180 days'
                    )
                    SELECT 
                        COUNT(*) as refilled_positions,
                        AVG(days_between)::DECIMAL(10,2) as avg_days_to_refill
                    FROM job_pairs
                """)
                
                refill = cur.fetchone()
                
                return {
                    'avg_days_open': float(retention['avg_days_open']) if retention['avg_days_open'] else 0,
                    'median_days_open': float(retention['median_days_open']) if retention['median_days_open'] else 0,
                    'closed_jobs': retention['closed_jobs'],
                    'open_jobs': retention['open_jobs'],
                    'refilled_positions': refill['refilled_positions'],
                    'avg_days_to_refill': float(refill['avg_days_to_refill']) if refill['avg_days_to_refill'] else 0
                }

    def cleanup_old_snapshots(self, days_to_keep=90):
        """Delete snapshots older than specified days"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM snapshots_6h 
                    WHERE snapshot_time < NOW() - INTERVAL %s
                """, (f'{days_to_keep} days',))
                deleted = cur.rowcount
                conn.commit()
                return deleted

    def add_performance_indexes(self):
        """Add missing indexes for better query performance"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Snapshot indexes
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_snapshots_time 
                    ON snapshots_6h(snapshot_time DESC)
                """)
                
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_snapshots_company_time 
                    ON snapshots_6h(company_id, snapshot_time DESC)
                """)
                
                # Job archive indexes
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_jobs_dates 
                    ON job_archive(first_seen DESC, last_seen DESC, closed_at)
                """)
                
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_jobs_salary 
                    ON job_archive(salary_min, salary_max) 
                    WHERE salary_min IS NOT NULL
                """)
                
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_jobs_department 
                    ON job_archive(department) 
                    WHERE department IS NOT NULL
                """)
                
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_jobs_location 
                    ON job_archive(location) 
                    WHERE location IS NOT NULL
                """)
                
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_jobs_work_type 
                    ON job_archive(work_type) 
                    WHERE work_type IS NOT NULL
                """)
                
                conn.commit()
                logger.info("✅ Performance indexes created")

    # ========================================================================
    # END TRENDS & RETENTION METRICS
    # ========================================================================


# ============================================================================
# GLOBAL DATABASE INSTANCE
# ============================================================================

_db_instance = None

def get_db() -> Database:
    global _db_instance
    if _db_instance is None:
        _db_instance = Database()
    return _db_instance
