"""
Configuration Management
Centralized application configuration
"""
import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class Config:
    """Application configuration"""
    
    # Database
    DATABASE_URL: str = os.getenv('DATABASE_URL', '')
    DB_POOL_SIZE: int = int(os.getenv('DB_POOL_SIZE', 15))
    DB_MAX_OVERFLOW: int = int(os.getenv('DB_MAX_OVERFLOW', 25))
    
    # Security
    API_KEY: str = os.getenv('API_KEY', '')
    ADMIN_API_KEY: str = os.getenv('ADMIN_API_KEY', '')
    
    # Redis
    REDIS_URL: Optional[str] = os.getenv('REDIS_URL')
    
    # Features
    ENABLE_WEB_SCRAPING: bool = os.getenv('ENABLE_WEB_SCRAPING', 'true').lower() == 'true'
    ENABLE_EMAIL_REPORTS: bool = os.getenv('ENABLE_EMAIL_REPORTS', 'false').lower() == 'true'
    ALLOW_QUERY_API_KEY: bool = os.getenv('ALLOW_QUERY_API_KEY', 'false').lower() == 'true'
    
    # Flask
    PORT: int = int(os.getenv('PORT', 8080))
    DEBUG: bool = os.getenv('DEBUG', 'false').lower() == 'true'
    
    # Scheduler
    SCHEDULER_TIMEZONE: str = os.getenv('SCHEDULER_TIMEZONE', 'UTC')
    REFRESH_INTERVAL_HOURS: int = int(os.getenv('REFRESH_INTERVAL_HOURS', 6))
    
    # Monitoring
    SENTRY_DSN: Optional[str] = os.getenv('SENTRY_DSN')
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')

config = Config()
