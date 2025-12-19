"""
Rate Limiting Middleware
Protects API endpoints from abuse using Flask-Limiter with Redis backend
"""
import os
import logging
from flask import Flask, request, g
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from typing import Callable

logger = logging.getLogger(__name__)

def get_rate_limit_key() -> str:
    """
    Generate rate limit key based on API key or IP address
    Authenticated users get higher limits
    """
    # Priority 1: Use API key hash if authenticated
    if hasattr(g, 'api_key_hash') and g.api_key_hash:
        return f"api_key:{g.api_key_hash}"
    
    # Priority 2: Use IP address
    return f"ip:{get_remote_address()}"

def create_limiter(app: Flask) -> Limiter:
    """
    Initialize and configure rate limiter
    Uses Redis if available, otherwise in-memory storage
    """
    redis_url = os.getenv('REDIS_URL')
    
    # Determine storage backend
    if redis_url:
        storage_uri = redis_url
        logger.info(f"✅ Rate limiter using Redis: {redis_url[:20]}...")
    else:
        storage_uri = 'memory://'
        logger.warning("⚠️ Rate limiter using in-memory storage (not recommended for production)")
    
    # Create limiter with custom key function
    limiter = Limiter(
        app=app,
        key_func=get_rate_limit_key,
        default_limits=["1000 per day", "100 per hour"],
        storage_uri=storage_uri,
        strategy="fixed-window",
        headers_enabled=True,  # Return rate limit info in headers
        swallow_errors=True,   # Don't crash on Redis errors
    )
    
    # Log rate limit configuration
    logger.info("✅ Rate limiter initialized")
    logger.info(f"   Default limits: 1000/day, 100/hour")
    logger.info(f"   Storage: {'Redis' if redis_url else 'Memory'}")
    
    return limiter

# Rate limit configurations for different endpoint types
RATE_LIMITS = {
    'health_check': None,  # No limit
    'public_read': "300 per hour",  # Public read endpoints
    'authenticated_read': "500 per hour",  # Authenticated read
    'write': "50 per hour",  # Write operations
    'expensive': "10 per hour",  # Expensive operations (collection, refresh)
    'very_expensive': "5 per hour",  # Very expensive (full expansion)
}

def adaptive_rate_limit(endpoint_type: str) -> str:
    """
    Get adaptive rate limit based on user role and endpoint type
    Admins get 2x the normal limits
    """
    base_limit = RATE_LIMITS.get(endpoint_type, "100 per hour")
    
    if base_limit is None:
        return None
    
    # Double limits for admin users
    if hasattr(g, 'user_role') and g.user_role == 'admin':
        # Parse limit and double it
        try:
            count, per, period = base_limit.split()
            doubled_count = int(count) * 2
            return f"{doubled_count} {per} {period}"
        except:
            pass
    
    return base_limit

# Custom error handler for rate limit exceeded
def rate_limit_exceeded_handler(e):
    """Custom error message when rate limit is exceeded"""
    return {
        'error': 'Rate limit exceeded',
        'message': f'Too many requests. Please try again later.',
        'retry_after': e.description
    }, 429
