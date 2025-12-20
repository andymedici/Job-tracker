"""Rate limiting middleware using Redis"""

import os
import logging
from flask import request
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

logger = logging.getLogger(__name__)

def get_rate_limit_key():
    """Get rate limit key from API key or IP"""
    api_key = request.headers.get('X-API-Key')
    if api_key:
        return f"api_key:{api_key}"
    return get_remote_address()

def setup_rate_limiter(app):
    """Setup rate limiter with Redis backend"""
    redis_url = os.getenv('REDIS_URL')
    
    if redis_url:
        logger.info(f"✅ Rate limiter using Redis: {redis_url[:20]}...")
        limiter = Limiter(
            app=app,
            key_func=get_rate_limit_key,
            storage_uri=redis_url,
            default_limits=["1000 per day", "100 per hour"],
            storage_options={"socket_connect_timeout": 30}
        )
    else:
        logger.warning("⚠️ Rate limiter using in-memory storage (Redis not configured)")
        limiter = Limiter(
            app=app,
            key_func=get_rate_limit_key,
            default_limits=["1000 per day", "100 per hour"]
        )
    
    logger.info("✅ Rate limiter initialized")
    logger.info(f"   Default limits: 1000/day, 100/hour")
    logger.info(f"   Storage: {'Redis' if redis_url else 'In-Memory'}")
    
    return limiter
