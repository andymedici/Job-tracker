"""
Authentication & Authorization Middleware
Provides API key authentication with admin/user roles
"""
import os
import hashlib
import hmac
import secrets
from functools import wraps
from flask import request, jsonify, g
from typing import Optional, Callable
import logging

logger = logging.getLogger(__name__)

class AuthManager:
    """Manages API authentication and authorization"""
    
    def __init__(self):
        self.api_key = os.getenv('API_KEY')
        self.admin_key = os.getenv('ADMIN_API_KEY')
        
        if not self.api_key:
            # Generate a key if not set (for development only)
            self.api_key = secrets.token_urlsafe(32)
            logger.warning(f"⚠️ No API_KEY set. Generated: {self.api_key}")
        
        if not self.admin_key:
            self.admin_key = secrets.token_urlsafe(32)
            logger.warning(f"⚠️ No ADMIN_API_KEY set. Generated: {self.admin_key}")
        
        logger.info("✅ Authentication manager initialized")
    
    def verify_api_key(self, provided_key: str) -> bool:
        """Verify API key using constant-time comparison"""
        if not provided_key:
            return False
        return hmac.compare_digest(provided_key, self.api_key)
    
    def verify_admin_key(self, provided_key: str) -> bool:
        """Verify admin-level API key"""
        if not provided_key:
            return False
        return hmac.compare_digest(provided_key, self.admin_key)
    
    def get_api_key_from_request(self) -> Optional[str]:
        """Extract API key from request headers or query params"""
        # Priority 1: Header (recommended for production)
        key = request.headers.get('X-API-Key')
        if key:
            return key
        
        # Priority 2: Authorization Bearer token
        auth_header = request.headers.get('Authorization', '')
        if auth_header.startswith('Bearer '):
            return auth_header[7:]
        
        # Priority 3: Query parameter (less secure, for testing only)
        if os.getenv('ALLOW_QUERY_API_KEY', 'false').lower() == 'true':
            return request.args.get('api_key')
        
        return None
    
    def get_user_role(self, api_key: str) -> Optional[str]:
        """Determine user role based on API key"""
        if self.verify_admin_key(api_key):
            return 'admin'
        elif self.verify_api_key(api_key):
            return 'user'
        return None

# Global auth manager instance
auth_manager = AuthManager()

def require_api_key(f: Callable) -> Callable:
    """
    Decorator to require valid API key for endpoint access
    Allows both regular users and admins
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = auth_manager.get_api_key_from_request()
        
        if not api_key:
            logger.warning(f"⚠️ Missing API key for {request.path}")
            return jsonify({
                'error': 'Authentication required',
                'message': 'Please provide a valid API key in X-API-Key header'
            }), 401
        
        role = auth_manager.get_user_role(api_key)
        if not role:
            logger.warning(f"⚠️ Invalid API key attempt for {request.path}")
            return jsonify({
                'error': 'Invalid API key',
                'message': 'The provided API key is not valid'
            }), 401
        
        # Store role in Flask g object for use in route
        g.user_role = role
        g.api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()[:16]
        
        logger.debug(f"✅ Authenticated request: {request.path} (role: {role})")
        return f(*args, **kwargs)
    
    return decorated_function

def require_admin_key(f: Callable) -> Callable:
    """
    Decorator to require admin-level API key
    Only allows admin access
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = auth_manager.get_api_key_from_request()
        
        if not api_key:
            logger.warning(f"⚠️ Missing API key for admin endpoint {request.path}")
            return jsonify({
                'error': 'Authentication required',
                'message': 'Admin API key required'
            }), 401
        
        if not auth_manager.verify_admin_key(api_key):
            logger.warning(f"🚫 Non-admin attempted to access {request.path}")
            return jsonify({
                'error': 'Insufficient permissions',
                'message': 'This endpoint requires admin access'
            }), 403
        
        # Store admin role
        g.user_role = 'admin'
        g.api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()[:16]
        
        logger.info(f"✅ Admin access granted: {request.path}")
        return f(*args, **kwargs)
    
    return decorated_function

def optional_auth(f: Callable) -> Callable:
    """
    Decorator for endpoints that work with or without authentication
    Provides enhanced data for authenticated users
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = auth_manager.get_api_key_from_request()
        
        if api_key:
            role = auth_manager.get_user_role(api_key)
            if role:
                g.user_role = role
                g.authenticated = True
                g.api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()[:16]
            else:
                g.authenticated = False
        else:
            g.authenticated = False
            g.user_role = None
        
        return f(*args, **kwargs)
    
    return decorated_function

def generate_api_key() -> str:
    """Generate a secure random API key"""
    return secrets.token_urlsafe(32)

def hash_api_key(api_key: str) -> str:
    """Hash API key for logging (first 16 chars of SHA256)"""
    return hashlib.sha256(api_key.encode()).hexdigest()[:16]
