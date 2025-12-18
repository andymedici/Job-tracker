"""
Utilities Module
================
Helper functions for normalization, hashing, and proxy management.
"""

import hashlib
import random
import re
from typing import Optional

# Common department mappings
DEPT_MAPPINGS = {
    'engineering': 'Engineering',
    'eng': 'Engineering',
    'r&d': 'Engineering',
    'dev': 'Engineering',
    'software': 'Engineering',
    'product': 'Product',
    'product management': 'Product',
    'design': 'Design',
    'ux': 'Design',
    'ui/ux': 'Design',
    'sales': 'Sales',
    'marketing': 'Marketing',
    'growth': 'Marketing',
    'customer success': 'Customer Success',
    'support': 'Customer Success',
    'finance': 'Finance',
    'accounting': 'Finance',
    'hr': 'HR',
    'people': 'HR',
    'talent': 'HR',
    'recruiting': 'HR',
    'operations': 'Operations',
    'legal': 'Legal',
    'it': 'IT',
    'security': 'IT'
}

def normalize_department(dept_name: str) -> str:
    """Normalizes department names to standard categories."""
    if not dept_name:
        return 'Other'
    
    clean_name = dept_name.lower().strip()
    
    for key, value in DEPT_MAPPINGS.items():
        if key == clean_name or key in clean_name.split():
            return value
    
    return 'Other'

def calculate_job_hash(company_id: str, title: str, location: str) -> str:
    """Creates a unique hash for a job to prevent ghost job duplicates."""
    raw = f"{company_id}|{title.strip().lower()}|{location.strip().lower()}"
    return hashlib.md5(raw.encode()).hexdigest()

class ProxyRotator:
    """Simple round-robin proxy rotator (Placeholder logic for now)."""
    
    def __init__(self, proxy_list: list = None):
        self.proxies = proxy_list or []
        self.current_index = 0
    
    def get_proxy(self) -> Optional[str]:
        if not self.proxies:
            return None
        proxy = self.proxies[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.proxies)
        return proxy

# Singleton
proxy_rotator = ProxyRotator()
