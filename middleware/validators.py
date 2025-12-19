"""
Input Validation Layer
Pydantic models for request validation and sanitization
"""
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Literal
from datetime import datetime
import re

class SeedCreateRequest(BaseModel):
    """Validation for adding new seed companies"""
    companies: List[str] = Field(..., min_length=1, max_length=100, 
                                 description="List of company names to add")
    
    @field_validator('companies')
    @classmethod
    def validate_companies(cls, v):
        """Sanitize and validate company names"""
        if not v:
            raise ValueError('At least one company name required')
        
        cleaned = []
        for company in v:
            # Strip whitespace
            company = company.strip()
            
            # Reject if too short or too long
            if len(company) < 2:
                continue
            if len(company) > 200:
                company = company[:200]
            
            # Remove potentially dangerous characters
            company = re.sub(r'[<>"\'`]', '', company)
            
            # Must have at least one letter
            if not re.search(r'[a-zA-Z]', company):
                continue
            
            cleaned.append(company)
        
        if not cleaned:
            raise ValueError('No valid company names provided after sanitization')
        
        return cleaned

    model_config = {"extra": "forbid"}

class SeedExpansionRequest(BaseModel):
    """Validation for seed expansion requests"""
    tier: Literal['tier1', 'tier2', 'full'] = Field(
        default='full',
        description="Expansion tier: tier1 (high-quality), tier2 (broader), or full (both)"
    )

    model_config = {"extra": "forbid"}

class CollectionRequest(BaseModel):
    """Validation for collection/discovery requests"""
    max_companies: Optional[int] = Field(
        default=500,
        ge=1,
        le=2000,
        description="Maximum number of companies to process"
    )
    force_refresh: bool = Field(
        default=False,
        description="Force refresh even if recently updated"
    )

    model_config = {"extra": "forbid"}

class RefreshRequest(BaseModel):
    """Validation for company refresh requests"""
    hours_since_update: int = Field(
        default=6,
        ge=1,
        le=168,  # Max 1 week
        description="Only refresh companies not updated in X hours"
    )
    max_companies: int = Field(
        default=500,
        ge=1,
        le=2000,
        description="Maximum number of companies to refresh"
    )

    model_config = {"extra": "forbid"}

class TrendsRequest(BaseModel):
    """Validation for market trends API requests"""
    days: int = Field(
        default=7,
        ge=1,
        le=365,
        description="Number of days of trend data to retrieve"
    )
    granularity: Literal['hourly', 'daily', 'weekly'] = Field(
        default='daily',
        description="Data granularity"
    )

    model_config = {"extra": "forbid"}

class IntelRequest(BaseModel):
    """Validation for intelligence API requests"""
    days: int = Field(
        default=7,
        ge=1,
        le=90,
        description="Number of days to analyze for intelligence"
    )
    min_change_percent: float = Field(
        default=0.10,
        ge=0.01,
        le=1.0,
        description="Minimum percentage change to report (0.10 = 10%)"
    )

    model_config = {"extra": "forbid"}

class AnalyticsRequest(BaseModel):
    """Validation for advanced analytics requests"""
    include_metrics: List[Literal[
        'time_to_fill', 'top_skills', 'top_regions', 
        'department_dist', 'work_type_dist', 'fastest_growing', 
        'ats_dist', 'events'
    ]] = Field(
        default=['time_to_fill', 'top_skills', 'top_regions'],
        description="Metrics to include in response"
    )
    limit: int = Field(
        default=20,
        ge=1,
        le=100,
        description="Limit for list results"
    )

    model_config = {"extra": "forbid"}

# Validation helper functions

def validate_request(model: type[BaseModel], data: dict):
    """
    Validate request data against Pydantic model
    Returns (validated_data, error_response)
    """
    try:
        validated = model(**data)
        return validated, None
    except Exception as e:
        error_response = {
            'error': 'Validation failed',
            'message': str(e),
            'details': e.errors() if hasattr(e, 'errors') else [str(e)]
        }
        return None, error_response

def sanitize_string(s: str, max_length: int = 1000) -> str:
    """Sanitize user input string"""
    if not s:
        return ''
    
    # Trim to max length
    s = s[:max_length]
    
    # Remove control characters
    s = ''.join(char for char in s if ord(char) >= 32 or char in '\n\r\t')
    
    # Remove potential XSS
    s = re.sub(r'<script[^>]*>.*?</script>', '', s, flags=re.IGNORECASE | re.DOTALL)
    s = re.sub(r'<[^>]+>', '', s)
    
    return s.strip()
