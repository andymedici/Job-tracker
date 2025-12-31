#!/bin/bash
# =============================================================================
# Job Intelligence Platform - Upgrade Package Deployment Script
# =============================================================================
# Usage: ./deploy.sh [command]
# Commands: install, migrate, test, run, all
# =============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# =============================================================================
# INSTALLATION
# =============================================================================

install_dependencies() {
    log_info "Installing Python dependencies..."
    
    # Check for pip
    if ! command -v pip &> /dev/null; then
        log_error "pip is not installed. Please install Python and pip first."
        exit 1
    fi
    
    # Install requirements
    pip install -r requirements.txt --break-system-packages --quiet
    
    log_success "Dependencies installed successfully!"
}

# =============================================================================
# DATABASE MIGRATION
# =============================================================================

run_migrations() {
    log_info "Running database migrations..."
    
    # Check if DATABASE_URL is set
    if [ -z "$DATABASE_URL" ]; then
        log_warning "DATABASE_URL not set. Using default local connection."
    fi
    
    # Run schema additions
    python3 << 'EOF'
import os
import sys

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from sqlalchemy import create_engine, text
    
    database_url = os.getenv("DATABASE_URL", "postgresql://localhost:5432/job_intel")
    engine = create_engine(database_url)
    
    migrations = [
        # Seed company enhancements
        "ALTER TABLE seed_companies ADD COLUMN IF NOT EXISTS discovery_source VARCHAR(100)",
        "ALTER TABLE seed_companies ADD COLUMN IF NOT EXISTS discovery_confidence DECIMAL(3,2)",
        "ALTER TABLE seed_companies ADD COLUMN IF NOT EXISTS discovered_from VARCHAR(255)",
        
        # ATS predictions table
        """
        CREATE TABLE IF NOT EXISTS ats_predictions (
            id SERIAL PRIMARY KEY,
            company_name VARCHAR(255),
            predicted_ats VARCHAR(50),
            actual_ats VARCHAR(50),
            confidence DECIMAL(3,2),
            correct BOOLEAN,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        
        # Self-growth discoveries table
        """
        CREATE TABLE IF NOT EXISTS self_growth_discoveries (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            source_company VARCHAR(255),
            discovery_type VARCHAR(50),
            confidence DECIMAL(3,2),
            context TEXT,
            url VARCHAR(500),
            promoted_to_seed BOOLEAN DEFAULT FALSE,
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        
        # Discovery runs table
        """
        CREATE TABLE IF NOT EXISTS discovery_runs (
            id SERIAL PRIMARY KEY,
            run_type VARCHAR(50),
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            seeds_tested INTEGER,
            companies_found INTEGER,
            jobs_found INTEGER,
            errors INTEGER,
            ats_breakdown JSONB,
            notes TEXT
        )
        """,
        
        # Indexes
        "CREATE INDEX IF NOT EXISTS idx_seed_tier ON seed_companies(tier)",
        "CREATE INDEX IF NOT EXISTS idx_seed_source ON seed_companies(discovery_source)",
        "CREATE INDEX IF NOT EXISTS idx_seed_tested ON seed_companies(tested_at)",
        "CREATE INDEX IF NOT EXISTS idx_discovery_type ON self_growth_discoveries(discovery_type)",
        "CREATE INDEX IF NOT EXISTS idx_discovery_confidence ON self_growth_discoveries(confidence)",
    ]
    
    with engine.connect() as conn:
        for migration in migrations:
            try:
                conn.execute(text(migration))
                conn.commit()
            except Exception as e:
                if "already exists" not in str(e).lower():
                    print(f"Migration warning: {e}")
    
    print("Migrations completed successfully!")
    
except Exception as e:
    print(f"Migration error: {e}")
    sys.exit(1)
EOF
    
    log_success "Database migrations completed!"
}

# =============================================================================
# TESTING
# =============================================================================

run_tests() {
    log_info "Running tests..."
    
    if [ -d "tests" ]; then
        python3 -m pytest tests/ -v --tb=short
        log_success "All tests passed!"
    else
        log_warning "No tests directory found. Skipping tests."
    fi
}

# =============================================================================
# SEED EXPANSION
# =============================================================================

run_seed_expansion() {
    log_info "Running mega seed expansion (Tier 1 only for initial run)..."
    
    python3 mega_seed_expander.py --tiers 1 --output seeds_expanded.txt
    
    log_success "Seed expansion completed! Check seeds_expanded.txt for results."
}

# =============================================================================
# COLLECTOR RUN
# =============================================================================

run_collector() {
    log_info "Running V7 collector (test mode with 10 seeds)..."
    
    # Check if seeds file exists
    if [ -f "seeds_expanded.txt" ]; then
        python3 collector_v7.py --file seeds_expanded.txt --batch-size 10 --limit 10
    elif [ -f "seeds.txt" ]; then
        python3 collector_v7.py --file seeds.txt --batch-size 10 --limit 10
    else
        log_warning "No seeds file found. Running with test companies..."
        python3 collector_v7.py --test "OpenAI,Anthropic,Stripe,Notion,Figma"
    fi
    
    log_success "Collector run completed!"
}

# =============================================================================
# SELF-GROWTH
# =============================================================================

run_self_growth() {
    log_info "Running self-growth intelligence..."
    
    python3 self_growth_intelligence.py --run --limit 100
    
    log_success "Self-growth analysis completed!"
}

# =============================================================================
# LOG SETUP
# =============================================================================

setup_logs() {
    log_info "Setting up log directories..."
    
    mkdir -p logs
    touch logs/collector.log
    touch logs/expander.log
    touch logs/self_growth.log
    touch logs/errors.log
    
    log_success "Log directories created!"
}

# =============================================================================
# HEALTH CHECK
# =============================================================================

health_check() {
    log_info "Running health check..."
    
    python3 << 'EOF'
import sys

checks = []

# Check imports
try:
    import collector_v7
    checks.append(("collector_v7", True))
except ImportError as e:
    checks.append(("collector_v7", False))

try:
    import mega_seed_expander
    checks.append(("mega_seed_expander", True))
except ImportError as e:
    checks.append(("mega_seed_expander", False))

try:
    import self_growth_intelligence
    checks.append(("self_growth_intelligence", True))
except ImportError as e:
    checks.append(("self_growth_intelligence", False))

try:
    import integration
    checks.append(("integration", True))
except ImportError as e:
    checks.append(("integration", False))

try:
    import config
    checks.append(("config", True))
except ImportError as e:
    checks.append(("config", False))

# Print results
print("\nModule Health Check:")
print("-" * 40)
all_passed = True
for module, status in checks:
    status_str = "✓ OK" if status else "✗ FAILED"
    print(f"  {module}: {status_str}")
    if not status:
        all_passed = False

print("-" * 40)
if all_passed:
    print("All modules loaded successfully!")
else:
    print("Some modules failed to load. Check errors above.")
    sys.exit(1)
EOF
    
    log_success "Health check completed!"
}

# =============================================================================
# FULL DEPLOYMENT
# =============================================================================

deploy_all() {
    log_info "Starting full deployment..."
    echo ""
    
    setup_logs
    echo ""
    
    install_dependencies
    echo ""
    
    run_migrations
    echo ""
    
    health_check
    echo ""
    
    run_tests
    echo ""
    
    log_success "Full deployment completed!"
    echo ""
    echo "Next steps:"
    echo "  1. Run seed expansion: ./deploy.sh expand"
    echo "  2. Run collector: ./deploy.sh collect"
    echo "  3. Run self-growth: ./deploy.sh growth"
}

# =============================================================================
# USAGE
# =============================================================================

show_usage() {
    echo "Job Intelligence Platform - Upgrade Deployment Script"
    echo ""
    echo "Usage: ./deploy.sh [command]"
    echo ""
    echo "Commands:"
    echo "  install    Install Python dependencies"
    echo "  migrate    Run database migrations"
    echo "  test       Run test suite"
    echo "  expand     Run seed expansion (Tier 1)"
    echo "  collect    Run V7 collector"
    echo "  growth     Run self-growth intelligence"
    echo "  health     Run health check"
    echo "  logs       Setup log directories"
    echo "  all        Full deployment (install + migrate + test)"
    echo ""
    echo "Examples:"
    echo "  ./deploy.sh all        # Full deployment"
    echo "  ./deploy.sh expand     # Just expand seeds"
    echo "  ./deploy.sh collect    # Just run collector"
}

# =============================================================================
# MAIN
# =============================================================================

case "${1:-help}" in
    install)
        install_dependencies
        ;;
    migrate)
        run_migrations
        ;;
    test)
        run_tests
        ;;
    expand)
        run_seed_expansion
        ;;
    collect)
        run_collector
        ;;
    growth)
        run_self_growth
        ;;
    health)
        health_check
        ;;
    logs)
        setup_logs
        ;;
    all)
        deploy_all
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        log_error "Unknown command: $1"
        echo ""
        show_usage
        exit 1
        ;;
esac
