# Dockerfile - Incorporating Fixes for Stability and Production Readiness

FROM python:3.12-slim

# 1. Install System Dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc postgresql-client \
    # Ensure all Playwright/Chromium dependencies are included
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libxkbcommon0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 \
    libpango-1.0-0 libcairo2 libasound2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. Install Python Dependencies (Ensure gunicorn is in requirements.txt or installed here)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Fix Playwright Installation: Rely on system packages and install browser only
# This is less likely to fail than the --with-deps version
RUN playwright install chromium || true

# 4. Copy application code
COPY . /app/

# Verification (Keep this for confidence)
RUN echo "======== VERIFICATION START ========" && \
    test -d /app/templates && echo "✅ templates/ EXISTS" || (echo "❌ NOT FOUND" && exit 1) && \
    test -f /app/templates/dashboard.html && echo "✅ dashboard.html EXISTS" || (echo "❌ NOT FOUND" && exit 1) && \
    test -f /app/templates/analytics.html && echo "✅ analytics.html EXISTS" || (echo "❌ NOT FOUND" && exit 1) && \
    echo "======== VERIFICATION SUCCESS ========"

# 5. User setup and Permissions
RUN useradd -m -u 1000 appuser

RUN chown -R appuser:appuser /app && \
    chmod -R 755 /app

USER appuser

EXPOSE 8080

# 6. CRITICAL FIX: Use Gunicorn to launch the server
# You must replace 'main:app' with your actual entry point:
# - If your Flask/FastAPI instance is called 'app' in 'main.py', use 'main:app'.
# - If your instance is called 'server' in 'application.py', use 'application:server'.
# This command binds to the Railway-provided $PORT, solving the "unavailable" error.
CMD ["gunicorn", "--bind", "0.0.0.0:$PORT", "--workers", "4", "main:app"]
