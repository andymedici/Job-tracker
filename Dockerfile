FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libxkbcommon0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 \
    libpango-1.0-0 libcairo2 libasound2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium --with-deps || true

# Copy ALL source code
COPY . /app/

# CRITICAL VERIFICATION - Will fail build if templates missing
RUN echo "======== VERIFICATION START ========" && \
    echo "Contents of /app:" && \
    ls -la /app && \
    echo "" && \
    echo "Checking for templates directory..." && \
    test -d /app/templates && echo "✅ templates/ EXISTS" || (echo "❌ templates/ NOT FOUND" && exit 1) && \
    echo "" && \
    echo "Contents of /app/templates:" && \
    ls -la /app/templates && \
    echo "" && \
    echo "Checking for dashboard.html..." && \
    test -f /app/templates/dashboard.html && echo "✅ dashboard.html EXISTS" || (echo "❌ dashboard.html NOT FOUND" && exit 1) && \
    echo "" && \
    echo "Checking for analytics.html..." && \
    test -f /app/templates/analytics.html && echo "✅ analytics.html EXISTS" || (echo "❌ analytics.html NOT FOUND" && exit 1) && \
    echo "" && \
    echo "Dashboard.html size:" && \
    wc -c /app/templates/dashboard.html && \
    echo "Analytics.html size:" && \
    wc -c /app/templates/analytics.html && \
    echo "======== VERIFICATION SUCCESS ========"

# Set permissions
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app && \
    chmod -R 755 /app/templates

USER appuser

EXPOSE 8080

CMD ["python", "main.py"]
