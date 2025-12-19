FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    gcc postgresql-client \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libxkbcommon0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 \
    libpango-1.0-0 libcairo2 libasound2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium --with-deps || true

COPY . /app/

# Verification
RUN echo "======== VERIFICATION START ========" && \
    test -d /app/templates && echo "✅ templates/ EXISTS" || (echo "❌ NOT FOUND" && exit 1) && \
    test -f /app/templates/dashboard.html && echo "✅ dashboard.html EXISTS" || (echo "❌ NOT FOUND" && exit 1) && \
    test -f /app/templates/analytics.html && echo "✅ analytics.html EXISTS" || (echo "❌ NOT FOUND" && exit 1) && \
    wc -c /app/templates/dashboard.html && \
    wc -c /app/templates/analytics.html && \
    echo "======== VERIFICATION SUCCESS ========"

# Create user and fix permissions BEFORE switching
RUN useradd -m -u 1000 appuser

# CRITICAL: Set permissions so appuser can read templates
RUN chown -R appuser:appuser /app && \
    chmod -R 755 /app && \
    chmod -R 755 /app/templates && \
    chmod 644 /app/templates/*.html

# Verify appuser can read templates
RUN su appuser -c "ls -la /app/templates" && \
    su appuser -c "cat /app/templates/dashboard.html | head -5" && \
    echo "✅ appuser CAN read templates"

USER appuser

EXPOSE 8080
CMD ["python", "main.py"]
