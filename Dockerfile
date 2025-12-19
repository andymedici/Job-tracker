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

# 2. Install Python Dependencies (Ensure gunicorn is in requirements.txt!)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Fix Playwright Installation
RUN playwright install chromium || true

# 4. Copy application code
COPY . /app/

# 5. User setup and Permissions
RUN useradd -m -u 1000 appuser
RUN chown -R appuser:appuser /app && \
    chmod -R 755 /app

USER appuser

EXPOSE 8080

# 6. CRITICAL FIX: Use Gunicorn to launch the server
# Target is confirmed as 'main:app'
CMD ["gunicorn", "--bind", "0.0.0.0:$PORT", "--workers", "4", "main:app"]
