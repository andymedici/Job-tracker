FROM python:3.12-slim

# 1. Install System Dependencies (Cached, OK)
RUN apt-get update && apt-get install -y --no-install-recommends \
    # ... system dependencies ...
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. Install Python Dependencies
COPY requirements.txt .
# THIS LAYER MUST BE REBUILT! Pushing the changed requirements.txt handles this.
RUN pip install --no-cache-dir -r requirements.txt

# 3. Playwright Installation (Cached, OK)
RUN playwright install chromium || true

# 4. Copy application code (New code from main.py fix)
COPY . /app/

# 5. User setup and Permissions (Cached, OK)
RUN useradd -m -u 1000 appuser
RUN chown -R appuser:appuser /app && \
    chmod -R 755 /app

USER appuser

EXPOSE 8080

# 6. CRITICAL: Start Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:$PORT", "--workers", "4", "main:app"]
