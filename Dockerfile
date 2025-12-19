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

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers
RUN playwright install chromium --with-deps || true

# Copy ALL application code
COPY . .

# CRITICAL: Verify templates directory exists and has files
RUN echo "========================================" && \
    echo "Verifying container contents:" && \
    echo "========================================" && \
    pwd && \
    echo "--- Root directory contents ---" && \
    ls -la && \
    echo "--- Templates directory check ---" && \
    if [ -d "templates" ]; then \
        echo "✅ templates/ directory exists"; \
        ls -la templates/; \
        if [ -f "templates/dashboard.html" ]; then \
            echo "✅ dashboard.html exists"; \
        else \
            echo "❌ dashboard.html NOT FOUND"; \
        fi; \
        if [ -f "templates/analytics.html" ]; then \
            echo "✅ analytics.html exists"; \
        else \
            echo "❌ analytics.html NOT FOUND"; \
        fi; \
    else \
        echo "❌ templates/ directory NOT FOUND"; \
    fi && \
    echo "========================================"

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8080

CMD ["python", "main.py"]
