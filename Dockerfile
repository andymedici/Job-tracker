# Use a slim Python image for a smaller final container size
FROM python:3.12-slim

# 1. Install System Dependencies (CRITICAL FIX)
# These are necessary for:
# - 'gcc' and 'postgresql-client' for psycopg2-binary to compile/run
# - All other libraries for Playwright/Chromium to run headless
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    postgresql-client \
    # Existing Playwright dependencies
    libnss3 libnspr4 libcups2 libdrm2 libxkbcommon0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 \
    libasound2 \
    # 🔑 NEW/FIXED: GTK dependencies for Playwright/Chromium rendering
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libgdk-pixbuf2.0-0 \
    libpango-1.0-0 \
    libcairo2 \
    libgtk-3-0 \
    # Clean up APT lists
    && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /app

# 2. Install Python Dependencies
# Copy requirements.txt first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Install Playwright browser
# Install Chromium, needed for scraping, using the Python package utility
# The '|| true' handles scenarios where installation warnings/non-zero exits occur.
RUN playwright install chromium || true

# 4. Copy the rest of the application code
COPY . /app/

# 5. User Setup and Permissions
# Create a dedicated non-root user for security
RUN useradd -m -u 1000 appuser
# Change ownership of the app directory to the new user
RUN chown -R appuser:appuser /app && \
    chmod -R 755 /app

# Switch to the non-root user
USER appuser

# Expose the default port, though Gunicorn will bind to $PORT
EXPOSE 8080

# 6. Start the Application Server
# Use Gunicorn as the production WSGI server, binding to the port provided by the platform
# It is important that Playwright processes run under the non-root 'appuser'
CMD gunicorn --bind 0.0.0.0:${PORT:-8080} --workers 4 main:app
