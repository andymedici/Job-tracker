# Use a slim Python image for a smaller final container size
FROM python:3.12-slim

# 1. Install System Dependencies (CRITICAL FIX)
# These are necessary for:
# - 'gcc' and 'postgresql-client' for psycopg2-binary to compile/run
# - All necessary libraries for Playwright/Chromium to run headless (including GTK)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    postgresql-client \
    libnss3 libnspr4 libcups2 libdrm2 libxkbcommon0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2 \
    libatk1.0-0 libatk-bridge2.0-0 libgdk-pixbuf2.0-0 \
    libpango-1.0-0 libcairo2 libgtk-3-0 \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /app

# 2. Install Python Dependencies (Run as root initially to use pip)
# Copy requirements.txt first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. User Setup and Permissions (PRE-INSTALL)
# Create a dedicated non-root user for security
RUN useradd -m -u 1000 appuser

# Change ownership of the app directory to the new user
RUN chown -R appuser:appuser /app && \
    chmod -R 755 /app

# Switch to the non-root user for all subsequent commands
# This ensures Playwright's cache path is correctly set.
USER appuser

# 4. Install Playwright browser (CRUCIAL MOVE)
# This will now install the browser into /home/appuser/.cache/..., 
# where the application will look for it at runtime.
RUN playwright install chromium || true

# 5. Copy the rest of the application code
# Use the --chown flag to ensure the appuser owns the copied files
COPY --chown=appuser:appuser . /app/

# Expose the default port
EXPOSE 8080

# 6. Start the Application Server
# Gunicorn runs as the appuser
CMD gunicorn --bind 0.0.0.0:${PORT:-8080} --workers 4 main:app
