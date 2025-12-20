# Use a slim Python image for a smaller final container size
FROM python:3.12-slim

# 1. Install System Dependencies (CRITICAL FIX)
# The dependency list has been updated for recent Debian versions (like Trixie).
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    postgresql-client \
    libnss3 libnspr4 libcups2 libdrm2 libxkbcommon0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2 \
    libatk1.0-0 libatk-bridge2.0-0 \
    libgdk-pixbuf-xlib-2.0-0 \
    libpango-1.0-0 libcairo2 libgtk-3-0 \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /app

# 2. Install Python Dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. User Setup and Permissions (PRE-INSTALL)
# Create a dedicated non-root user for security
RUN useradd -m -u 1000 appuser

# Change ownership of the app directory to the new user
RUN chown -R appuser:appuser /app && \
    chmod -R 755 /app

# Switch to the non-root user for all subsequent commands
USER appuser

# 4. Install Playwright browser (CRUCIAL MOVE)
RUN playwright install chromium || true

# 5. Copy the rest of the application code
COPY --chown=appuser:appuser . /app/

# Expose the default port
EXPOSE 8080

# 6. Start the Application Server
CMD gunicorn --bind 0.0.0.0:${PORT:-8080} --workers 4 main:app
