# Use a slim Python image for a smaller final container size

FROM python:3.12-slim



# 1. Install System Dependencies (CRITICAL FIX)

# These are necessary for:

# - 'gcc' and 'postgresql-client' for psycopg2-binary to compile/run

# - All other libraries for Playwright/Chromium to run headless

RUN apt-get update && apt-get install -y --no-install-recommends \

    gcc \

    postgresql-client \

    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \

    libcups2 libdrm2 libxkbcommon0 libxcomposite1 \

    libxdamage1 libxfixes3 libxrandr2 libgbm1 \

    libpango-1.0-0 libcairo2 libasound2 \

    && rm -rf /var/lib/apt/lists/*



# Set the working directory inside the container

WORKDIR /app



# 2. Install Python Dependencies

# Copy requirements.txt first to leverage Docker layer caching

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt



# 3. Install Playwright browser

# Install Chromium, needed for scraping, using the Python package utility

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

# 'main:app' assumes your Flask application object is named 'app' in 'main.py'

CMD ["gunicorn", "--bind", "0.0.0.0:$PORT", "--workers", "4", "main:app"]
