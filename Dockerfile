# Use official Python runtime as base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py .
COPY index.html .

# Cloud Run uses PORT environment variable
ENV PORT=8080

# Run with gunicorn (production WSGI server)
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app:app
