# Stage 1: Build environment
FROM python:3.8-slim AS builder

# Set environment variables for non-interactive installation
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get install -y default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the application code
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: Production environment
FROM python:3.8-slim AS production

# Set environment variables for non-interactive installation
ENV DEBIAN_FRONTEND=noninteractive

# Install only necessary JDK for production if needed (optional)
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the application code
COPY --from=builder /usr/local/lib/python3.8/site-packages /usr/local/lib/python3.8/site-packages
COPY --from=builder /app /app

# Expose the Flask app port
EXPOSE 5000

# Set environment variables
ENV FLASK_APP=app_react.py
ENV FLASK_ENV=production

# Run the Flask app
CMD ["python", "-m", "flask", "run", "--host=0.0.0.0"]