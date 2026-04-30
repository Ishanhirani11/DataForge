#!/bin/bash
set -e

echo "=== DataForge Deployment ==="

# Check if .env exists
if [ ! -f .env ]; then
    echo "Error: .env file not found. Please create it from .env template."
    exit 1
fi

# Build and start services
echo "Building Docker images..."
docker-compose build

echo "Starting services..."
docker-compose up -d

echo "Waiting for services to be healthy..."
sleep 10

# Check service health
echo "Checking service status..."
docker-compose ps

echo ""
echo "=== Deployment Complete ==="
echo "Services are running. Check logs with: docker-compose logs -f"
echo "Stop services with: docker-compose down"
