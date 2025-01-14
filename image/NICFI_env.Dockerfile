# Use an official Python base image
FROM python:3.8-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

# Set the working directory in the container
WORKDIR /app

# Install system dependencies and Python packages in one step
RUN apt-get update && apt-get install -y \
    gcc \
    libgeos-dev \
    && pip install --no-cache-dir \
    requests \
    geopandas \
    shapely \
    tqdm \
    && apt-get clean && rm -rf /var/lib/apt/lists/*


