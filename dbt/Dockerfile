FROM python:3.11-slim

# Avoid Python creating .pyc files & enable unbuffered logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /usr/app

# Install system deps for dbt-postgres
RUN apt-get update && apt-get install -y --no-install-recommends \
    git gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install dbt-postgres
RUN pip install --no-cache-dir dbt-postgres==1.7.11

# Copy dbt project files
COPY dbt_project.yml ./dbt_project.yml
COPY profiles.yml ./profiles.yml
COPY models ./models
