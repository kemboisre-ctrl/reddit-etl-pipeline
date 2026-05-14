#!/bin/bash
# =============================================================================
# SETUP SCRIPT FOR REDDIT ETL PIPELINE (TaskFlow + LocalExecutor)
# =============================================================================


set -e

echo "Creating project directory structure..."

mkdir -p dags src/extract src/transform src/load tests notebooks infrastructure/terraform docs logs plugins scripts

touch src/__init__.py
touch src/extract/__init__.py
touch src/transform/__init__.py
touch src/load/__init__.py
touch tests/__init__.py

if [ ! -f .env ]; then
    cp .env.example .env
    echo ""
    echo "Created .env from template. PLEASE EDIT IT with your real credentials."
else
    echo ""
    echo ".env already exists, skipping."
fi

echo ""
echo "Setup complete! Next steps:"
echo "  1. Edit .env with your Reddit and AWS credentials"
echo "  2. Run: docker-compose build"
echo "  3. Run: docker-compose up airflow-init    (first time only)"
echo "  4. Run: docker-compose up -d"
echo "  5. Open http://localhost:8080 (admin / admin)"
echo ""
echo "To trigger the pipeline:"
echo "  - In Airflow UI, toggle 'reddit_etl_pipeline' ON"
echo "  - Click the play button to trigger a manual run"