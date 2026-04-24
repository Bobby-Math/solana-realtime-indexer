#!/bin/bash
# Run migrations on Docker TimescaleDB container

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

CONTAINER_NAME="solana_timescaledb"
DB_USER="solana"
DB_NAME="solana_pipeline"
MIGRATIONS_DIR="$PROJECT_ROOT/migrations"

echo "🔄 Running migrations on TimescaleDB..."

# Check if container is running
if ! docker ps --filter "name=$CONTAINER_NAME" --format "{{.Names}}" | grep -q "^$CONTAINER_NAME$"; then
    echo "❌ Container $CONTAINER_NAME is not running"
    echo "   Start it with: cd $PROJECT_ROOT && docker compose up -d"
    exit 1
fi

# Run migrations in order
for migration in $(ls $MIGRATIONS_DIR/*.sql | sort); do
    echo "📄 Running: $(basename $migration)"
    docker exec -i $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME < "$migration"
done

echo "✅ Migrations completed successfully!"

# Verify the materialized view was created
echo ""
echo "🔍 Verifying slot_latency_1m materialized view..."
docker exec $CONTAINER_NAME psql -U $DB_USER -d $DB_NAME -c "\d+ slot_latency_1m"

echo ""
echo "✅ Setup complete! You can now run: cargo run"
echo ""
echo "💡 To stop the database later: cd $PROJECT_ROOT && docker compose down"
