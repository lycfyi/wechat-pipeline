#!/bin/bash

# PostgreSQL Database Migration Script: Supabase → Zeabur
# This script performs a complete database migration using pg_dump/pg_restore

set -e  # Exit on any error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
DUMP_FILE="supabase_dump_$(date +%Y%m%d_%H%M%S).sql"
COMPRESSED_DUMP="supabase_dump_$(date +%Y%m%d_%H%M%S).sql.gz"

# Supabase connection (source)
SUPABASE_HOST="aws-0-us-east-2.pooler.supabase.com"
SUPABASE_PORT="5432"
SUPABASE_DB="postgres"
SUPABASE_USER="postgres.zuhyozglodhpezkeobvu"
SUPABASE_PASSWORD="brh7fhn8UCA0jvg*wxd"

# Zeabur connection (destination) - you'll need to fill these in
POSTGRES_HOST="sjc1.clusters.zeabur.com"
ZEABUR_HOST="${POSTGRES_HOST:-localhost}"
ZEABUR_PORT="${POSTGRES_PORT:-30929}"
ZEABUR_DB="${POSTGRES_DATABASE:-postgres}"
ZEABUR_USER="${POSTGRES_USERNAME:-root}"
ZEABUR_PASSWORD="${POSTGRES_PASSWORD:-gmu4K8wEY2efGP5k90il1VX7I3T6JLBh}"

echo -e "${BLUE}🚀 Starting PostgreSQL Database Migration${NC}"
echo -e "${BLUE}================================================${NC}"
echo "Source: Supabase PostgreSQL"
echo "Target: Zeabur PostgreSQL"
echo "Dump file: $COMPRESSED_DUMP"
echo ""

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check required tools
echo -e "${YELLOW}🔍 Checking required tools...${NC}"
if ! command_exists pg_dump; then
    echo -e "${RED}❌ pg_dump not found. Please install PostgreSQL client tools.${NC}"
    echo "macOS: brew install postgresql"
    echo "Ubuntu: sudo apt-get install postgresql-client"
    exit 1
fi

if ! command_exists pg_restore; then
    echo -e "${RED}❌ pg_restore not found. Please install PostgreSQL client tools.${NC}"
    exit 1
fi

echo -e "${GREEN}✅ All required tools found${NC}"
echo ""

# Step 1: Create schema dump from Supabase
echo -e "${YELLOW}📋 Step 1: Dumping schema from Supabase...${NC}"
PGPASSWORD="$SUPABASE_PASSWORD" pg_dump \
    --host="$SUPABASE_HOST" \
    --port="$SUPABASE_PORT" \
    --username="$SUPABASE_USER" \
    --dbname="$SUPABASE_DB" \
    --schema-only \
    --no-owner \
    --no-privileges \
    --clean \
    --if-exists \
    --file="schema_only.sql"

echo -e "${GREEN}✅ Schema dump completed${NC}"

# Step 2: Create data-only dump from Supabase (compressed)
echo -e "${YELLOW}📦 Step 2: Dumping data from Supabase (with compression)...${NC}"
echo "This may take several minutes for 500k rows..."

PGPASSWORD="$SUPABASE_PASSWORD" pg_dump \
    --host="$SUPABASE_HOST" \
    --port="$SUPABASE_PORT" \
    --username="$SUPABASE_USER" \
    --dbname="$SUPABASE_DB" \
    --data-only \
    --no-owner \
    --no-privileges \
    --disable-triggers \
    --table="chat_rooms" \
    --table="users" \
    --table="messages" \
    --compress=9 \
    --file="$COMPRESSED_DUMP"

echo -e "${GREEN}✅ Data dump completed and compressed${NC}"

# Show dump file size
if [[ -f "$COMPRESSED_DUMP" ]]; then
    DUMP_SIZE=$(du -h "$COMPRESSED_DUMP" | cut -f1)
    echo -e "${BLUE}📊 Compressed dump size: $DUMP_SIZE${NC}"
fi

# Step 3: Prepare Zeabur database
echo -e "${YELLOW}🎯 Step 3: Preparing Zeabur database...${NC}"

# Test Zeabur connection
echo "Testing Zeabur connection..."
PGPASSWORD="$ZEABUR_PASSWORD" psql \
    --host="$ZEABUR_HOST" \
    --port="$ZEABUR_PORT" \
    --username="$ZEABUR_USER" \
    --dbname="$ZEABUR_DB" \
    --command="SELECT version();" > /dev/null

echo -e "${GREEN}✅ Zeabur connection successful${NC}"

# Step 4: Apply schema to Zeabur
echo -e "${YELLOW}🏗️  Step 4: Creating schema on Zeabur...${NC}"
PGPASSWORD="$ZEABUR_PASSWORD" psql \
    --host="$ZEABUR_HOST" \
    --port="$ZEABUR_PORT" \
    --username="$ZEABUR_USER" \
    --dbname="$ZEABUR_DB" \
    --file="schema_only.sql"

echo -e "${GREEN}✅ Schema created on Zeabur${NC}"

# Step 5: Restore data to Zeabur
echo -e "${YELLOW}📥 Step 5: Restoring data to Zeabur...${NC}"
echo "This may take several minutes for 500k rows..."

# Decompress and restore
gunzip -c "$COMPRESSED_DUMP" | PGPASSWORD="$ZEABUR_PASSWORD" psql \
    --host="$ZEABUR_HOST" \
    --port="$ZEABUR_PORT" \
    --username="$ZEABUR_USER" \
    --dbname="$ZEABUR_DB" \
    --quiet

echo -e "${GREEN}✅ Data restoration completed${NC}"

# Step 6: Verification
echo -e "${YELLOW}🔍 Step 6: Verifying migration...${NC}"

# Count records in each table
echo "Counting records in Zeabur database..."

CHAT_ROOMS_COUNT=$(PGPASSWORD="$ZEABUR_PASSWORD" psql \
    --host="$ZEABUR_HOST" \
    --port="$ZEABUR_PORT" \
    --username="$ZEABUR_USER" \
    --dbname="$ZEABUR_DB" \
    --tuples-only \
    --command="SELECT COUNT(*) FROM chat_rooms;")

USERS_COUNT=$(PGPASSWORD="$ZEABUR_PASSWORD" psql \
    --host="$ZEABUR_HOST" \
    --port="$ZEABUR_PORT" \
    --username="$ZEABUR_USER" \
    --dbname="$ZEABUR_DB" \
    --tuples-only \
    --command="SELECT COUNT(*) FROM users;")

MESSAGES_COUNT=$(PGPASSWORD="$ZEABUR_PASSWORD" psql \
    --host="$ZEABUR_HOST" \
    --port="$ZEABUR_PORT" \
    --username="$ZEABUR_USER" \
    --dbname="$ZEABUR_DB" \
    --tuples-only \
    --command="SELECT COUNT(*) FROM messages;")

echo -e "${BLUE}📊 Migration Results:${NC}"
echo "   Chat Rooms: $(echo $CHAT_ROOMS_COUNT | xargs)"
echo "   Users: $(echo $USERS_COUNT | xargs)"
echo "   Messages: $(echo $MESSAGES_COUNT | xargs)"

# Step 7: Cleanup
echo -e "${YELLOW}🧹 Step 7: Cleaning up temporary files...${NC}"
read -p "Delete dump files? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm -f "schema_only.sql" "$COMPRESSED_DUMP"
    echo -e "${GREEN}✅ Temporary files cleaned up${NC}"
else
    echo -e "${BLUE}📁 Dump files preserved:${NC}"
    echo "   - schema_only.sql"
    echo "   - $COMPRESSED_DUMP"
fi

echo ""
echo -e "${GREEN}🎉 Migration completed successfully!${NC}"
echo -e "${BLUE}================================================${NC}"
echo "Next steps:"
echo "1. Update your application's DATABASE_URL to point to Zeabur"
echo "2. Test your application with the new database"
echo "3. Update any environment variables"
echo ""
