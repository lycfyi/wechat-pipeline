# Database Migration Tools

This folder contains tools for migrating data from Supabase to Zeabur PostgreSQL.

## 📁 Files Overview

### Migration Scripts

- **`migrate_db_dump.sh`** - Shell script for PostgreSQL dump/restore migration (recommended for simplicity)
- **`migrate_db_advanced.py`** - Python script with advanced monitoring and error handling
- **`test_zeabur_connection.py`** - Connection testing utility for various Zeabur patterns

### Configuration & Documentation

- **`zeabur_env_template.txt`** - Environment variables template for Zeabur connection
- **`zeabur_troubleshooting.md`** - Comprehensive troubleshooting guide
- **`README.md`** - This file

## 🚀 Quick Start

### Prerequisites

```bash
# Install PostgreSQL client tools
brew install postgresql  # macOS
# or
sudo apt-get install postgresql-client  # Ubuntu

# Install Python dependencies (for Python script)
pip install asyncpg
```

### Step 1: Configure Zeabur Connection

1. Enable **Port Forwarding** in your Zeabur dashboard
2. Update connection details in scripts:
   - Host: `sjc1.clusters.zeabur.com`
   - Port: `38929` (from Zeabur dashboard)
   - Database: `postgres` or `zeabur`
   - User: `root`
   - Password: `gmu4K8wEY2efGP5k90il1VX7I3T6JLBh`

### Step 2: Test Connection

```bash
cd migration_tools
python test_zeabur_connection.py
```

### Step 3: Run Migration

#### Option A: Shell Script (Simple)

```bash
./migrate_db_dump.sh
```

#### Option B: Python Script (Advanced)

```bash
python migrate_db_advanced.py
```

## 📊 Expected Performance

- **500k rows**: ~2-5 minutes total migration time
- **Compression**: ~70-80% size reduction
- **Verification**: Automatic record count matching

## 🔧 Configuration Updates Needed

Before running migration, update these files with correct Zeabur connection details:

### migrate_db_dump.sh

```bash
ZEABUR_HOST="sjc1.clusters.zeabur.com"
ZEABUR_PORT="38929"  # Update from 5432
ZEABUR_DB="postgres"  # Confirm database name
```

### migrate_db_advanced.py

```python
self.zeabur_config = {
    'host': 'sjc1.clusters.zeabur.com',
    'port': 38929,  # Update from 5432
    'database': 'postgres',  # Confirm database name
    'user': 'root',
    'password': 'gmu4K8wEY2efGP5k90il1VX7I3T6JLBh',
}
```

## 📋 Migration Process

1. **Test Connections** - Verify both Supabase and Zeabur connectivity
2. **Schema Preparation** - Ensure Zeabur has correct schema (via Prisma)
3. **Data Dump** - Export data from Supabase with compression
4. **Data Restore** - Import data to Zeabur in batches
5. **Verification** - Compare record counts between databases
6. **Cleanup** - Remove temporary dump files

## 🆘 Troubleshooting

If you encounter connection issues:

1. Check `zeabur_troubleshooting.md` for detailed solutions
2. Verify Port Forwarding is enabled in Zeabur dashboard
3. Confirm connection details match your Zeabur service
4. Test with `test_zeabur_connection.py`

## 🔄 Post-Migration

After successful migration:

1. Update your application's `DATABASE_URL` to point to Zeabur
2. Test your application with the new database
3. Update deployment configurations
4. Consider backing up the migration dump files
