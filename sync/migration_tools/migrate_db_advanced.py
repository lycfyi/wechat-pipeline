#!/usr/bin/env python3
"""
Advanced PostgreSQL Database Migration: Supabase → Zeabur
Alternative Python-based approach with more control and monitoring
"""

import asyncio
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import asyncpg


class DatabaseMigrator:
    def __init__(self):
        # Supabase connection (source)
        self.supabase_config = {
            'host': 'aws-0-us-east-2.pooler.supabase.com',
            'port': 5432,
            'database': 'postgres',
            'user': 'postgres.zuhyozglodhpezkeobvu',
            'password': 'brh7fhn8UCA0jvg*wxd',
        }

        # Zeabur connection (destination)
        self.zeabur_config = {
            'host': os.getenv('POSTGRES_HOST', 'sjc1.clusters.zeabur.com'),
            'port': int(os.getenv('POSTGRES_PORT', 30929)),
            'database': os.getenv('POSTGRES_DATABASE', 'postgres'),
            'user': os.getenv('POSTGRES_USERNAME', 'root'),
            'password': os.getenv('POSTGRES_PASSWORD', 'gmu4K8wEY2efGP5k90il1VX7I3T6JLBh'),
        }

        self.dump_dir = Path('migration_dumps')
        self.dump_dir.mkdir(exist_ok=True)

        self.stats = {
            'start_time': None,
            'dump_time': None,
            'restore_time': None,
            'total_time': None,
            'dump_size': 0,
            'records_migrated': {},
        }

    def log(self, message: str, level: str = "INFO"):
        """Enhanced logging with timestamps"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        colors = {
            "INFO": "\033[0;34m",  # Blue
            "SUCCESS": "\033[0;32m",  # Green
            "WARNING": "\033[1;33m",  # Yellow
            "ERROR": "\033[0;31m",  # Red
            "RESET": "\033[0m",
        }

        color = colors.get(level, colors["INFO"])
        reset = colors["RESET"]
        print(f"{color}[{timestamp}] {level}: {message}{reset}")

    async def test_connections(self) -> bool:
        """Test both source and destination connections"""
        self.log("Testing database connections...")

        try:
            # Test Supabase connection
            self.log("Testing Supabase connection...")
            supabase_conn = await asyncpg.connect(**self.supabase_config)
            version = await supabase_conn.fetchval("SELECT version()")
            await supabase_conn.close()
            self.log(f"✅ Supabase connected: {version[:50]}...", "SUCCESS")

            # Test Zeabur connection
            self.log("Testing Zeabur connection...")
            zeabur_conn = await asyncpg.connect(**self.zeabur_config)
            version = await zeabur_conn.fetchval("SELECT version()")
            await zeabur_conn.close()
            self.log(f"✅ Zeabur connected: {version[:50]}...", "SUCCESS")

            return True

        except Exception as e:
            self.log(f"❌ Connection test failed: {e}", "ERROR")
            return False

    async def get_table_counts(self, config: Dict) -> Dict[str, int]:
        """Get record counts from source database"""
        self.log("Getting table record counts...")

        conn = await asyncpg.connect(**config)
        try:
            counts = {}
            tables = ['chat_rooms', 'users', 'messages']

            for table in tables:
                try:
                    count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                    counts[table] = count
                    self.log(f"  {table}: {count:,} records")
                except Exception as e:
                    self.log(f"  {table}: Error - {e}", "WARNING")
                    counts[table] = 0

            return counts

        finally:
            await conn.close()

    def create_dump_command(self) -> list:
        """Create pg_dump command with optimal settings"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dump_file = self.dump_dir / f"supabase_migration_{timestamp}.sql"

        cmd = [
            'pg_dump',
            f'--host={self.supabase_config["host"]}',
            f'--port={self.supabase_config["port"]}',
            f'--username={self.supabase_config["user"]}',
            f'--dbname={self.supabase_config["database"]}',
            '--no-owner',
            '--no-privileges',
            '--disable-triggers',
            '--data-only',  # Only data, schema will be handled by Prisma
            '--inserts',  # Use INSERT statements for better compatibility
            '--table=chat_rooms',
            '--table=users',
            '--table=messages',
            f'--file={dump_file}',
        ]

        self.dump_file = dump_file
        return cmd

    def create_restore_command(self) -> list:
        """Create psql restore command"""
        cmd = [
            'psql',
            f'--host={self.zeabur_config["host"]}',
            f'--port={self.zeabur_config["port"]}',
            f'--username={self.zeabur_config["user"]}',
            f'--dbname={self.zeabur_config["database"]}',
            '--quiet',
            f'--file={self.dump_file}',
        ]

        return cmd

    async def run_dump(self) -> bool:
        """Execute pg_dump with progress monitoring"""
        self.log("🚀 Starting database dump from Supabase...")
        self.stats['start_time'] = datetime.now()

        dump_cmd = self.create_dump_command()

        # Set password environment variable
        env = os.environ.copy()
        env['PGPASSWORD'] = self.supabase_config['password']

        try:
            # Run pg_dump
            self.log(f"Executing: {' '.join(dump_cmd[:8])}... (password hidden)")
            process = subprocess.Popen(dump_cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            # Monitor progress (pg_dump doesn't provide progress, so we monitor file size)
            while process.poll() is None:
                if self.dump_file.exists():
                    size = self.dump_file.stat().st_size
                    size_mb = size / (1024 * 1024)
                    self.log(f"Dump in progress... {size_mb:.1f} MB written")
                await asyncio.sleep(5)

            stdout, stderr = process.communicate()

            if process.returncode == 0:
                self.stats['dump_time'] = datetime.now()
                self.stats['dump_size'] = self.dump_file.stat().st_size
                size_mb = self.stats['dump_size'] / (1024 * 1024)
                self.log(f"✅ Dump completed successfully! Size: {size_mb:.1f} MB", "SUCCESS")
                return True
            else:
                self.log(f"❌ Dump failed: {stderr}", "ERROR")
                return False

        except Exception as e:
            self.log(f"❌ Dump error: {e}", "ERROR")
            return False

    async def prepare_zeabur_schema(self) -> bool:
        """Ensure Zeabur database has the correct schema"""
        self.log("🏗️  Preparing Zeabur database schema...")

        try:
            # Run Prisma migrations to ensure schema is up to date
            self.log("Running Prisma migrations...")
            result = subprocess.run(
                ['npx', 'prisma', 'migrate', 'deploy'],
                cwd=Path.cwd(),
                capture_output=True,
                text=True,
                env={**os.environ, 'DATABASE_URL': self.get_zeabur_connection_string()},
            )

            if result.returncode == 0:
                self.log("✅ Schema prepared successfully", "SUCCESS")
                return True
            else:
                self.log(f"❌ Schema preparation failed: {result.stderr}", "ERROR")
                return False

        except Exception as e:
            self.log(f"❌ Schema preparation error: {e}", "ERROR")
            return False

    def get_zeabur_connection_string(self) -> str:
        """Generate Zeabur connection string"""
        config = self.zeabur_config
        return (
            f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
        )

    async def run_restore(self) -> bool:
        """Execute data restore to Zeabur"""
        self.log("📥 Starting data restore to Zeabur...")

        restore_cmd = self.create_restore_command()

        # Set password environment variable
        env = os.environ.copy()
        env['PGPASSWORD'] = self.zeabur_config['password']

        try:
            self.log(f"Executing: {' '.join(restore_cmd[:6])}... (password hidden)")
            process = subprocess.Popen(restore_cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            stdout, stderr = process.communicate()

            if process.returncode == 0:
                self.stats['restore_time'] = datetime.now()
                self.log("✅ Restore completed successfully!", "SUCCESS")
                return True
            else:
                self.log(f"❌ Restore failed: {stderr}", "ERROR")
                return False

        except Exception as e:
            self.log(f"❌ Restore error: {e}", "ERROR")
            return False

    async def verify_migration(self) -> bool:
        """Verify migration by comparing record counts"""
        self.log("🔍 Verifying migration...")

        try:
            # Get counts from both databases
            source_counts = await self.get_table_counts(self.supabase_config)
            dest_counts = await self.get_table_counts(self.zeabur_config)

            self.log("📊 Migration Verification Results:")
            all_match = True

            for table in ['chat_rooms', 'users', 'messages']:
                source_count = source_counts.get(table, 0)
                dest_count = dest_counts.get(table, 0)

                if source_count == dest_count:
                    self.log(f"  ✅ {table}: {dest_count:,} records (matches source)", "SUCCESS")
                else:
                    self.log(f"  ❌ {table}: {dest_count:,} records (source: {source_count:,})", "ERROR")
                    all_match = False

                self.stats['records_migrated'][table] = dest_count

            return all_match

        except Exception as e:
            self.log(f"❌ Verification error: {e}", "ERROR")
            return False

    def print_final_summary(self):
        """Print final migration summary"""
        self.stats['total_time'] = datetime.now() - self.stats['start_time']

        self.log("🎉 Migration Summary", "SUCCESS")
        self.log("=" * 50)
        self.log(f"Total time: {self.stats['total_time']}")
        self.log(f"Dump size: {self.stats['dump_size'] / (1024*1024):.1f} MB")

        total_records = sum(self.stats['records_migrated'].values())
        self.log(f"Total records migrated: {total_records:,}")

        for table, count in self.stats['records_migrated'].items():
            self.log(f"  {table}: {count:,}")

        if self.stats['total_time'].total_seconds() > 0:
            rate = total_records / self.stats['total_time'].total_seconds()
            self.log(f"Migration rate: {rate:.0f} records/second")

    async def migrate(self) -> bool:
        """Execute complete migration process"""
        self.log("🚀 Starting PostgreSQL Database Migration: Supabase → Zeabur")
        self.log("=" * 60)

        # Step 1: Test connections
        if not await self.test_connections():
            return False

        # Step 2: Get source record counts
        source_counts = await self.get_table_counts(self.supabase_config)
        total_records = sum(source_counts.values())
        self.log(f"📊 Total records to migrate: {total_records:,}")

        # Step 3: Prepare destination schema
        if not await self.prepare_zeabur_schema():
            return False

        # Step 4: Create dump
        if not await self.run_dump():
            return False

        # Step 5: Restore data
        if not await self.run_restore():
            return False

        # Step 6: Verify migration
        if not await self.verify_migration():
            self.log("⚠️  Migration completed but verification failed", "WARNING")

        # Step 7: Final summary
        self.print_final_summary()

        return True


async def main():
    """Main migration function"""
    migrator = DatabaseMigrator()

    try:
        success = await migrator.migrate()
        if success:
            print("\n🎉 Migration completed successfully!")
            print("\nNext steps:")
            print("1. Update your .env file to use Zeabur connection string")
            print("2. Test your application with the new database")
            print("3. Update any deployment configurations")
        else:
            print("\n❌ Migration failed. Check logs above for details.")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n⚠️  Migration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Check dependencies
    try:
        import asyncpg
    except ImportError:
        print("❌ asyncpg not installed. Run: pip install asyncpg")
        sys.exit(1)

    asyncio.run(main())
