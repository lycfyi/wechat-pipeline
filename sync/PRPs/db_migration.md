Goal:
Migrate everything from Supabase to Zeabur.
there are about half million rows in total.
help me figure out the most efficient way to migrate from supabase to zeabur postgres

# From: Supabase Postgres connection

## Connect to Supabase via connection pooling

DATABASE_URL="postgresql://postgres.zuhyozglodhpezkeobvu:brh7fhn8UCA0jvg\*wxd@aws-0-us-east-2.pooler.supabase.com:6543/postgres?pgbouncer=true"

## Direct connection to the database. Used for migrations

DIRECT_URL="postgresql://postgres.zuhyozglodhpezkeobvu:brh7fhn8UCA0jvg\*wxd@aws-0-us-east-2.pooler.supabase.com:5432/postgres"

# To: Zeabur wechat-sync postgres

POSTGRES_PORT=${DATABASE_PORT}
POSTGRES_DATABASE=${POSTGRES_DB}
POSTGRES_USER=root
POSTGRES_USERNAME=${POSTGRES_USER}
POSTGRES_CONNECTION_STRING=postgresql://${POSTGRES_USERNAME}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DATABASE}
POSTGRES_HOST=${CONTAINER_HOSTNAME}
POSTGRES_PASSWORD=${PASSWORD}
POSTGRES_DB=zeabur
POSTGRES_URI=${POSTGRES_CONNECTION_STRING}
PASSWORD=gmu4K8wEY2efGP5k90il1VX7I3T6JLBh
