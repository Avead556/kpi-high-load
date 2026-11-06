#!/bin/sh
cd "$(dirname "$0")/.."

echo "$ docker compose ps db"
docker compose ps db --format 'table {{.Name}}\t{{.Status}}\t{{.Ports}}'

echo "$ docker compose port db 5432"
docker compose port db 5432 2>&1 || true

echo "$ docker network inspect vaccine-lots_backend --format 'internal={{.Internal}}'"
docker network inspect vaccine-lots_backend --format 'internal={{.Internal}}'

echo "$ python3 -c 'socket.create_connection((\"127.0.0.1\", 5432))'   (from the host)"
python3 -c 'import socket
try:
    socket.create_connection(("127.0.0.1", 5432), timeout=2)
    print("connected")
except OSError as e:
    print("host -> 127.0.0.1:5432:", type(e).__name__, e)'

DB_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' vaccine-lots-db-1)
echo "$ python3 -c 'socket.create_connection((\"$DB_IP\", 5432))'   (container IP, from the host)"
python3 -c "import socket
try:
    socket.create_connection(('$DB_IP', 5432), timeout=2)
    print('connected')
except OSError as e:
    print('host -> $DB_IP:5432:', type(e).__name__, e)"

echo "$ docker compose exec web python -c 'asyncpg.connect(host=\"db\") ... SELECT version()'"
docker compose exec -T web python -c 'import asyncio, os, asyncpg
async def main():
    conn = await asyncpg.connect(host="db", user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"], database=os.environ["DB_NAME"])
    print("web -> db:5432:", await conn.fetchval("SELECT version()"))
    print("lots in table:", await conn.fetchval("SELECT count(*) FROM vaccine_lots"))
    await conn.close()
asyncio.run(main())'

echo "$ docker compose exec db wget -q -T 3 -O- http://example.com   (db has no route outside)"
docker compose exec -T db wget -q -T 3 -O- http://example.com 2>&1 | head -1 || true
