#!/usr/bin/env sh
# Runs E1-E3 inside the compose network (E4 needs the host's docker CLI: npm run bench:e4).
set -e
docker compose build -q
docker compose up -d postgres
docker compose run --rm bench node dist/bench/e1-throughput.js
docker compose run --rm -e SYNC_COMMIT=off bench node dist/bench/e1-throughput.js
docker compose run --rm bench node dist/bench/e2-claim-strategies.js
docker compose run --rm -e SYNC_COMMIT=off bench node dist/bench/e2-claim-strategies.js
docker compose run --rm bench node dist/bench/e3-wakeup.js
