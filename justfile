build:
    docker compose build

test:
    docker compose run --rm test

lint:
    docker compose run --rm lint

run:
    docker compose up api