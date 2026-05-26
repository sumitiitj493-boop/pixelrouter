.PHONY: build up down logs ps clean test

build:
	docker compose build --no-cache

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

clean:
	docker compose down -v
	docker system prune -f

test:
	pytest tests/ -v

restart-%:
	docker compose restart $*

build-%:
	docker compose build $*
