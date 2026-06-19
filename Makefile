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

smoke-test:
	RUN_DOCKER_SMOKE_TESTS=1 pytest tests/test_load_balancer_smoke.py -v

restart-%:
	docker compose restart $*

build-%:
	docker compose build $*
