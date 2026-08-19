COMPOSE := docker compose
COMPOSE_CACHE := $(COMPOSE) -f docker-compose.yml -f docker-compose.cache.yml

.PHONY: up up-build up-cache up-build-cache down test

# App only — cache off (CACHE_ENABLED from .env, default false)
up:
	$(COMPOSE) up

up-build:
	$(COMPOSE) up --build

# App + Redis — cache on
up-cache:
	$(COMPOSE_CACHE) up

up-build-cache:
	$(COMPOSE_CACHE) up --build

down:
	$(COMPOSE) down

test:
	$(COMPOSE) run --rm tests
