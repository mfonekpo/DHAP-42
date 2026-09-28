.PHONY: up down config network

up: network
	docker compose --env-file .env -f containers/airflow/docker-compose.yaml up --build -d
	docker compose --env-file .env -f containers/minio/docker-compose.yaml up --build -d

down:
	docker compose --env-file .env -f containers/airflow/docker-compose.yaml down
	docker compose --env-file .env -f containers/minio/docker-compose.yaml down

config:
	docker compose --env-file .env -f containers/airflow/docker-compose.yaml config --quiet
	docker compose --env-file .env -f containers/minio/docker-compose.yaml config --quiet

network:
	@docker network inspect dhap42 >/dev/null 2>&1 || docker network create dhap42