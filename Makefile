DB_URL="postgres://postgres:postgres@10.151.6.210:5433/email_db?sslmode=disable"

migrate-up:
	migrate -path db/migrations -database $(DB_URL) up

migrate-down:
	migrate -path db/migrations -database $(DB_URL) down