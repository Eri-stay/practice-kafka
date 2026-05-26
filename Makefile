DB_URL="postgres://postgres:postgres@192.168.0.178:5433/email_db?sslmode=disable"

migrate-up:
	migrate -path services/email-sender/db/migrations -database $(DB_URL) up

migrate-down:
	migrate -path services/email-sender/db/migrations -database $(DB_URL) down