package main

import (
	"database/sql"
	"log"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Пример: Migrations как отдельный бинарник
// Запускается один раз при деплое или через CI/CD
func main() {
	db, err := sql.Open("pgx", "postgres://user:secret@0.0.0.0:5432/service_2?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Пример: можно использовать библиотеку для миграций (например, golang-migrate, goose)
	// или просто выполнять SQL файлы
	if len(os.Args) < 2 {
		log.Fatal("Usage: migrate [up|down]")
	}

	command := os.Args[1]
	switch command {
	case "up":
		runMigrationsUp(db)
	case "down":
		runMigrationsDown(db)
	default:
		log.Fatalf("Unknown command: %s", command)
	}

	log.Println("Migrations completed")
}

func runMigrationsUp(db *sql.DB) {
	// Пример миграции
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS statistics (
			id SERIAL PRIMARY KEY,
			slug VARCHAR(255) UNIQUE NOT NULL,
			value INTEGER DEFAULT 0
		);
	`)
	if err != nil {
		log.Fatal("Migration failed:", err)
	}
	log.Println("Migrations up completed")
}

func runMigrationsDown(db *sql.DB) {
	_, err := db.Exec(`DROP TABLE IF EXISTS statistics;`)
	if err != nil {
		log.Fatal("Migration rollback failed:", err)
	}
	log.Println("Migrations down completed")
}
