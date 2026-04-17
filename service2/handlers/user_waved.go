package handlers

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

type UserWavedHandler struct {
	db *sql.DB
}

func NewUserWavedHandler(db *sql.DB) *UserWavedHandler {
	return &UserWavedHandler{db: db}
}

func (h *UserWavedHandler) Handle(ctx context.Context, msg *sarama.ConsumerMessage) error {
	log.Printf("user_waved: %s", string(msg.Value))

	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx,
		`INSERT INTO statistics (slug, value) VALUES ('user_waved', 1)
                                 ON CONFLICT (slug) DO UPDATE SET value = statistics.value + 1`)
	if err != nil {
		return fmt.Errorf("update stats: %w", err)
	}

	return tx.Commit()
}
