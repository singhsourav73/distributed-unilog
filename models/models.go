package models

import (
	"time"

	"github.com/google/uuid"
)

type LogEvent struct {
	ID             string    `json:"id"`
	OrganizationID string    `json:"organizationId"`
	Level          string    `json:"level"`
	Message        string    `json:"message"`
	Source         string    `json:"source"`
	Timestamp      time.Time `json:"timestamp"`
}

func NewLogEvent(orgID, level, msg, source string) LogEvent {
	return LogEvent{
		ID:             uuid.New().String(),
		OrganizationID: orgID,
		Level:          level,
		Message:        msg,
		Source:         source,
		Timestamp:      time.Now(),
	}
}
