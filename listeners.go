package godatabaseversioner

import (
	"database/sql"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Listener that won't do anything on events
type NoOpListener struct {
}

func (l NoOpListener) On(_ Event) error {
	return nil
}

// Listener that will log events using zerolog logger
type ZerologListener struct {
	// Logger to use, with level at which logger should log (eg. log.Debug())
	Logger *zerolog.Event
}

// EventBroadcastListener will handle and forward events to multiple sub listeners
type EventBroadcastListener struct {
	Listeners []Listener
}

func (l EventBroadcastListener) On(event Event) error {
	for _, listener := range l.Listeners {
		if err := listener.On(event); nil != err {
			return err
		}
	}
	return nil
}

// NewZerologListener that will log events using zerolog logger at debug level
func NewZerologListener() *ZerologListener {
	return &ZerologListener{log.Debug()}
}

func (l ZerologListener) On(event Event) error {
	switch event.Type {
	case EventBeforeSync:
		l.Logger.Msg("starting syncing process")
	case EventBeforeChange:
		l.Logger.Int("version", event.Version.Number()).Msg("applying version")
	case EventAfterChange:
		l.Logger.Int("version", event.Version.Number()).Msg("version applied")
	case EventAfterSync:
		l.Logger.Msg("end of syncing process")
	}
	return nil
}

// TransactionalChangesListener will open and commit a transaction during each version application
type TransactionalChangesListener struct {
	DB                 *sql.DB
	currentTransaction *sql.Tx
}

func (l TransactionalChangesListener) On(event Event) error {
	var err error
	switch event.Type {
	case EventBeforeChange:
		l.currentTransaction, err = l.DB.Begin()
		if err != nil {
			return err
		}
	case EventAfterChange:
		err = l.currentTransaction.Commit()
		l.currentTransaction = nil
		if err != nil {
			return err
		}
	case EventErrorDuringChange:
		err = l.currentTransaction.Rollback()
		l.currentTransaction = nil
		if err != nil {
			return err
		}
	}
	return nil
}
