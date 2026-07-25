package dbconnect

import (
	"database/sql"
	"errors"
)

func IsNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}
