package tetralcleanup

import "database/sql"

func rowsAffected(result sql.Result) bool {
	if result == nil {
		return false
	}
	affected, err := result.RowsAffected()
	return err == nil && affected > 0
}
