package dbconnect

import (
	"context"
	"database/sql/driver"

	"github.com/jackc/pgx/v5/stdlib"

	"github.com/tetral-ai/tetral/internal/transportsecurity"
)

// A transaction keeps its driver connection until database/sql returns it. The
// validator rejects a retired generation at that return boundary, before it
// can become an idle connection or serve another operation. Embedding preserves
// the selected pgx driver's actual query/exec/ping/value interfaces.
type protectedConnector struct {
	driver.Connector
	owner *transportsecurity.Owner
}

func (c protectedConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	native, ok := conn.(*stdlib.Conn)
	if !ok {
		_ = conn.Close()
		return nil, driver.ErrBadConn
	}
	return &protectedConnection{Conn: native, owner: c.owner}, nil
}

type protectedConnection struct {
	*stdlib.Conn
	owner *transportsecurity.Owner
}

func (c *protectedConnection) IsValid() bool {
	return !c.Conn.Conn().IsClosed() && c.owner.IsCurrentTLSConfig(c.Conn.Conn().Config().TLSConfig)
}
