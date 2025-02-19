package database

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/id"
)

const userColumns = "mxid, uin, management_room, space_room"

type UserQuery struct {
	db  *Database
	log zerolog.Logger
}

func (uq *UserQuery) New() *User {
	return &User{
		db:  uq.db,
		log: uq.log,

		lastReadCache: make(map[PortalKey]time.Time),
		inSpaceCache:  make(map[PortalKey]bool),
	}
}

func (uq *UserQuery) GetAll(ctx context.Context) []*User {
	users := []*User{}

	query := fmt.Sprintf("SELECT %s FROM \"user\"", userColumns)

	rows, err := uq.db.Query(ctx, query)
	if err != nil || rows == nil {
		return users
	}

	defer rows.Close()
	for rows.Next() {
		users = append(users, uq.New().Scan(rows))
	}

	return users
}

func (uq *UserQuery) GetByMXID(ctx context.Context, userID id.UserID) *User {
	query := fmt.Sprintf("SELECT %s FROM \"user\" WHERE mxid=$1", userColumns)
	args := []interface{}{userID}

	row := uq.db.QueryRow(ctx, query, args...)
	if row == nil {
		return nil
	}
	return uq.New().Scan(row)
}

func (uq *UserQuery) GetByUin(ctx context.Context, uin string) *User {
	query := fmt.Sprintf("SELECT %s FROM \"user\" WHERE uin=$1", userColumns)
	args := []interface{}{uin}

	row := uq.db.QueryRow(ctx, query, args...)
	if row == nil {
		return nil
	}

	return uq.New().Scan(row)
}
