package service

import (
	daemonadmin "github.com/myceldb/mycel/internal/identity/service/admin"
	daemonuser "github.com/myceldb/mycel/internal/identity/service/user"
)

type UserRaftStateMachine = daemonuser.RaftStateMachine
type AdminRaftStateMachine = daemonadmin.RaftStateMachine
