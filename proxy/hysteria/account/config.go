package account

import (
	"sync"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

func (a *Account) AsAccount() (protocol.Account, error) {
	var VR net.Port
	if id, err := uuid.Parse(a.Auth); err == nil {
		VR = net.PortFromBytes(id[6:8])
	}
	return &MemoryAccount{
		Auth: a.Auth,
		VR:   VR,
	}, nil
}

type MemoryAccount struct {
	Auth string
	VR   net.Port
}

func (a *MemoryAccount) Equals(another protocol.Account) bool {
	if account, ok := another.(*MemoryAccount); ok {
		return a.Auth == account.Auth
	}
	return false
}

func (a *MemoryAccount) ToProto() proto.Message {
	return &Account{
		Auth: a.Auth,
	}
}

type Validator struct {
	emails map[string]struct{}
	users  map[string]*protocol.MemoryUser
	ids    map[uuid.UUID]*protocol.MemoryUser

	mutex sync.Mutex
}

func NewValidator() *Validator {
	return &Validator{
		emails: make(map[string]struct{}),
		users:  make(map[string]*protocol.MemoryUser),
		ids:    make(map[uuid.UUID]*protocol.MemoryUser),
	}
}

func (v *Validator) Add(u *protocol.MemoryUser) error {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	if u.Email != "" {
		if _, ok := v.emails[u.Email]; ok {
			return errors.New("User ", u.Email, " already exists.")
		}
		v.emails[u.Email] = struct{}{}
	}
	auth := u.Account.(*MemoryAccount).Auth
	v.users[auth] = u
	if id, err := uuid.Parse(auth); err == nil {
		id[6] = 0
		id[7] = 0
		v.ids[id] = u
	}

	return nil
}

func (v *Validator) DelByEmail(email string) error {
	if email == "" {
		return errors.New("Email must not be empty.")
	}

	v.mutex.Lock()
	defer v.mutex.Unlock()

	if _, ok := v.emails[email]; !ok {
		return errors.New("User ", email, " not found.")
	}
	delete(v.emails, email)
	for key, user := range v.users {
		if user.Email == email {
			delete(v.users, key)
			if id, err := uuid.Parse(key); err == nil {
				id[6] = 0
				id[7] = 0
				delete(v.ids, id)
			}
			break
		}
	}

	return nil
}

func (v *Validator) Get(auth string) *protocol.MemoryUser {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	if id, err := uuid.Parse(auth); err == nil {
		if user := v.getID(id); user != nil {
			VR := net.PortFromBytes(id[6:8])
			if user.Account.(*MemoryAccount).VR != VR {
				return &protocol.MemoryUser{
					Email: user.Email,
					Level: user.Level,
					Account: &MemoryAccount{
						Auth: auth,
						VR:   VR,
					},
				}
			}
			return user
		}
	}
	return v.users[auth]
}

func (v *Validator) getID(id uuid.UUID) *protocol.MemoryUser {
	id[6] = 0
	id[7] = 0
	return v.ids[id]
}

func (v *Validator) GetByEmail(email string) *protocol.MemoryUser {
	if email == "" {
		return nil
	}

	v.mutex.Lock()
	defer v.mutex.Unlock()

	if _, ok := v.emails[email]; ok {
		for _, user := range v.users {
			if user.Email == email {
				return user
			}
		}
	}

	return nil
}

func (v *Validator) GetAll() []*protocol.MemoryUser {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	users := make([]*protocol.MemoryUser, 0, len(v.users))
	for _, user := range v.users {
		users = append(users, user)
	}

	return users
}

func (v *Validator) GetCount() int64 {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	return int64(len(v.users))
}

func (v *Validator) NotEmpty() bool {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	return len(v.users) > 0
}
