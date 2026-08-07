package user

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const StoreFilename = "users.json"

var ErrDuplicateUser = errors.New("user already exists")
var ErrStoreNotFound = errors.New("user store not found")
var ErrUserNotFound = errors.New("user not found")
var ErrInvalidCredentials = errors.New("invalid user credentials")
var ErrInvalidRefreshToken = errors.New("invalid refresh token")

type Store interface {
	List(context.Context) ([]User, error)
	Create(context.Context, User) error
	GetByID(ctx context.Context, userID string) (User, error)
	Find(ctx context.Context, username string) (User, error)
	Update(ctx context.Context, userID string, update func(*User) error) (User, error)
	UpdatePasswordHash(ctx context.Context, userID string, passwordHash string) (User, error)
	ApplyPut(ctx context.Context, user User) (User, error)
}

type FileStore struct {
	path string
	mu   sync.Mutex
}

type storeDocument struct {
	Users []User `json:"users"`
}

func OpenStore(dir string) (*FileStore, bool, error) {
	path := filepath.Join(dir, StoreFilename)
	if _, err := os.Stat(path); err == nil {
		return &FileStore{path: path}, false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, false, fmt.Errorf("stat user store: %w", err)
	}
	store := &FileStore{path: path}
	if err := store.write(storeDocument{Users: []User{}}); err != nil {
		return nil, false, err
	}
	return store, true, nil
}

func OpenExistingStore(dir string) (*FileStore, error) {
	path := filepath.Join(dir, StoreFilename)
	if _, err := os.Stat(path); err == nil {
		return &FileStore{path: path}, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return nil, ErrStoreNotFound
	} else {
		return nil, fmt.Errorf("stat user store: %w", err)
	}
}

func (s *FileStore) List(ctx context.Context) ([]User, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read()
	if err != nil {
		return nil, err
	}
	users := make([]User, 0, len(doc.Users))
	for _, user := range doc.Users {
		users = append(users, user.normalized())
	}
	sort.Slice(users, func(i, j int) bool { return strings.ToLower(users[i].Username) < strings.ToLower(users[j].Username) })
	return users, nil
}

func (s *FileStore) GetByID(ctx context.Context, userID string) (User, error) {
	users, err := s.List(ctx)
	if err != nil {
		return User{}, err
	}
	for _, user := range users {
		if user.ID == userID {
			return user.normalized(), nil
		}
	}
	return User{}, ErrUserNotFound
}

func (s *FileStore) Find(ctx context.Context, username string) (User, error) {
	users, err := s.List(ctx)
	if err != nil {
		return User{}, err
	}
	username = strings.TrimSpace(username)
	for _, user := range users {
		if username != "" && strings.EqualFold(user.Username, username) {
			return user.normalized(), nil
		}
	}
	return User{}, ErrUserNotFound
}

func (s *FileStore) Create(ctx context.Context, user User) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read()
	if err != nil {
		return err
	}
	for _, existing := range doc.Users {
		if strings.EqualFold(existing.Username, user.Username) || existing.ID == user.ID {
			return ErrDuplicateUser
		}
	}
	doc.Users = append(doc.Users, user.normalized())
	return s.write(doc)
}

func (s *FileStore) Update(ctx context.Context, userID string, update func(*User) error) (User, error) {
	if err := ctx.Err(); err != nil {
		return User{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read()
	if err != nil {
		return User{}, err
	}
	for i := range doc.Users {
		if doc.Users[i].ID != userID {
			continue
		}
		user := doc.Users[i].normalized()
		if err := update(&user); err != nil {
			return User{}, err
		}
		user.UpdatedAt = time.Now().UTC()
		doc.Users[i] = user.normalized()
		return doc.Users[i], s.write(doc)
	}
	return User{}, ErrUserNotFound
}

func (s *FileStore) UpdatePasswordHash(ctx context.Context, userID string, passwordHash string) (User, error) {
	return s.Update(ctx, userID, func(user *User) error { user.PasswordHash = passwordHash; return nil })
}

func (s *FileStore) ApplyPut(ctx context.Context, user User) (User, error) {
	if err := ctx.Err(); err != nil {
		return User{}, err
	}
	user = user.normalized()
	if strings.TrimSpace(user.ID) == "" || strings.TrimSpace(user.Username) == "" {
		return User{}, ErrUserNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, err := s.read()
	if err != nil {
		return User{}, err
	}
	for i, existing := range doc.Users {
		if existing.ID == user.ID {
			doc.Users[i] = user
			return user, s.write(doc)
		}
		if strings.EqualFold(existing.Username, user.Username) {
			// ApplyPut replays authoritative WAL/Raft state. If a local store already
			// contains the username under a different ID, converge to the replayed
			// record instead of failing startup with ErrDuplicateUser.
			doc.Users[i] = user
			return user, s.write(doc)
		}
	}
	doc.Users = append(doc.Users, user)
	return user, s.write(doc)
}

func (s *FileStore) read() (storeDocument, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return storeDocument{}, fmt.Errorf("read user store: %w", err)
	}
	var doc storeDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return storeDocument{}, fmt.Errorf("decode user store: %w", err)
	}
	if doc.Users == nil {
		doc.Users = []User{}
	}
	for i := range doc.Users {
		doc.Users[i] = doc.Users[i].normalized()
	}
	return doc, nil
}

func (s *FileStore) write(doc storeDocument) error {
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("encode user store: %w", err)
	}
	data = append(data, '\n')
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write user store: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace user store: %w", err)
	}
	return nil
}
