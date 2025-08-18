package mqttmgr

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

type Manager interface {
	Add(ctx context.Context, cfg ConnConfig) error
	Remove(ctx context.Context, name string) error
	List() []string
	Publish(ctx context.Context, name string, req PublishReq) error
	Subscribe(ctx context.Context, name string, req SubReq) error
	Unsubscribe(ctx context.Context, name string, filters ...string) error
	Shutdown(ctx context.Context) error
}

type mgr struct {
	mu      sync.RWMutex
	clients map[string]*Client
	logger  *slog.Logger
}

func New(logger *slog.Logger) Manager {
	if logger == nil {
		logger = slog.Default()
	}

	return &mgr{
		clients: make(map[string]*Client),
		logger:  logger,
	}
}

func (m *mgr) Add(ctx context.Context, cfg ConnConfig) error {
	if cfg.Name == "" {
		return errors.New("ConnConfig.Name is required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.clients[cfg.Name]; exists {
		return fmt.Errorf("connection %q already exists", cfg.Name)
	}

	c, err := newV5Client(ctx, cfg, m.logger.With("conn", cfg.Name))
	if err != nil {
		return err
	}

	m.clients[cfg.Name] = c
	return nil
}

func (m *mgr) Remove(ctx context.Context, name string) error {
	m.mu.Lock()
	c := m.clients[name]
	if c == nil {
		m.mu.Unlock()
		return fmt.Errorf("unknown connection %q", name)
	}
	delete(m.clients, name)
	m.mu.Unlock()
	return c.Close(ctx)
}

func (m *mgr) List() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.clients))
	for k := range m.clients {
		names = append(names, k)
	}
	return names
}

func (m *mgr) Publish(ctx context.Context, name string, req PublishReq) error {
	m.mu.RLock()
	c := m.clients[name]
	m.mu.RUnlock()

	if c == nil {
		return fmt.Errorf("unknown connection %q", name)
	}
	return c.Publish(ctx, req)
}

func (m *mgr) Subscribe(ctx context.Context, name string, req SubReq) error {
	m.mu.RLock()
	c := m.clients[name]
	m.mu.RUnlock()

	if c == nil {
		return fmt.Errorf("unknown connection %q", name)
	}

	return c.Subscribe(ctx, req)

}

func (m *mgr) Unsubscribe(ctx context.Context, name string, filters ...string) error {
	m.mu.RLock()
	c := m.clients[name]
	m.mu.RUnlock()

	if c == nil {
		return fmt.Errorf("unknown connection %q", name)
	}
	return c.Unsubscribe(ctx, filters...)
}

func (m *mgr) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var firstErr error
	for name, c := range m.clients {
		if err := c.Close(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("%s: %w", name, err)
		}
	}
	m.clients = map[string]*Client{}
	return firstErr
}
