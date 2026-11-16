package tenant

import (
	"context"
	"errors"
	"time"
)

// Tenant 是一个租户的投递配置。
type Tenant struct {
	ID             string
	Destination    string
	MaxConcurrency int
	RetryWindow    time.Duration // 投递窗口，超窗进 DLQ（默认 24h）
}

// Registry 提供租户配置查询。
type Registry interface {
	Get(ctx context.Context, id string) (Tenant, error)
}

// ErrNotFound 表示租户不存在。
var ErrNotFound = errors.New("tenant not found")

// Static 是 map 支撑的静态实现（M2 用；可替换为 Redis/DB）。
type Static struct {
	tenants map[string]Tenant
}

func NewStatic(tenants ...Tenant) *Static {
	m := make(map[string]Tenant, len(tenants))
	for _, t := range tenants {
		m[t.ID] = t
	}
	return &Static{tenants: m}
}

func (s *Static) Get(ctx context.Context, id string) (Tenant, error) {
	t, ok := s.tenants[id]
	if !ok {
		return Tenant{}, ErrNotFound
	}
	return t, nil
}
