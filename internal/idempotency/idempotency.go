package idempotency

import "github.com/google/uuid"

// Key 是透传给接收方的幂等键。
type Key string

// KeyGenerator 在 ingest 时生成幂等键。
// 生产者带 producerKey 时返回确定性的 "{tenantID}:{producerKey}"，否则用 NewID 铸一个随机键。
type KeyGenerator struct {
	NewID func() string
}

// NewKeyGenerator 返回以 uuid v4 作为随机键来源的生成器。
func NewKeyGenerator() *KeyGenerator {
	return &KeyGenerator{NewID: uuid.NewString}
}

func (g *KeyGenerator) Generate(tenantID, producerKey string) Key {
	if producerKey != "" {
		return Key(tenantID + ":" + producerKey)
	}
	return Key(g.NewID())
}
