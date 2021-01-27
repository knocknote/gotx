package gotx

type Config struct {
	ReadOnly     bool
	RollbackOnly bool
	VendorOption interface{}
}

func NewDefaultConfig() Config {
	return Config{
		ReadOnly:     false,
		RollbackOnly: false,
	}
}

type Option interface {
	Apply(*Config)
}

// read only transaction
type ReadOnly bool

func (o ReadOnly) Apply(c *Config) {
	c.ReadOnly = bool(o)
}

func OptionReadOnly() ReadOnly {
	return true
}

// rollback only transaction
type RollbackOnly bool

func (o RollbackOnly) Apply(c *Config) {
	c.RollbackOnly = bool(o)
}

func OptionRollbackOnly() RollbackOnly {
	return true
}
