package spanner

import (
	"cloud.google.com/go/spanner"
	"github.com/knocknote/gotx"
)

type config struct {
	timestampBoundEnabled bool
	timestampBound        spanner.TimestampBound
}

func newConfig() gotx.Config {
	baseConfig := gotx.NewDefaultConfig()
	baseConfig.VendorOption = &config{
		timestampBoundEnabled: false,
	}
	return baseConfig
}

type StaleRead spanner.TimestampBound

func (o StaleRead) Apply(c *gotx.Config) {
	c.VendorOption.(*config).timestampBoundEnabled = true
	c.VendorOption.(*config).timestampBound = spanner.TimestampBound(o)
}

func OptionStaleRead(bound spanner.TimestampBound) StaleRead {
	return StaleRead(bound)
}
