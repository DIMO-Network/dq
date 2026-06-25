package config

import "testing"

func TestSettingsValidate(t *testing.T) {
	valid := Settings{Port: 8080, GRPCPort: 8086, MonPort: 8888, DIMORegistryChainID: 137}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid settings rejected: %v", err)
	}

	cases := []struct {
		name string
		mut  func(*Settings)
	}{
		{"zero PORT", func(s *Settings) { s.Port = 0 }},
		{"negative PORT", func(s *Settings) { s.Port = -1 }},
		{"zero GRPC_PORT", func(s *Settings) { s.GRPCPort = 0 }},
		{"zero MON_PORT", func(s *Settings) { s.MonPort = 0 }},
		{"zero CHAIN_ID", func(s *Settings) { s.DIMORegistryChainID = 0 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := valid
			c.mut(&s)
			if err := s.Validate(); err == nil {
				t.Errorf("%s: expected a validation error, got nil", c.name)
			}
		})
	}
}
