package duck

import "testing"

func TestWithCatalogConnectTimeout(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"url no params", "postgres://u:p@h:5432/db", "postgres://u:p@h:5432/db?connect_timeout=10"},
		{"url with params", "postgres://u:p@h/db?sslmode=require", "postgres://u:p@h/db?sslmode=require&connect_timeout=10"},
		{"postgresql scheme", "postgresql://h/db", "postgresql://h/db?connect_timeout=10"},
		{"keyword", "host=h dbname=db user=u", "host=h dbname=db user=u connect_timeout=10"},
		{"url already set", "postgres://h/db?connect_timeout=3", "postgres://h/db?connect_timeout=3"},
		{"keyword already set", "host=h dbname=db connect_timeout=3", "host=h dbname=db connect_timeout=3"},
		{"file catalog untouched", "/var/lib/dq/catalog.db", "/var/lib/dq/catalog.db"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := withCatalogConnectTimeout(c.in); got != c.want {
				t.Errorf("withCatalogConnectTimeout(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}
