package duck

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestAttachDuckLakeSQL_Encrypted pins the gate: only a writable attach (the
// materializer) carries ENCRYPTED, so it can't create a plaintext catalog din
// rejects; a read-only query pod omits it and reads the existing encrypted
// catalog transparently (avoiding any READ_ONLY+ENCRYPTED interaction).
func TestAttachDuckLakeSQL_Encrypted(t *testing.T) {
	writer := attachDuckLakeSQL(Config{DataPath: "s3://b/lake/", Encrypted: true})
	assert.Contains(t, writer, "ENCRYPTED", "writer attach must be encrypted")
	assert.NotContains(t, writer, "READ_ONLY")

	reader := attachDuckLakeSQL(Config{DataPath: "s3://b/lake/", Encrypted: true, ReadOnly: true})
	assert.NotContains(t, reader, "ENCRYPTED", "read-only pod must not pass ENCRYPTED")
	assert.Contains(t, reader, "READ_ONLY")

	off := attachDuckLakeSQL(Config{DataPath: "s3://b/lake/"})
	assert.NotContains(t, off, "ENCRYPTED", "disabled → no ENCRYPTED")
}
