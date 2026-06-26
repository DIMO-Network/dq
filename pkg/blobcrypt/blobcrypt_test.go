package blobcrypt

import (
	"crypto/rand"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testKey(t *testing.T) string {
	t.Helper()
	k := make([]byte, 32)
	_, err := rand.Read(k)
	require.NoError(t, err)
	return base64.StdEncoding.EncodeToString(k)
}

func TestCipher_RoundTrip(t *testing.T) {
	c, err := NewCipher(testKey(t))
	require.NoError(t, err)
	require.NotNil(t, c)
	const key = "cloudevent/blobs/sub/2026/06/26/uuid"
	plaintext := []byte("sensitive vehicle payload \x00\x01\x02")
	sealed, err := c.Seal(key, plaintext)
	require.NoError(t, err)
	assert.True(t, IsSealed(sealed))
	got, err := c.Open(key, sealed)
	require.NoError(t, err)
	assert.Equal(t, plaintext, got)
}

func TestCipher_AADBindsToKey(t *testing.T) {
	c, _ := NewCipher(testKey(t))
	sealed, err := c.Seal("key-A", []byte("secret"))
	require.NoError(t, err)
	_, err = c.Open("key-B", sealed)
	require.Error(t, err, "ciphertext bound to one object key must not open under another")
}

func TestCipher_PlaintextPassthrough(t *testing.T) {
	c, _ := NewCipher(testKey(t))
	legacy := []byte("raw legacy payload written before the key existed")
	got, err := c.Open("k", legacy)
	require.NoError(t, err, "an unsealed (no-magic) blob is returned as-is")
	assert.Equal(t, legacy, got)
}

func TestNewCipher_Validation(t *testing.T) {
	c, err := NewCipher("")
	require.NoError(t, err)
	assert.Nil(t, c, "empty key → nil cipher (encryption off)")

	_, err = NewCipher("not!base64!")
	require.Error(t, err)

	_, err = NewCipher(base64.StdEncoding.EncodeToString(make([]byte, 16)))
	require.Error(t, err, "AES-128 key (16 bytes) must be rejected")
}

// TestOpensDinGolden proves dq decrypts a blob sealed by din's
// internal/blobcrypt: the cross-repo wire-format contract. This exact vector is
// pinned in din's blobcrypt_test too — if either repo's format drifts, that
// repo's golden test fails. Key is 32 bytes of 0x2a; see the test that produced it.
func TestOpensDinGolden(t *testing.T) {
	const (
		keyB64    = "KioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKio="
		sealedB64 = "REJFMfpbx2YHkkRYAdtRPzuxzz7dwfQJNizKGPt2rTfyuQxLDF656mDN5H8zlpCmikO3wJcKpg=="
		aad       = "cloudevent/blobs/golden"
		want      = "din<->dq blob format v1"
	)
	c, err := NewCipher(keyB64)
	require.NoError(t, err)
	sealed, err := base64.StdEncoding.DecodeString(sealedB64)
	require.NoError(t, err)
	got, err := c.Open(aad, sealed)
	require.NoError(t, err)
	assert.Equal(t, want, string(got), "dq must decrypt din-sealed blobs")
}
