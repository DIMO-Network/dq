// Package blobcrypt seals/opens externalized cloudevent blob payloads with
// AES-256-GCM. din seals payloads before uploading them to the blob bucket; dq
// (this copy) opens them on the read path. The Parquet lake gets at-rest
// protection from DuckLake ENCRYPTED, but the >1MB blob payloads aren't in the
// lake, so without this they'd have only S3 SSE — readable by anything holding an
// S3 GET credential. The key lives in the pod secret (BLOB_ENCRYPTION_KEY), never
// in the bucket, so bucket access alone yields ciphertext.
//
// Wire format: magic("DBE1") || nonce(12) || ciphertext+tag. This MUST stay
// byte-identical to din's internal/blobcrypt — the two are a cross-repo contract.
// A blob without the magic prefix is treated as legacy plaintext and returned
// as-is, so a reader with a key set still reads blobs written before the key.
package blobcrypt

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
)

// magic prefixes every sealed blob. Wire constant — must match din.
var magic = []byte("DBE1")

// ErrDecrypt indicates a sealed blob failed to open: a wrong/rotated key, a
// truncated blob, or corruption. It is DETERMINISTIC (retrying with the same key
// fails identically), so callers classify it as a poison payload to skip rather
// than a transient error to retry. Match it with errors.Is / IsDecryptError.
var ErrDecrypt = errors.New("blobcrypt: decrypt failed")

// IsDecryptError reports whether err is (or wraps) ErrDecrypt.
func IsDecryptError(err error) bool {
	return errors.Is(err, ErrDecrypt)
}

// Cipher seals and opens blob payloads with one AES-256 key.
type Cipher struct{ aead cipher.AEAD }

// NewCipher builds a Cipher from a base64-encoded 32-byte key. An empty key
// returns (nil, nil) so callers treat "no key" as "no encryption" without a
// special case; a malformed key is an error (fail fast at startup).
func NewCipher(b64Key string) (*Cipher, error) {
	if b64Key == "" {
		return nil, nil
	}
	key, err := base64.StdEncoding.DecodeString(b64Key)
	if err != nil {
		return nil, fmt.Errorf("blobcrypt: BLOB_ENCRYPTION_KEY is not valid base64: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("blobcrypt: BLOB_ENCRYPTION_KEY must decode to 32 bytes (AES-256), got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("blobcrypt: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("blobcrypt: %w", err)
	}
	return &Cipher{aead: aead}, nil
}

// Seal returns magic || nonce || ciphertext+tag. aad (the object key) binds the
// ciphertext to its path. Present mainly for tests/symmetry — din does the
// production sealing.
func (c *Cipher) Seal(aad string, plaintext []byte) ([]byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("blobcrypt: nonce: %w", err)
	}
	out := make([]byte, 0, len(magic)+len(nonce)+len(plaintext)+c.aead.Overhead())
	out = append(out, magic...)
	out = append(out, nonce...)
	return c.aead.Seal(out, nonce, plaintext, []byte(aad)), nil
}

// Open decrypts a sealed blob. A blob without the magic prefix is returned
// unchanged (legacy/plaintext). aad must match what din's Seal used: the object
// key (the row's data_index_key).
func (c *Cipher) Open(aad string, blob []byte) ([]byte, error) {
	if !IsSealed(blob) {
		return blob, nil
	}
	rest := blob[len(magic):]
	ns := c.aead.NonceSize()
	if len(rest) < ns {
		return nil, fmt.Errorf("blobcrypt: sealed blob too short: %w", ErrDecrypt)
	}
	nonce, ct := rest[:ns], rest[ns:]
	pt, err := c.aead.Open(nil, nonce, ct, []byte(aad))
	if err != nil {
		return nil, fmt.Errorf("blobcrypt: open %q: %w: %w", aad, err, ErrDecrypt)
	}
	return pt, nil
}

// IsSealed reports whether b carries the blobcrypt magic prefix.
func IsSealed(b []byte) bool {
	return len(b) >= len(magic) && bytes.Equal(b[:len(magic)], magic)
}
