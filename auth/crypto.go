package auth

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
)

const (
	masterKeyLength = 32
	gcmNonceLength  = 12
)

func decodeMasterKey(raw string) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMasterKey, err)
	}
	if len(decoded) != masterKeyLength {
		return nil, fmt.Errorf("%w: expected %d-byte decoded key", ErrInvalidMasterKey, masterKeyLength)
	}
	return decoded, nil
}

func encryptSecret(masterKey []byte, accessKeyID, secret string) (ciphertextB64 string, nonceB64 string, err error) {
	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return "", "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", "", err
	}

	nonce := make([]byte, gcmNonceLength)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", "", err
	}

	ciphertext := gcm.Seal(nil, nonce, []byte(secret), []byte(accessKeyID))
	return base64.StdEncoding.EncodeToString(ciphertext), base64.StdEncoding.EncodeToString(nonce), nil
}

func decryptSecret(masterKey []byte, accessKeyID, ciphertextB64, nonceB64 string) (string, error) {
	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextB64)
	if err != nil {
		return "", err
	}
	nonce, err := base64.StdEncoding.DecodeString(nonceB64)
	if err != nil {
		return "", err
	}
	if len(nonce) != gcmNonceLength {
		return "", fmt.Errorf("invalid nonce length: %d", len(nonce))
	}

	plaintext, err := gcm.Open(nil, nonce, ciphertext, []byte(accessKeyID))
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}
