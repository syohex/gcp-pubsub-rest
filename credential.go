package pubsub

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"strings"
	"time"
)

const pubsubScope = "https://www.googleapis.com/auth/pubsub"

type Credential struct {
	PrivateKey  string `json:"private_key"`
	ClientEmail string `json:"client_email"`
	ProjectID   string `json:"project_id"`
}

type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

type jwtClaim struct {
	ISS   string `json:"iss"`
	Scope string `json:"scope"`
	Aud   string `json:"aud"`
	Exp   int64  `json:"exp"`
	Iat   int64  `json:"iat"`
}

type accessTokenResponse struct {
	AccessToken string `json:"access_token"`
}

func NewCredential(r io.Reader) (*Credential, error) {
	var credential Credential
	err := json.NewDecoder(r).Decode(&credential)
	if err != nil {
		return nil, fmt.Errorf("failed to decode json: %w", err)
	}

	return &credential, nil
}

func trimEqualSuffix(str string) string {
	return strings.TrimRight(str, "=")
}

func (c *Credential) toAssertion() (string, error) {
	header := jwtHeader{
		Alg: "RS256",
		Typ: "JWT",
	}

	headerB, err := json.Marshal(header)
	if err != nil {
		return "", fmt.Errorf("failed to marshal header: %w", err)
	}

	now := time.Now()
	claim := jwtClaim{
		ISS:   c.ClientEmail,
		Scope: pubsubScope,
		Aud:   "https://www.googleapis.com/oauth2/v4/token",
		Exp:   now.Unix() + 3600,
		Iat:   now.Unix(),
	}

	claimB, err := json.Marshal(claim)
	if err != nil {
		return "", fmt.Errorf("failed to marshal claim: %w", err)
	}

	headerStr := base64.URLEncoding.EncodeToString(headerB)
	claimStr := base64.URLEncoding.EncodeToString(claimB)

	requestBody := trimEqualSuffix(headerStr) + "." + trimEqualSuffix(claimStr)

	block, _ := pem.Decode([]byte(c.PrivateKey))
	if block == nil {
		return "", fmt.Errorf("failed to decode private key: %w", err)
	}

	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("failed to parse private key: %w", err)
	}

	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return "", fmt.Errorf("invalid private key")
	}

	hasher := crypto.SHA256.New()
	hasher.Write([]byte(requestBody))

	sigByte, err := rsa.SignPKCS1v15(rand.Reader, rsaKey, crypto.SHA256, hasher.Sum(nil))
	if err != nil {
		return "", fmt.Errorf("failed to encrypt: %w", err)
	}

	signature := base64.URLEncoding.EncodeToString(sigByte)
	assertion := requestBody + "." + trimEqualSuffix(signature)
	return assertion, nil
}
