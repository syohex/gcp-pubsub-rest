package pubsub

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

func getAccessToken(c *Credential) (string, error) {
	assertion, err := c.toAssertion()
	if err != nil {
		return "", err
	}

	data := url.Values{}
	data.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
	data.Set("assertion", assertion)

	r := strings.NewReader(data.Encode())

	client := &http.Client{}
	baseURL := "https://www.googleapis.com/oauth2/v4/token"
	req, err := http.NewRequest("POST", baseURL, r)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	res, err := client.Do(req)
	if err != nil {
		return "", nil
	}
	defer res.Body.Close()

	var tokenRes accessTokenResponse
	if err := json.NewDecoder(res.Body).Decode(&tokenRes); err != nil {
		return "", err
	}

	return tokenRes.AccessToken, nil
}
