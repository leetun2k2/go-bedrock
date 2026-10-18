package credential

import (
	"net/http"
	"strings"
)

const bearerPrefix = "bearer "

// Credentials holds client_id and client_secret extracted from an HTTP request.
type Credentials struct {
	ClientID     string
	ClientSecret string
}

// Extract extracts client credentials from either the HTTP Basic Auth
// header or the X-Client-Id / X-Client-Secret header pair. Basic auth and
// X-Client-Secret always require a non-empty secret (confidential client).
// X-Client-Id alone, with no secret in either form, is returned with an
// empty ClientSecret for a public client (e.g. a registered mobile app
// using PKCE instead of a static secret); whether an empty secret is
// acceptable is a policy decision for the caller, based on the resolved
// registration's client type, not this extraction step. Supplying both
// forms, or no credentials at all, fails closed with ErrInvalidClient.
func Extract(r *http.Request) (Credentials, error) {
	if r == nil {
		return Credentials{}, ErrInvalidClient
	}

	authHeader := r.Header.Get("Authorization")
	hasBasic := strings.HasPrefix(strings.ToLower(authHeader), "basic ")

	headerID := r.Header.Get("X-Client-Id")
	headerSecret := r.Header.Get("X-Client-Secret")
	hasHeader := headerID != "" || headerSecret != ""

	if hasBasic && hasHeader {
		return Credentials{}, ErrInvalidClient
	}

	switch {
	case hasBasic:
		id, secret, ok := r.BasicAuth()
		if !ok || strings.TrimSpace(id) == "" || strings.TrimSpace(secret) == "" {
			return Credentials{}, ErrInvalidClient
		}
		return Credentials{ClientID: id, ClientSecret: secret}, nil
	case headerID != "":
		return Credentials{ClientID: headerID, ClientSecret: headerSecret}, nil
	case hasHeader:
		// X-Client-Secret with no X-Client-Id is never valid.
		return Credentials{}, ErrInvalidClient
	default:
		return Credentials{}, ErrInvalidClient
	}
}

// ExtractBearerToken extracts a bearer token from an Authorization header value.
// The scheme check is case-insensitive per RFC 7235. Returns false if the header
// is missing, does not start with Bearer, or has an empty token.
func ExtractBearerToken(header string) (string, bool) {
	if len(header) < len(bearerPrefix) || !strings.EqualFold(header[:len(bearerPrefix)], bearerPrefix) {
		return "", false
	}

	token := strings.TrimSpace(header[len(bearerPrefix):])
	if token == "" {
		return "", false
	}

	return token, true
}

// ExtractBearerTokenFromRequest extracts a bearer token from the Authorization
// header of an HTTP request.
func ExtractBearerTokenFromRequest(r *http.Request) (string, bool) {
	if r == nil {
		return "", false
	}
	return ExtractBearerToken(r.Header.Get("Authorization"))
}
