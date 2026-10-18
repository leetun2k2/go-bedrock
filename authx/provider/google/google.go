// Package google implements Google OpenID Connect authentication.
package google

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/leetun2k2/go-bedrock/authx/provider"
)

const (
	defaultGoogleAuthURL       = "https://accounts.google.com/o/oauth2/v2/auth"
	defaultGoogleTokenEndpoint = "https://oauth2.googleapis.com/token"
	defaultGoogleCertsEndpoint = "https://www.googleapis.com/oauth2/v3/certs"

	googleIssuerHTTPS = "https://accounts.google.com"
	googleIssuerBare  = "accounts.google.com"

	googleJWTAlg = "RS256"

	defaultGoogleKeysCacheTTL = 1 * time.Hour
	defaultGoogleHTTPTimeout  = 10 * time.Second
)

// AuthorizationRequest is a type alias so consumers of this package do not
// need to import package provider directly.
type AuthorizationRequest = provider.AuthorizationRequest

// ExchangeRequest is a type alias so consumers of this package do not need
// to import package provider directly.
type ExchangeRequest = provider.ExchangeRequest

// Identity is a type alias so consumers of this package do not need to
// import package provider directly.
type Identity = provider.Identity

// Config contains configuration for the Google identity provider.
type Config struct {
	ClientID      string
	ClientSecret  string
	HTTPClient    *http.Client
	AuthURL       string // Optional override, defaults to Google OAuth auth endpoint
	TokenEndpoint string // Optional override, defaults to Google token endpoint
	CertsEndpoint string // Optional override, defaults to Google certs endpoint
}

// Provider implements provider.Provider for Google Sign-In via OpenID Connect.
type Provider struct {
	clientID      string
	clientSecret  string
	httpClient    *http.Client
	authURL       string
	tokenEndpoint string
	certsEndpoint string

	keysMu      sync.Mutex
	keys        map[string]*rsa.PublicKey
	keysFetched time.Time
}

var _ provider.Provider = (*Provider)(nil)

// New creates a Google identity provider adapter.
func New(cfg Config) (*Provider, error) {
	if strings.TrimSpace(cfg.ClientID) == "" {
		return nil, errors.New("google: client ID must not be empty")
	}
	if strings.TrimSpace(cfg.ClientSecret) == "" {
		return nil, errors.New("google: client secret must not be empty")
	}

	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultGoogleHTTPTimeout}
	}

	authURL := cfg.AuthURL
	if authURL == "" {
		authURL = defaultGoogleAuthURL
	}

	tokenEndpoint := cfg.TokenEndpoint
	if tokenEndpoint == "" {
		tokenEndpoint = defaultGoogleTokenEndpoint
	}

	certsEndpoint := cfg.CertsEndpoint
	if certsEndpoint == "" {
		certsEndpoint = defaultGoogleCertsEndpoint
	}

	return &Provider{
		clientID:      cfg.ClientID,
		clientSecret:  cfg.ClientSecret,
		httpClient:    client,
		authURL:       authURL,
		tokenEndpoint: tokenEndpoint,
		certsEndpoint: certsEndpoint,
	}, nil
}

// AuthorizationURL constructs the Google authorization redirect URL.
func (g *Provider) AuthorizationURL(req provider.AuthorizationRequest) (string, error) {
	redirectMissing := strings.TrimSpace(req.RedirectURI) == ""
	stateMissing := strings.TrimSpace(req.State) == ""
	nonceMissing := strings.TrimSpace(req.Nonce) == ""
	if redirectMissing || stateMissing || nonceMissing {
		return "", provider.ErrInvalidRequest
	}

	q := url.Values{}
	q.Set("client_id", g.clientID)
	q.Set("redirect_uri", req.RedirectURI)
	q.Set("response_type", "code")
	q.Set("scope", "openid email profile")
	q.Set("state", req.State)
	q.Set("nonce", req.Nonce)
	q.Set("prompt", "select_account")

	return g.authURL + "?" + q.Encode(), nil
}

type googleTokenResponse struct {
	IDToken string `json:"id_token"`
}

// Exchange exchanges the authorization code for tokens, validates the ID token,
// and returns a normalized Identity. Raw provider tokens are discarded.
func (g *Provider) Exchange(ctx context.Context, req provider.ExchangeRequest) (*provider.Identity, error) {
	codeMissing := strings.TrimSpace(req.Code) == ""
	redirectMissing := strings.TrimSpace(req.RedirectURI) == ""
	nonceMissing := strings.TrimSpace(req.Nonce) == ""
	if codeMissing || redirectMissing || nonceMissing {
		return nil, provider.ErrInvalidRequest
	}

	idToken, err := g.exchangeCode(ctx, req.Code, req.RedirectURI)
	if err != nil {
		return nil, err
	}

	return g.validateIDToken(ctx, idToken, req.Nonce)
}

func (g *Provider) exchangeCode(ctx context.Context, code, redirectURI string) (string, error) {
	form := url.Values{}
	form.Set("code", code)
	form.Set("client_id", g.clientID)
	form.Set("client_secret", g.clientSecret)
	form.Set("redirect_uri", redirectURI)
	form.Set("grant_type", "authorization_code")

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, g.tokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("%w: build request", provider.ErrExchangeFailed)
	}
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := g.httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("%w: call token endpoint", provider.ErrExchangeFailed)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("%w: read response", provider.ErrExchangeFailed)
	}
	if resp.StatusCode != http.StatusOK {
		return "", provider.ErrExchangeFailed
	}

	var tokenResp googleTokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", fmt.Errorf("%w: decode response", provider.ErrExchangeFailed)
	}
	if tokenResp.IDToken == "" {
		return "", provider.ErrExchangeFailed
	}

	return tokenResp.IDToken, nil
}

type googleIDTokenHeader struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
}

type googleIDTokenClaims struct {
	Issuer        string `json:"iss"`
	Audience      string `json:"aud"`
	Subject       string `json:"sub"`
	Nonce         string `json:"nonce"`
	ExpiresAt     int64  `json:"exp"`
	Email         string `json:"email"`
	EmailVerified any    `json:"email_verified"`
	Name          string `json:"name"`
	Picture       string `json:"picture"`
}

func (g *Provider) validateIDToken(ctx context.Context, idToken, expectedNonce string) (*provider.Identity, error) {
	parts := strings.Split(idToken, ".")
	if len(parts) != 3 {
		return nil, provider.ErrInvalidProof
	}
	headerPart, payloadPart, signaturePart := parts[0], parts[1], parts[2]

	headerJSON, err := decodeBase64URLSegment(headerPart)
	if err != nil {
		return nil, provider.ErrInvalidProof
	}
	var header googleIDTokenHeader
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return nil, provider.ErrInvalidProof
	}
	if header.Alg != googleJWTAlg {
		return nil, provider.ErrInvalidProof
	}

	publicKey, err := g.lookupKey(ctx, header.Kid)
	if err != nil {
		return nil, provider.ErrInvalidProof
	}

	signature, err := decodeBase64URLSegment(signaturePart)
	if err != nil {
		return nil, provider.ErrInvalidProof
	}
	hash := sha256.Sum256([]byte(headerPart + "." + payloadPart))
	if err := rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, hash[:], signature); err != nil {
		return nil, provider.ErrInvalidProof
	}

	payloadJSON, err := decodeBase64URLSegment(payloadPart)
	if err != nil {
		return nil, provider.ErrInvalidProof
	}
	var claims googleIDTokenClaims
	if err := json.Unmarshal(payloadJSON, &claims); err != nil {
		return nil, provider.ErrInvalidProof
	}

	if err := g.validateClaims(claims, expectedNonce); err != nil {
		return nil, err
	}

	return &provider.Identity{
		Provider:      "google",
		Subject:       claims.Subject,
		Email:         claims.Email,
		EmailVerified: emailVerifiedTrue(claims.EmailVerified),
		DisplayName:   claims.Name,
		AvatarURL:     claims.Picture,
	}, nil
}

func (g *Provider) validateClaims(claims googleIDTokenClaims, expectedNonce string) error {
	if claims.Issuer != googleIssuerHTTPS && claims.Issuer != googleIssuerBare {
		return provider.ErrInvalidProof
	}
	if claims.Audience != g.clientID {
		return provider.ErrInvalidProof
	}
	if !time.Unix(claims.ExpiresAt, 0).After(time.Now()) {
		return provider.ErrInvalidProof
	}
	if claims.Subject == "" {
		return provider.ErrInvalidProof
	}
	if claims.Nonce != expectedNonce {
		return provider.ErrInvalidProof
	}

	return nil
}

func emailVerifiedTrue(v any) bool {
	b, ok := v.(bool)
	return ok && b
}
