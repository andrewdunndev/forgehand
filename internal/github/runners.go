package github

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/bradleyfalzon/ghinstallation/v2"
	jwt "github.com/golang-jwt/jwt/v4"
)

const runnersPageSize = 100

// lister reads a repository's runner names over the REST API, which the
// scaleset client has no call for.
type lister struct {
	hc   *http.Client
	path string
}

// issuerSigner signs the App JWT with the App's client id as issuer.
type issuerSigner struct {
	ghinstallation.Signer
	issuer string
}

func (s issuerSigner) Sign(claims jwt.Claims) (string, error) {
	if c, ok := claims.(*jwt.RegisteredClaims); ok {
		c.Issuer = s.issuer
	}
	return s.Signer.Sign(claims)
}

// newLister authenticates as the App installation. apiBase is the REST root
// for repoURL's host.
func newLister(repoURL, apiBase, clientID string, installationID int64, pem []byte, base http.RoundTripper) (*lister, error) {
	u, err := url.Parse(repoURL)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) != 2 {
		return nil, fmt.Errorf("url %q is not a repository URL", repoURL)
	}
	var key *rsa.PrivateKey
	if key, err = jwt.ParseRSAPrivateKeyFromPEM(pem); err != nil {
		return nil, fmt.Errorf("parse github_app.private_key_path: %w", err)
	}
	atr, err := ghinstallation.NewAppsTransportWithOptions(base, 0, ghinstallation.WithSigner(
		issuerSigner{ghinstallation.NewRSASigner(jwt.SigningMethodRS256, key), clientID}))
	if err != nil {
		return nil, err
	}
	atr.BaseURL = apiBase
	tr := ghinstallation.NewFromAppsTransport(atr, installationID)
	return &lister{
		hc:   &http.Client{Transport: tr},
		path: apiBase + "/repos/" + url.PathEscape(parts[0]) + "/" + url.PathEscape(parts[1]) + "/actions/runners",
	}, nil
}

// apiRoot returns the REST root serving repoURL.
func apiRoot(repoURL string) string {
	u, err := url.Parse(repoURL)
	if err != nil || u.Host == "github.com" {
		return "https://api.github.com"
	}
	return u.Scheme + "://" + u.Host + "/api/v3"
}

// ListRunnerNames returns the name of every runner registered on the repository.
func (l *lister) ListRunnerNames(ctx context.Context) ([]string, error) {
	var names []string
	for page := 1; ; page++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s?per_page=%d&page=%d", l.path, runnersPageSize, page), nil)
		if err != nil {
			return nil, err
		}
		resp, err := l.hc.Do(req)
		if err != nil {
			return nil, fmt.Errorf("list runners: %w", err)
		}
		var body struct {
			Runners []struct {
				Name string `json:"name"`
			} `json:"runners"`
		}
		err = func() error {
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("list runners: %s", resp.Status)
			}
			return json.NewDecoder(resp.Body).Decode(&body)
		}()
		if err != nil {
			return nil, err
		}
		for _, r := range body.Runners {
			names = append(names, r.Name)
		}
		if len(body.Runners) < runnersPageSize {
			return names, nil
		}
	}
}
