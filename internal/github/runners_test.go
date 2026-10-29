package github

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testKeyPEM(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

// runnersServer serves total runners named r0.. in pages, and counts token mints.
func runnersServer(t *testing.T, total int, status int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var mints atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("POST /app/installations/7/access_tokens", func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			http.Error(w, "no jwt", http.StatusUnauthorized)
			return
		}
		seg := strings.Split(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), ".")
		claims, _ := base64.RawURLEncoding.DecodeString(seg[1])
		if !strings.Contains(string(claims), `"iss":"Iv1.abc"`) {
			http.Error(w, "wrong issuer", http.StatusUnauthorized)
			return
		}
		mints.Add(1)
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"token": "tok", "expires_at": time.Now().Add(time.Hour)})
	})
	mux.HandleFunc("GET /repos/o/r/actions/runners", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "token tok" {
			http.Error(w, "bad token", http.StatusUnauthorized)
			return
		}
		if status != 0 {
			http.Error(w, "boom", status)
			return
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		per, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
		type runner struct {
			Name string `json:"name"`
		}
		out := []runner{}
		for i := (page - 1) * per; i < min(page*per, total); i++ {
			out = append(out, runner{fmt.Sprintf("r%d", i)})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"total_count": total, "runners": out})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &mints
}

func TestListRunnerNamesPagesAndReusesToken(t *testing.T) {
	srv, mints := runnersServer(t, 250, 0)
	l, err := newLister("https://github.com/o/r", srv.URL, "Iv1.abc", 7, testKeyPEM(t), http.DefaultTransport)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		names, err := l.ListRunnerNames(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(names) != 250 || names[0] != "r0" || names[249] != "r249" {
			t.Fatalf("got %d names", len(names))
		}
	}
	if n := mints.Load(); n != 1 {
		t.Fatalf("minted %d installation tokens, want 1", n)
	}
}

func TestListRunnerNamesExactPageBoundary(t *testing.T) {
	srv, _ := runnersServer(t, 100, 0)
	l, _ := newLister("https://github.com/o/r", srv.URL, "Iv1.abc", 7, testKeyPEM(t), http.DefaultTransport)
	names, err := l.ListRunnerNames(context.Background())
	if err != nil || len(names) != 100 {
		t.Fatalf("got %d names, err %v", len(names), err)
	}
}

func TestListRunnerNamesSurfacesStatus(t *testing.T) {
	srv, _ := runnersServer(t, 3, http.StatusForbidden)
	l, _ := newLister("https://github.com/o/r", srv.URL, "Iv1.abc", 7, testKeyPEM(t), http.DefaultTransport)
	if _, err := l.ListRunnerNames(context.Background()); err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("err = %v", err)
	}
}

func TestListRunnerNamesSurfacesTokenFailure(t *testing.T) {
	srv, _ := runnersServer(t, 3, 0)
	l, _ := newLister("https://github.com/o/r", srv.URL, "Iv1.abc", 8, testKeyPEM(t), http.DefaultTransport)
	if _, err := l.ListRunnerNames(context.Background()); err == nil {
		t.Fatal("want an error")
	}
}

func TestNewListerRejectsNonRepositoryURL(t *testing.T) {
	if _, err := newLister("https://github.com/o", "", "c", 1, testKeyPEM(t), http.DefaultTransport); err == nil {
		t.Fatal("want an error")
	}
}

func TestAPIRoot(t *testing.T) {
	if got := apiRoot("https://github.com/o/r"); got != "https://api.github.com" {
		t.Fatal(got)
	}
	if got := apiRoot("https://ghe.example/o/r"); got != "https://ghe.example/api/v3" {
		t.Fatal(got)
	}
}
