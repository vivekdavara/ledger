package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vivekdavara/ledger/internal/server"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

type response struct {
	code   int
	header http.Header
	body   map[string]any
}

// call sends one request to h and decodes the JSON answer.
func call(t *testing.T, h http.Handler, method, path, key, body string) response {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("%s %s: body %q is not JSON: %v", method, path, rec.Body.String(), err)
	}
	return response{rec.Code, rec.Header(), out}
}

// reason is the error reason in a failed response, and the index of the posting it names (-1 for none).
func (r response) reason() (string, int) {
	e, _ := r.body["error"].(map[string]any)
	reason, _ := e["reason"].(string)
	posting := -1
	if p, ok := e["posting"].(float64); ok {
		posting = int(p)
	}
	return reason, posting
}

func (r response) message() string {
	e, _ := r.body["error"].(map[string]any)
	m, _ := e["message"].(string)
	return m
}

func newAPI(t *testing.T) http.Handler {
	t.Helper()
	db := storetest.Migrated(t)
	return server.New(func(ctx context.Context) error { return store.Ready(ctx, db) }, store.New(db), quiet)
}

func createAccount(t *testing.T, h http.Handler, name, typ, currency string) int64 {
	t.Helper()
	r := call(t, h, "POST", "/v1/accounts", "", fmt.Sprintf(`{"name":%q,"type":%q,"currency":%q}`, name, typ, currency))
	if r.code != http.StatusCreated {
		t.Fatalf("create %s: %d %v", name, r.code, r.body)
	}
	return int64(r.body["id"].(float64))
}

func TestRESTAccounts(t *testing.T) {
	t.Parallel()
	h := newAPI(t)
	body := `{"name":"wallet:alice","type":"liability","currency":"USD"}`
	r := call(t, h, "POST", "/v1/accounts", "", body)
	if r.code != http.StatusCreated || r.header.Get("Location") != fmt.Sprintf("/v1/accounts/%v", r.body["id"]) {
		t.Fatalf("create: %d, Location %q, %v", r.code, r.header.Get("Location"), r.body)
	}
	want := map[string]any{"name": "wallet:alice", "type": "liability", "currency": "USD", "allow_negative": false,
		"balance": "0.00", "shown_balance": "0.00", "normal_side": "credit", "version": float64(0)}
	for k, v := range want {
		if r.body[k] != v {
			t.Errorf("%s = %v, want %v", k, r.body[k], v)
		}
	}
	// The same account again is a retry: 200 and the same account. A different one under the name is a clash.
	if again := call(t, h, "POST", "/v1/accounts", "", body); again.code != http.StatusOK || again.body["id"] != r.body["id"] {
		t.Fatalf("again: %d %v", again.code, again.body)
	}
	clash := call(t, h, "POST", "/v1/accounts", "", `{"name":"wallet:alice","type":"asset","currency":"USD"}`)
	if reason, _ := clash.reason(); clash.code != http.StatusConflict || reason != "account_name_taken" {
		t.Fatalf("clash: %d %v", clash.code, clash.body)
	}
	got := call(t, h, "GET", fmt.Sprintf("/v1/accounts/%v", r.body["id"]), "", "")
	if got.code != http.StatusOK || got.body["name"] != "wallet:alice" {
		t.Fatalf("get: %d %v", got.code, got.body)
	}
	if missing := call(t, h, "GET", "/v1/accounts/999999", "", ""); missing.code != http.StatusNotFound {
		t.Fatalf("missing: %d %v", missing.code, missing.body)
	}
}

func TestRESTPostingAndHistory(t *testing.T) {
	t.Parallel()
	h := newAPI(t)
	cash := createAccount(t, h, "cash:bank", "asset", "USD")
	alice := createAccount(t, h, "wallet:alice", "liability", "USD")
	bob := createAccount(t, h, "wallet:bob", "liability", "USD")
	fees := createAccount(t, h, "revenue:fees", "revenue", "USD")
	post := func(key, body string) response { return call(t, h, "POST", "/v1/entries", key, body) }

	topUp := fmt.Sprintf(`{"description":"Alice tops up $50","postings":[
		{"account_id":%d,"currency":"USD","amount":"50.00"},{"account_id":%d,"currency":"USD","amount":"-50"}]}`,
		cash, alice)
	if r := post("top-up-1", topUp); r.code != http.StatusCreated || r.header.Get("Idempotent-Replayed") != "" {
		t.Fatalf("top up: %d %v", r.code, r.body)
	}
	pay := fmt.Sprintf(`{"description":"Alice pays Bob $20, 3%% fee","postings":[
		{"account_id":%d,"currency":"USD","amount":"20.00"},{"account_id":%d,"currency":"USD","amount":"-19.40"},
		{"account_id":%d,"currency":"USD","amount":"-0.60"}]}`, alice, bob, fees)
	first := post("pay-1", pay)
	if first.code != http.StatusCreated {
		t.Fatalf("pay: %d %v", first.code, first.body)
	}
	postings := first.body["postings"].([]any)
	if p := postings[0].(map[string]any); p["amount"] != "20.00" || p["balance_after"] != "-30.00" ||
		p["account_version"] != float64(2) {
		t.Fatalf("alice's posting: %v", p)
	}
	// A retry gets the same entry back, marked as replayed, and moves nothing.
	again := post("pay-1", pay)
	if again.code != http.StatusOK || again.header.Get("Idempotent-Replayed") != "true" || again.body["id"] != first.body["id"] {
		t.Fatalf("retry: %d %v %v", again.code, again.header, again.body)
	}
	a := call(t, h, "GET", fmt.Sprintf("/v1/accounts/%d", alice), "", "")
	if a.body["balance"] != "-30.00" || a.body["shown_balance"] != "30.00" || a.body["version"] != float64(2) {
		t.Fatalf("alice: %v", a.body)
	}
	// History in pages of one: the top-up, the payment, then an empty page.
	page1 := call(t, h, "GET", fmt.Sprintf("/v1/accounts/%d/postings?limit=1", alice), "", "")
	lines := page1.body["postings"].([]any)
	if page1.code != http.StatusOK || len(lines) != 1 || page1.body["next_after_version"] != float64(1) ||
		lines[0].(map[string]any)["description"] != "Alice tops up $50" {
		t.Fatalf("page 1: %d %v", page1.code, page1.body)
	}
	page2 := call(t, h, "GET", fmt.Sprintf("/v1/accounts/%d/postings?limit=1&after_version=1", alice), "", "")
	if l := page2.body["postings"].([]any); len(l) != 1 || l[0].(map[string]any)["balance_after"] != "-30.00" {
		t.Fatalf("page 2: %v", page2.body)
	}
	page3 := call(t, h, "GET", fmt.Sprintf("/v1/accounts/%d/postings?limit=1&after_version=2", alice), "", "")
	if l := page3.body["postings"].([]any); len(l) != 0 || page3.body["next_after_version"] != nil {
		t.Fatalf("page 3: %v", page3.body)
	}
	all := call(t, h, "GET", fmt.Sprintf("/v1/accounts/%d/postings", alice), "", "")
	if l := all.body["postings"].([]any); len(l) != 2 || all.body["next_after_version"] != nil {
		t.Fatalf("one default page: %v", all.body)
	}
}

func TestRESTErrors(t *testing.T) {
	t.Parallel()
	h := newAPI(t)
	cash := createAccount(t, h, "cash:bank", "asset", "USD")
	alice := createAccount(t, h, "wallet:alice", "liability", "USD")
	eur := createAccount(t, h, "wallet:alice:eur", "liability", "EUR")
	entry := func(postings ...string) string {
		return `{"description":"test","postings":[` + strings.Join(postings, ",") + `]}`
	}
	leg := func(account int64, currency, amount string) string {
		return fmt.Sprintf(`{"account_id":%d,"currency":%q,"amount":%q}`, account, currency, amount)
	}
	if r := call(t, h, "POST", "/v1/entries", "fund", entry(leg(cash, "USD", "10.00"), leg(alice, "USD", "-10.00"))); r.code != 201 {
		t.Fatalf("fund: %d %v", r.code, r.body)
	}
	cases := []struct {
		name, key, body string
		code            int
		reason          string
		posting         int
		message         string
	}{
		{"no key", "", entry(leg(cash, "USD", "1.00"), leg(alice, "USD", "-1.00")), 400, "malformed_request", -1,
			"Idempotency-Key"},
		{"bad key", "has space", entry(leg(cash, "USD", "1.00"), leg(alice, "USD", "-1.00")), 400,
			"invalid_argument", -1, "visible ASCII"},
		{"not JSON", "k1", `{"postings":`, 400, "malformed_request", -1, ""},
		{"unknown field", "k2", `{"description":"x","postings":[],"memo":"x"}`, 400, "malformed_request", -1, "memo"},
		{"two objects", "k3", entry(leg(cash, "USD", "1"), leg(alice, "USD", "-1")) + "{}", 400, "malformed_request",
			-1, "more than one"},
		{"amount as a number", "k4", `{"postings":[{"account_id":1,"currency":"USD","amount":1.5}]}`, 400,
			"malformed_request", -1, "decimal strings"},
		{"too many decimals", "k5", entry(leg(cash, "USD", "1.005"), leg(alice, "USD", "-1.005")), 400,
			"invalid_argument", 0, "2 decimal places"},
		{"unknown currency", "k6", entry(leg(cash, "USD", "1"), leg(alice, "XXX", "-1")), 400, "invalid_argument", 1,
			"XXX"},
		{"one posting", "k7", entry(leg(cash, "USD", "1")), 400, "invalid_argument", -1, "postings"},
		{"unbalanced", "k8", entry(leg(cash, "USD", "1.00"), leg(alice, "USD", "-0.99")), 400, "unbalanced", -1,
			"off in USD by 1"},
		{"unknown account", "k9", entry(leg(cash, "USD", "1"), leg(999999, "USD", "-1")), 422, "unknown_account", 1,
			"999999"},
		{"wrong currency", "k10", entry(leg(cash, "USD", "1"), leg(eur, "USD", "-1")), 422, "currency_mismatch", 1,
			"is in EUR"},
		{"overdraft", "k11", entry(leg(alice, "USD", "10.01"), leg(cash, "USD", "-10.01")), 422,
			"insufficient_funds", 0, "has 10.00 USD"},
		{"key reused", "fund", entry(leg(cash, "USD", "2.00"), leg(alice, "USD", "-2.00")), 422,
			"idempotency_key_reused", -1, "fund"},
	}
	for _, tc := range cases {
		r := call(t, h, "POST", "/v1/entries", tc.key, tc.body)
		reason, posting := r.reason()
		if r.code != tc.code || reason != tc.reason || posting != tc.posting || !strings.Contains(r.message(), tc.message) {
			t.Errorf("%s: %d %s posting %d %q; want %d %s posting %d mentioning %q", tc.name, r.code, reason,
				posting, r.message(), tc.code, tc.reason, tc.posting, tc.message)
		}
	}
	for _, path := range []string{"/v1/accounts/abc", "/v1/accounts/0", "/v1/accounts/1/postings?limit=-1",
		"/v1/accounts/1/postings?after_version=x"} {
		if r := call(t, h, "GET", path, "", ""); r.code != 400 {
			t.Errorf("GET %s: %d %v", path, r.code, r.body)
		}
	}
	if r := call(t, h, "GET", "/v1/accounts/1/postings?limit=1001", "", ""); r.code != 400 || !strings.Contains(r.message(), "1 to 1000") {
		t.Errorf("limit 1001: %d %v", r.code, r.body)
	}
	if r := call(t, h, "POST", "/v1/entries", "big", `{"description":"`+strings.Repeat("x", 70<<10)+`"}`); r.code != 400 {
		t.Errorf("a 70 KB body: %d %v", r.code, r.body)
	}
}
