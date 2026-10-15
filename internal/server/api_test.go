package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vivekdavara/ledger/internal/ledger"
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
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
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
}

// raw sends a request with exactly the given headers and body.
func raw(t *testing.T, h http.Handler, method, path string, header map[string][]string, body string) response {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("%s %s: body %q is not JSON: %v", method, path, rec.Body.String(), err)
	}
	return response{rec.Code, rec.Header(), out}
}

// encoding/json quietly accepts things the API must not: bytes that aren't UTF-8 (turned into U+FFFD, so two names
// became one account), a key given twice (the last wins), and keys that match a field only when case is ignored.
// The API also wants a JSON content type and a body under 64 KB.
func TestRESTRefusesWhatEncodingJSONWouldAccept(t *testing.T) {
	t.Parallel()
	h := newAPI(t)
	cash := createAccount(t, h, "cash:bank", "asset", "USD")
	alice := createAccount(t, h, "wallet:alice", "liability", "USD")
	jsonType := map[string][]string{"Content-Type": {"application/json"}}
	keyed := func(keys ...string) map[string][]string {
		return map[string][]string{"Content-Type": {"application/json"}, "Idempotency-Key": keys}
	}
	valid := fmt.Sprintf(`{"postings":[{"account_id":%d,"currency":"USD","amount":"1.00"},
		{"account_id":%d,"currency":"USD","amount":"-1.00"}]}`, cash, alice)
	cases := []struct {
		name, path string
		header     map[string][]string
		body       string
		code       int
		reason     string
		message    string
	}{
		{"not UTF-8", "/v1/accounts", jsonType, "{\"name\":\"caf\xe9\",\"type\":\"asset\",\"currency\":\"USD\"}", 400,
			"malformed_request", "not valid UTF-8"},
		{"a key twice", "/v1/accounts", jsonType, `{"name":"x","type":"asset","currency":"USD","name":"y"}`, 400,
			"malformed_request", `"name" appears twice`},
		{"a key in another case", "/v1/accounts", jsonType, `{"Name":"y","type":"asset","currency":"USD"}`, 400,
			"malformed_request", `unknown field "Name"`},
		{"an amount twice", "/v1/entries", keyed("k1"), fmt.Sprintf(`{"postings":[{"account_id":%d,"currency":"USD",
			"amount":"1.00","amount":"2.00"},{"account_id":%d,"currency":"USD","amount":"-2.00"}]}`, cash, alice), 400,
			"malformed_request", `"amount" appears twice`},
		{"a capitalised field in a posting", "/v1/entries", keyed("k2"), fmt.Sprintf(`{"postings":[{"ACCOUNT_ID":%d,
			"currency":"USD","amount":"1.00"},{"account_id":%d,"currency":"USD","amount":"-1.00"}]}`, cash, alice), 400,
			"malformed_request", `unknown field "ACCOUNT_ID"`},
		{"no content type", "/v1/entries", map[string][]string{"Idempotency-Key": {"k3"}}, valid, 415,
			"unsupported_media_type", "application/json"},
		{"text/plain", "/v1/entries", map[string][]string{"Content-Type": {"text/plain"}, "Idempotency-Key": {"k4"}},
			valid, 415, "unsupported_media_type", ""},
		{"two keys", "/v1/entries", keyed("k5", "k6"), valid, 400, "malformed_request", "exactly one Idempotency-Key"},
		// A valid entry, padded past 64 KB: without the cap it would post.
		{"over 64 KB", "/v1/entries", keyed("k7"), valid + strings.Repeat(" ", 64<<10), 413, "request_too_large",
			"at most 65536 bytes"},
		{"an array", "/v1/entries", keyed("k8"), `[]`, 400, "malformed_request", "the body must be a JSON object"},
		{"a huge account id", "/v1/entries", keyed("k9"), `{"postings":[{"account_id":99999999999999999999,
			"currency":"USD","amount":"1.00"}]}`, 400, "malformed_request", "fits in 64 bits"},
		{"an empty body", "/v1/entries", keyed("k10"), ``, 400, "malformed_request", "empty or cut short"},
	}
	for _, tc := range cases {
		r := raw(t, h, "POST", tc.path, tc.header, tc.body)
		if reason, _ := r.reason(); r.code != tc.code || reason != tc.reason || !strings.Contains(r.message(), tc.message) {
			t.Errorf("%s: %d %s %q; want %d %s mentioning %q", tc.name, r.code, reason, r.message(), tc.code,
				tc.reason, tc.message)
		}
		if strings.Contains(r.message(), "Go value") || strings.Contains(r.message(), "server.") {
			t.Errorf("%s: the message names Go types: %q", tc.name, r.message())
		}
	}
	// A charset parameter is fine, and so is the same valid entry under the cap.
	if r := raw(t, h, "POST", "/v1/entries", map[string][]string{"Content-Type": {"application/json; charset=utf-8"},
		"Idempotency-Key": {"ok"}}, valid); r.code != http.StatusCreated {
		t.Fatalf("the valid entry: %d %v", r.code, r.body)
	}
	if r := call(t, h, "GET", fmt.Sprintf("/v1/accounts/%d/postings", alice), "", ""); len(r.body["postings"].([]any)) != 1 {
		t.Fatalf("a refused request posted something: %v", r.body)
	}
}

func TestRouterErrorsAreJSON(t *testing.T) {
	t.Parallel()
	h := newAPI(t)
	if r := call(t, h, "GET", "/v1/nope", "", ""); r.code != 404 {
		t.Errorf("an unknown path: %d %v", r.code, r.body)
	} else if reason, _ := r.reason(); reason != "not_found" {
		t.Errorf("an unknown path: reason %q", reason)
	}
	r := call(t, h, "DELETE", "/v1/accounts/1", "", "")
	if reason, _ := r.reason(); r.code != 405 || reason != "method_not_allowed" || !strings.Contains(r.header.Get("Allow"), "GET") {
		t.Errorf("DELETE an account: %d %v, Allow %q", r.code, r.body, r.header.Get("Allow"))
	}
}

func TestADefaultPageSaysWhereTheNextStarts(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	s := store.New(db)
	h := server.New(func(ctx context.Context) error { return store.Ready(ctx, db) }, s, quiet)
	ctx := context.Background()
	open := func(name string, typ ledger.AccountType) int64 {
		a, _, err := s.CreateAccount(ctx, ledger.NewAccount{Name: name, Type: typ, Currency: "USD", AllowNegative: true})
		if err != nil {
			t.Fatal(err)
		}
		return a.ID
	}
	a, b := open("a", ledger.Asset), open("b", ledger.Asset)
	for i := range store.DefaultPageSize + 1 {
		if _, _, err := s.PostEntry(ctx, fmt.Sprintf("p%d", i), ledger.Entry{Postings: []ledger.Posting{
			{AccountID: a, Currency: "USD", Amount: 1}, {AccountID: b, Currency: "USD", Amount: -1}}}); err != nil {
			t.Fatal(err)
		}
	}
	r := call(t, h, "GET", fmt.Sprintf("/v1/accounts/%d/postings", a), "", "")
	if n := len(r.body["postings"].([]any)); n != store.DefaultPageSize || r.body["next_after_version"] != float64(store.DefaultPageSize) {
		t.Fatalf("default page: %d lines, next_after_version %v", n, r.body["next_after_version"])
	}
}

func TestARequestStuckOnALockGivesUpAtItsDeadline(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	h := server.NewWithTimeout(func(ctx context.Context) error { return store.Ready(ctx, db) }, store.New(db), quiet,
		300*time.Millisecond)
	cash := createAccount(t, h, "cash:bank", "asset", "USD")
	alice := createAccount(t, h, "wallet:alice", "liability", "USD")
	pay := fmt.Sprintf(`{"postings":[{"account_id":%d,"currency":"USD","amount":"1.00"},
		{"account_id":%d,"currency":"USD","amount":"-1.00"}]}`, cash, alice)
	// Another session holds Alice's row, as a stuck psql session might.
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT 1 FROM accounts WHERE id = $1 FOR UPDATE`, alice); err != nil {
		t.Fatal(err)
	}
	// The client's own 10 s deadline is a backstop: without the server's bound this test fails on the time
	// taken, instead of hanging.
	backstop, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req := httptest.NewRequest("POST", "/v1/entries", strings.NewReader(pay)).WithContext(backstop)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "pay-1")
	rec := httptest.NewRecorder()
	start := time.Now()
	h.ServeHTTP(rec, req)
	took := time.Since(start)
	if rec.Code != http.StatusGatewayTimeout || !strings.Contains(rec.Body.String(), `"deadline_exceeded"`) {
		t.Fatalf("got %d %s, want 504 deadline_exceeded", rec.Code, rec.Body.String())
	}
	if took > 5*time.Second {
		t.Fatalf("gave up after %v, not at the server's 300 ms bound", took)
	}
	// Nothing was posted and the key wasn't used up: once the lock is gone, the same request posts.
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if r := call(t, h, "POST", "/v1/entries", "pay-1", pay); r.code != http.StatusCreated {
		t.Fatalf("after the lock: %d %v", r.code, r.body)
	}
}

// A replay is the original answer: the entry is rebuilt from append-only history, so the body is the same bytes.
// GET /v1/entries/{id} answers with what posting the entry answered, byte for byte: a client that lost the answer to
// a POST can read the entry back by the id it finds in an account's history.
func TestAnEntryReadBackIsWhatPostingItAnswered(t *testing.T) {
	t.Parallel()
	h := newAPI(t)
	cash := createAccount(t, h, "cash:bank", "asset", "USD")
	alice := createAccount(t, h, "wallet:alice", "liability", "USD")
	body := fmt.Sprintf(`{"description":"top up","postings":[{"account_id":%d,"currency":"USD","amount":"-50.00"},
		{"account_id":%d,"currency":"USD","amount":"50.00"}]}`, alice, cash) // not in account-id order
	req := httptest.NewRequest("POST", "/v1/entries", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "top-up-1")
	posted := httptest.NewRecorder()
	h.ServeHTTP(posted, req)
	var entry struct{ ID int64 }
	if err := json.Unmarshal(posted.Body.Bytes(), &entry); err != nil || posted.Code != http.StatusCreated {
		t.Fatalf("post: %d %s", posted.Code, posted.Body.String())
	}
	read := httptest.NewRecorder()
	h.ServeHTTP(read, httptest.NewRequest("GET", fmt.Sprintf("/v1/entries/%d", entry.ID), nil))
	if read.Code != http.StatusOK || read.Body.String() != posted.Body.String() {
		t.Fatalf("read back %d:\n%s\nposted:\n%s", read.Code, read.Body.String(), posted.Body.String())
	}
	for _, tc := range []struct {
		path, reason string
		code         int
		message      string
	}{
		{fmt.Sprintf("/v1/entries/%d", entry.ID+1), "not_found", http.StatusNotFound, "there is no entry"},
		{"/v1/entries/0", "invalid_argument", http.StatusBadRequest, "entry id 0 is not positive"},
		{"/v1/entries/-3", "invalid_argument", http.StatusBadRequest, "entry id -3 is not positive"},
		{"/v1/entries/top-up-1", "malformed_request", http.StatusBadRequest, `"top-up-1" is not an entry id`},
	} {
		r := call(t, h, "GET", tc.path, "", "")
		if reason, posting := r.reason(); r.code != tc.code || reason != tc.reason || posting != -1 ||
			!strings.Contains(r.message(), tc.message) {
			t.Errorf("GET %s: %d %s %d %q; want %d %s, no posting, %q", tc.path, r.code, reason, posting, r.message(),
				tc.code, tc.reason, tc.message)
		}
	}
}

func TestAReplayIsTheOriginalResponseByteForByte(t *testing.T) {
	t.Parallel()
	h := newAPI(t)
	cash := createAccount(t, h, "cash:bank", "asset", "USD")
	alice := createAccount(t, h, "wallet:alice", "liability", "USD")
	body := fmt.Sprintf(`{"description":"top up","postings":[{"account_id":%d,"currency":"USD","amount":"50.00"},
		{"account_id":%d,"currency":"USD","amount":"-50.00"}]}`, cash, alice)
	send := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/v1/entries", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "top-up-1")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	first, again := send(), send()
	if first.Code != http.StatusCreated || again.Code != http.StatusOK {
		t.Fatalf("statuses %d and %d", first.Code, again.Code)
	}
	if first.Body.String() != again.Body.String() {
		t.Fatalf("the replay differs:\n%s\n%s", first.Body.String(), again.Body.String())
	}
}
