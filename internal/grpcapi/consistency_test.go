package grpcapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	ledgerv1 "github.com/vivekdavara/ledger/internal/gen/ledger/v1"
	"github.com/vivekdavara/ledger/internal/grpcapi"
	"github.com/vivekdavara/ledger/internal/server"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// TestBothAPIsGiveTheSameReason sends the same failing requests through REST and gRPC, over one store, and requires
// the same reason and the same posting from both. DESIGN.md says the two APIs can't disagree; this checks it.
func TestBothAPIsGiveTheSameReason(t *testing.T) {
	t.Parallel()
	db := storetest.Migrated(t)
	svc := store.New(db)
	rest := server.New(func(ctx context.Context) error { return store.Ready(ctx, db) }, svc, quiet)
	lis := bufconn.Listen(1 << 20)
	srv := grpcapi.New(svc, quiet)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///ledger",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := ledgerv1.NewLedgerServiceClient(conn)
	ctx := context.Background()

	cash := open(t, c, "cash:bank", ledgerv1.AccountType_ACCOUNT_TYPE_ASSET, "USD")
	alice := open(t, c, "wallet:alice", ledgerv1.AccountType_ACCOUNT_TYPE_LIABILITY, "USD")
	eur := open(t, c, "wallet:alice:eur", ledgerv1.AccountType_ACCOUNT_TYPE_LIABILITY, "EUR")
	if _, err := c.PostEntry(ctx, &ledgerv1.PostEntryRequest{IdempotencyKey: "fund",
		Postings: []*ledgerv1.Posting{posting(cash, 1000), posting(alice, -1000)}}); err != nil {
		t.Fatal(err)
	}

	// An entry in both forms: REST amounts in dollars, gRPC in cents.
	type leg struct {
		account  int64
		currency string
		cents    int64
	}
	restEntry := func(description string, legs ...leg) string {
		parts := make([]string, len(legs))
		for i, l := range legs {
			sign, cents := "", l.cents
			if cents < 0 {
				sign, cents = "-", -cents
			}
			parts[i] = fmt.Sprintf(`{"account_id":%d,"currency":%q,"amount":"%s%d.%02d"}`, l.account, l.currency, sign,
				cents/100, cents%100)
		}
		return fmt.Sprintf(`{"description":%q,"postings":[%s]}`, description, strings.Join(parts, ","))
	}
	grpcEntry := func(key, description string, legs ...leg) *ledgerv1.PostEntryRequest {
		req := &ledgerv1.PostEntryRequest{IdempotencyKey: key, Description: description}
		for _, l := range legs {
			req.Postings = append(req.Postings, &ledgerv1.Posting{AccountId: l.account, Currency: l.currency,
				Amount: l.cents})
		}
		return req
	}
	type viaREST struct{ method, path, key, body string }
	cases := []struct {
		name string
		rest viaREST
		grpc func() error
	}{}
	entryCase := func(name, key, description string, legs ...leg) {
		cases = append(cases, struct {
			name string
			rest viaREST
			grpc func() error
		}{name, viaREST{"POST", "/v1/entries", key, restEntry(description, legs...)}, func() error {
			_, err := c.PostEntry(ctx, grpcEntry(key, description, legs...))
			return err
		}})
	}
	entryCase("unbalanced", "k1", "", leg{cash, "USD", 100}, leg{alice, "USD", -99})
	entryCase("one posting", "k2", "", leg{cash, "USD", 100})
	entryCase("zero amount", "k3", "", leg{cash, "USD", 0}, leg{alice, "USD", 0})
	entryCase("same account twice", "k4", "", leg{cash, "USD", 100}, leg{cash, "USD", -100})
	entryCase("unknown currency", "k5", "", leg{cash, "XXX", 100}, leg{alice, "XXX", -100})
	entryCase("description too long", "k6", strings.Repeat("d", 501), leg{cash, "USD", 1}, leg{alice, "USD", -1})
	entryCase("invalid key", "has space", "", leg{cash, "USD", 100}, leg{alice, "USD", -100})
	entryCase("unknown account", "k7", "", leg{cash, "USD", 100}, leg{999999, "USD", -100})
	entryCase("account id zero", "k8", "", leg{cash, "USD", 100}, leg{0, "USD", -100})
	entryCase("currency mismatch", "k9", "", leg{cash, "USD", 100}, leg{eur, "USD", -100})
	entryCase("insufficient funds", "k10", "", leg{alice, "USD", 1001}, leg{cash, "USD", -1001})
	entryCase("key reused", "fund", "", leg{cash, "USD", 1}, leg{alice, "USD", -1})
	account := func(name, path string, id int64) {
		cases = append(cases, struct {
			name string
			rest viaREST
			grpc func() error
		}{name, viaREST{"GET", path, "", ""}, func() error {
			_, err := c.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: id})
			return err
		}})
	}
	account("missing account", "/v1/accounts/999999", 999999)
	account("account zero", "/v1/accounts/0", 0)
	account("negative account", "/v1/accounts/-3", -3)
	history := func(name, query string, id, after int64, size int32) {
		cases = append(cases, struct {
			name string
			rest viaREST
			grpc func() error
		}{name, viaREST{"GET", fmt.Sprintf("/v1/accounts/%d/postings%s", id, query), "", ""}, func() error {
			_, err := c.ListPostings(ctx, &ledgerv1.ListPostingsRequest{AccountId: id, AfterVersion: after,
				PageSize: size})
			return err
		}})
	}
	history("history of a missing account", "", 999999, 0, 0)
	history("page too big", "?limit=1001", alice, 0, 1001)
	history("history of account zero", "", 0, 0, 0)
	newAccount := func(name, body string, req *ledgerv1.CreateAccountRequest) {
		cases = append(cases, struct {
			name string
			rest viaREST
			grpc func() error
		}{name, viaREST{"POST", "/v1/accounts", "", body}, func() error {
			_, err := c.CreateAccount(ctx, req)
			return err
		}})
	}
	newAccount("name taken", `{"name":"wallet:alice","type":"asset","currency":"USD"}`,
		&ledgerv1.CreateAccountRequest{Name: "wallet:alice", Type: ledgerv1.AccountType_ACCOUNT_TYPE_ASSET, Currency: "USD"})
	newAccount("empty name", `{"name":"","type":"asset","currency":"USD"}`,
		&ledgerv1.CreateAccountRequest{Type: ledgerv1.AccountType_ACCOUNT_TYPE_ASSET, Currency: "USD"})
	newAccount("no type", `{"name":"x","currency":"USD"}`, &ledgerv1.CreateAccountRequest{Name: "x", Currency: "USD"})
	newAccount("unknown currency for an account", `{"name":"x","type":"asset","currency":"usd"}`,
		&ledgerv1.CreateAccountRequest{Name: "x", Type: ledgerv1.AccountType_ACCOUNT_TYPE_ASSET, Currency: "usd"})

	for _, tc := range cases {
		req := httptest.NewRequest(tc.rest.method, tc.rest.path, strings.NewReader(tc.rest.body))
		if tc.rest.key != "" {
			req.Header.Set("Idempotency-Key", tc.rest.key)
		}
		rec := httptest.NewRecorder()
		rest.ServeHTTP(rec, req)
		var body struct {
			Error struct {
				Reason  string
				Posting *int
			}
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || rec.Code < 400 {
			t.Errorf("%s: REST answered %d %s", tc.name, rec.Code, rec.Body.String())
			continue
		}
		restPosting := ""
		if body.Error.Posting != nil {
			restPosting = strconv.Itoa(*body.Error.Posting)
		}
		_, grpcReason, grpcPosting := failure(t, tc.grpc())
		if strings.ToUpper(body.Error.Reason) != grpcReason || restPosting != grpcPosting {
			t.Errorf("%s: REST says %s (posting %q), gRPC says %s (posting %q)", tc.name, body.Error.Reason,
				restPosting, grpcReason, grpcPosting)
		}
		if rec.Code == http.StatusInternalServerError {
			t.Errorf("%s: a 500 for a client's mistake", tc.name)
		}
	}
}
