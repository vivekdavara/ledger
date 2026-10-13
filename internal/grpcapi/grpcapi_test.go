package grpcapi_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"

	ledgerv1 "github.com/vivekdavara/ledger/internal/gen/ledger/v1"
	"github.com/vivekdavara/ledger/internal/grpcapi"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// client starts the gRPC server over an in-memory connection on a fresh schema and returns a client for it.
func client(t *testing.T) ledgerv1.LedgerServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpcapi.New(store.New(storetest.Migrated(t)), quiet)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///ledger",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return ledgerv1.NewLedgerServiceClient(conn)
}

func open(t *testing.T, c ledgerv1.LedgerServiceClient, name string, typ ledgerv1.AccountType, currency string) int64 {
	t.Helper()
	r, err := c.CreateAccount(context.Background(), &ledgerv1.CreateAccountRequest{Name: name, Type: typ, Currency: currency})
	if err != nil || !r.GetCreated() {
		t.Fatalf("create %s: %v, %v", name, r, err)
	}
	return r.GetAccount().GetId()
}

func posting(account int64, amount int64) *ledgerv1.Posting {
	return &ledgerv1.Posting{AccountId: account, Currency: "USD", Amount: amount}
}

// failure is a call's gRPC code, its ErrorInfo reason and the posting it names ("" for none).
func failure(t *testing.T, err error) (codes.Code, string, string) {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok || err == nil {
		t.Fatalf("not a status error: %v", err)
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			if info.GetDomain() != grpcapi.ErrorDomain {
				t.Fatalf("domain %q", info.GetDomain())
			}
			return st.Code(), info.GetReason(), info.GetMetadata()["posting"]
		}
	}
	t.Fatalf("no ErrorInfo in %v", st)
	return 0, "", ""
}

func TestGRPCPostsAndPages(t *testing.T) {
	t.Parallel()
	c := client(t)
	ctx := context.Background()
	cash := open(t, c, "cash:bank", ledgerv1.AccountType_ACCOUNT_TYPE_ASSET, "USD")
	alice := open(t, c, "wallet:alice", ledgerv1.AccountType_ACCOUNT_TYPE_LIABILITY, "USD")
	bob := open(t, c, "wallet:bob", ledgerv1.AccountType_ACCOUNT_TYPE_LIABILITY, "USD")
	fees := open(t, c, "revenue:fees", ledgerv1.AccountType_ACCOUNT_TYPE_REVENUE, "USD")

	again, err := c.CreateAccount(ctx, &ledgerv1.CreateAccountRequest{Name: "wallet:alice",
		Type: ledgerv1.AccountType_ACCOUNT_TYPE_LIABILITY, Currency: "USD"})
	if err != nil || again.GetCreated() || again.GetAccount().GetId() != alice {
		t.Fatalf("create again: %v, %v", again, err)
	}
	if _, err := c.PostEntry(ctx, &ledgerv1.PostEntryRequest{IdempotencyKey: "top-up-1", Description: "top up",
		Postings: []*ledgerv1.Posting{posting(cash, 5000), posting(alice, -5000)}}); err != nil {
		t.Fatal(err)
	}
	pay := &ledgerv1.PostEntryRequest{IdempotencyKey: "pay-1", Description: "Alice pays Bob $20, 3% fee",
		Postings: []*ledgerv1.Posting{posting(fees, -60), posting(alice, 2000), posting(bob, -1940)}}
	first, err := c.PostEntry(ctx, pay)
	if err != nil || first.GetReplayed() {
		t.Fatalf("pay: %v, %v", first, err)
	}
	// In account-id order, whatever the request's order.
	got := first.GetEntry().GetPostings()
	if len(got) != 3 || got[0].GetAccountId() != alice || got[0].GetBalanceAfter() != -3000 ||
		got[0].GetAccountVersion() != 2 || got[2].GetAccountId() != fees {
		t.Fatalf("postings %v", got)
	}
	replay, err := c.PostEntry(ctx, pay)
	if err != nil || !replay.GetReplayed() || replay.GetEntry().GetId() != first.GetEntry().GetId() {
		t.Fatalf("replay: %v, %v", replay, err)
	}
	a, err := c.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: alice})
	if err != nil || a.GetAccount().GetBalance() != -3000 || a.GetAccount().GetShownBalance() != 3000 ||
		a.GetAccount().GetType() != ledgerv1.AccountType_ACCOUNT_TYPE_LIABILITY {
		t.Fatalf("alice: %v, %v", a, err)
	}
	page, err := c.ListPostings(ctx, &ledgerv1.ListPostingsRequest{AccountId: alice, PageSize: 1})
	if err != nil || len(page.GetPostings()) != 1 || page.NextAfterVersion == nil || page.GetNextAfterVersion() != 1 {
		t.Fatalf("page 1: %v, %v", page, err)
	}
	page, err = c.ListPostings(ctx, &ledgerv1.ListPostingsRequest{AccountId: alice, AfterVersion: 1})
	if err != nil || len(page.GetPostings()) != 1 || page.NextAfterVersion != nil ||
		page.GetPostings()[0].GetDescription() != pay.GetDescription() {
		t.Fatalf("page 2: %v, %v", page, err)
	}
}

func TestGRPCErrorsCarryTheReason(t *testing.T) {
	t.Parallel()
	c := client(t)
	ctx := context.Background()
	cash := open(t, c, "cash:bank", ledgerv1.AccountType_ACCOUNT_TYPE_ASSET, "USD")
	alice := open(t, c, "wallet:alice", ledgerv1.AccountType_ACCOUNT_TYPE_LIABILITY, "USD")
	if _, err := c.PostEntry(ctx, &ledgerv1.PostEntryRequest{IdempotencyKey: "fund",
		Postings: []*ledgerv1.Posting{posting(cash, 1000), posting(alice, -1000)}}); err != nil {
		t.Fatal(err)
	}
	entry := func(key string, postings ...*ledgerv1.Posting) func() error {
		return func() error {
			_, err := c.PostEntry(ctx, &ledgerv1.PostEntryRequest{IdempotencyKey: key, Postings: postings})
			return err
		}
	}
	cases := []struct {
		name    string
		call    func() error
		code    codes.Code
		reason  string
		posting string
	}{
		{"unbalanced", entry("k1", posting(cash, 100), posting(alice, -99)), codes.InvalidArgument, "UNBALANCED", ""},
		{"no key", entry("", posting(cash, 100), posting(alice, -100)), codes.InvalidArgument, "INVALID_ARGUMENT", ""},
		{"unknown account", entry("k2", posting(cash, 100), posting(999999, -100)), codes.NotFound,
			"UNKNOWN_ACCOUNT", "1"},
		{"overdraft", entry("k3", posting(alice, 1001), posting(cash, -1001)), codes.FailedPrecondition,
			"INSUFFICIENT_FUNDS", "0"},
		{"key reused", entry("fund", posting(cash, 1), posting(alice, -1)), codes.FailedPrecondition,
			"IDEMPOTENCY_KEY_REUSED", ""},
		{"missing account", func() error {
			_, err := c.GetAccount(ctx, &ledgerv1.GetAccountRequest{Id: 999999})
			return err
		}, codes.NotFound, "NOT_FOUND", ""},
		{"name taken", func() error {
			_, err := c.CreateAccount(ctx, &ledgerv1.CreateAccountRequest{Name: "wallet:alice",
				Type: ledgerv1.AccountType_ACCOUNT_TYPE_ASSET, Currency: "USD"})
			return err
		}, codes.AlreadyExists, "ACCOUNT_NAME_TAKEN", ""},
		{"no type", func() error {
			_, err := c.CreateAccount(ctx, &ledgerv1.CreateAccountRequest{Name: "x", Currency: "USD"})
			return err
		}, codes.InvalidArgument, "INVALID_ARGUMENT", ""},
		{"bad page size", func() error {
			_, err := c.ListPostings(ctx, &ledgerv1.ListPostingsRequest{AccountId: alice, PageSize: -1})
			return err
		}, codes.InvalidArgument, "INVALID_ARGUMENT", ""},
	}
	for _, tc := range cases {
		code, reason, posting := failure(t, tc.call())
		if code != tc.code || reason != tc.reason || posting != tc.posting {
			t.Errorf("%s: %v %s posting %q; want %v %s posting %q", tc.name, code, reason, posting, tc.code,
				tc.reason, tc.posting)
		}
	}
	// The message is the ledger's own, as in REST.
	_, err := c.PostEntry(ctx, &ledgerv1.PostEntryRequest{IdempotencyKey: "k9",
		Postings: []*ledgerv1.Posting{posting(alice, 1001), posting(cash, -1001)}})
	if !strings.Contains(status.Convert(err).Message(), "has 10.00 USD, the posting takes 10.01") {
		t.Fatalf("message %q", status.Convert(err).Message())
	}
}
