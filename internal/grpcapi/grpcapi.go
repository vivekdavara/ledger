// Package grpcapi is the ledger's gRPC layer: ledger.v1.LedgerService (proto/ledger/v1/ledger.proto) over the same
// ledger.Service as the REST API. It only translates: messages to the ledger's types and back, and errors to a
// status through the shared table in internal/apierr.
package grpcapi

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/vivekdavara/ledger/internal/apierr"
	ledgerv1 "github.com/vivekdavara/ledger/internal/gen/ledger/v1"
	"github.com/vivekdavara/ledger/internal/ledger"
	"github.com/vivekdavara/ledger/internal/store"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ErrorDomain is the domain of every google.rpc.ErrorInfo the service returns.
const ErrorDomain = "ledger.vivekdavara.github.com"

// maxMessage bounds a request. The largest valid entry (64 postings, a 500-character description) is under 4 KB.
const maxMessage = 64 << 10

// MaxCallTime bounds each call, as RequestTimeout does each REST request: a client's own deadline applies when it
// is sooner, and a call without one gets this.
const MaxCallTime = 10 * time.Second

// New returns a gRPC server with LedgerService and server reflection (for grpcurl) registered.
func New(svc ledger.Service, log *slog.Logger) *grpc.Server { return newServer(svc, log, MaxCallTime) }

func newServer(svc ledger.Service, log *slog.Logger, maxCall time.Duration) *grpc.Server {
	bound := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		ctx, cancel := context.WithTimeout(ctx, maxCall) // keeps the client's deadline if that is sooner
		defer cancel()
		return next(ctx, req)
	}
	s := grpc.NewServer(grpc.MaxRecvMsgSize(maxMessage), grpc.UnaryInterceptor(bound))
	ledgerv1.RegisterLedgerServiceServer(s, &service{svc: svc, log: log})
	reflection.Register(s)
	return s
}

type service struct {
	ledgerv1.UnimplementedLedgerServiceServer
	svc ledger.Service
	log *slog.Logger
}

var (
	typeToProto = map[ledger.AccountType]ledgerv1.AccountType{
		ledger.Asset:     ledgerv1.AccountType_ACCOUNT_TYPE_ASSET,
		ledger.Liability: ledgerv1.AccountType_ACCOUNT_TYPE_LIABILITY,
		ledger.Equity:    ledgerv1.AccountType_ACCOUNT_TYPE_EQUITY,
		ledger.Revenue:   ledgerv1.AccountType_ACCOUNT_TYPE_REVENUE,
		ledger.Expense:   ledgerv1.AccountType_ACCOUNT_TYPE_EXPENSE,
	}
	typeFromProto = func() map[ledgerv1.AccountType]ledger.AccountType {
		m := make(map[ledgerv1.AccountType]ledger.AccountType, len(typeToProto))
		for k, v := range typeToProto {
			m[v] = k
		}
		return m
	}()
)

func (s *service) CreateAccount(ctx context.Context, req *ledgerv1.CreateAccountRequest) (
	*ledgerv1.CreateAccountResponse, error) {
	typ, ok := typeFromProto[req.GetType()]
	if !ok {
		// An unset or unknown enum value: let the validator say so, in the same words as REST.
		typ = ledger.AccountType(strings.ToLower(strings.TrimPrefix(req.GetType().String(), "ACCOUNT_TYPE_")))
	}
	acct, created, err := s.svc.CreateAccount(ctx, ledger.NewAccount{Name: req.GetName(), Type: typ,
		Currency: req.GetCurrency(), AllowNegative: req.GetAllowNegative()})
	if err != nil {
		return nil, s.status(err)
	}
	return &ledgerv1.CreateAccountResponse{Account: toAccount(acct), Created: created}, nil
}

func (s *service) GetAccount(ctx context.Context, req *ledgerv1.GetAccountRequest) (*ledgerv1.GetAccountResponse, error) {
	acct, err := s.svc.Account(ctx, req.GetId())
	if err != nil {
		return nil, s.status(err)
	}
	return &ledgerv1.GetAccountResponse{Account: toAccount(acct)}, nil
}

func (s *service) ListPostings(ctx context.Context, req *ledgerv1.ListPostingsRequest) (
	*ledgerv1.ListPostingsResponse, error) {
	lines, err := s.svc.History(ctx, req.GetAccountId(), req.GetAfterVersion(), int(req.GetPageSize()))
	if err != nil {
		return nil, s.status(err)
	}
	out := &ledgerv1.ListPostingsResponse{Postings: make([]*ledgerv1.HistoryLine, len(lines))}
	for i, l := range lines {
		p := l.Posting
		out.Postings[i] = &ledgerv1.HistoryLine{PostingId: p.ID, EntryId: p.EntryID, Description: l.Description,
			PostedAt: timestamppb.New(l.PostedAt), Currency: p.Currency, Amount: p.Amount,
			BalanceAfter: p.BalanceAfter, AccountVersion: p.AccountVersion}
	}
	size := int(req.GetPageSize())
	if size == 0 {
		size = store.DefaultPageSize
	}
	if n := len(lines); n > 0 && n == size {
		next := lines[n-1].Posting.AccountVersion
		out.NextAfterVersion = &next
	}
	return out, nil
}

func (s *service) PostEntry(ctx context.Context, req *ledgerv1.PostEntryRequest) (*ledgerv1.PostEntryResponse, error) {
	e := ledger.Entry{Description: req.GetDescription(), Postings: make([]ledger.Posting, len(req.GetPostings()))}
	for i, p := range req.GetPostings() {
		e.Postings[i] = ledger.Posting{AccountID: p.GetAccountId(), Currency: p.GetCurrency(), Amount: p.GetAmount()}
	}
	posted, replayed, err := s.svc.PostEntry(ctx, req.GetIdempotencyKey(), e)
	if err != nil {
		return nil, s.status(err)
	}
	out := &ledgerv1.Entry{Id: posted.ID, Description: posted.Description, CreatedAt: timestamppb.New(posted.CreatedAt),
		Postings: make([]*ledgerv1.PostedPosting, len(posted.Postings))}
	for i, p := range posted.Postings {
		out.Postings[i] = &ledgerv1.PostedPosting{Id: p.ID, AccountId: p.AccountID, Currency: p.Currency,
			Amount: p.Amount, BalanceAfter: p.BalanceAfter, AccountVersion: p.AccountVersion}
	}
	return &ledgerv1.PostEntryResponse{Entry: out, Replayed: replayed}, nil
}

func toAccount(a ledger.Account) *ledgerv1.Account {
	return &ledgerv1.Account{Id: a.ID, Name: a.Name, Type: typeToProto[a.Type], Currency: a.Currency,
		AllowNegative: a.AllowNegative, Balance: a.Balance, ShownBalance: a.Shown(), Version: a.Version,
		CreatedAt: timestamppb.New(a.CreatedAt)}
}

// status turns an error into a gRPC status with the code from the shared table and an ErrorInfo carrying the
// same reason the REST API gives, in capitals.
func (s *service) status(err error) error {
	kind := apierr.Classify(err)
	msg := err.Error()
	if kind.Redact {
		s.log.Error("call failed", "err", err)
		msg = kind.GRPC.String()
	}
	info := &errdetails.ErrorInfo{Reason: strings.ToUpper(kind.Reason), Domain: ErrorDomain}
	if i := apierr.Posting(err); i >= 0 {
		info.Metadata = map[string]string{"posting": strconv.Itoa(i)}
	}
	st, detailErr := status.New(kind.GRPC, msg).WithDetails(info)
	if detailErr != nil {
		return status.Error(kind.GRPC, msg) // an ErrorInfo always marshals; this is only a fallback
	}
	return st.Err()
}
