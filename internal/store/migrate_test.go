package store_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivekdavara/ledger/internal/store"
	"github.com/vivekdavara/ledger/internal/store/storetest"
)

func files(m map[string]string) fstest.MapFS {
	fsys := fstest.MapFS{}
	for name, sql := range m {
		fsys[name] = &fstest.MapFile{Data: []byte(sql)}
	}
	return fsys
}

var twoMigrations = map[string]string{
	"0001_widgets.sql": `CREATE TABLE widgets (id bigint PRIMARY KEY);`,
	"0002_gadgets.sql": `CREATE TABLE gadgets (id bigint PRIMARY KEY); INSERT INTO gadgets VALUES (1), (2);`,
}

func migrate(t *testing.T, pool *pgxpool.Pool, m map[string]string) ([]int, error) {
	t.Helper()
	return store.MigrateFS(context.Background(), pool, files(m))
}

func count(t *testing.T, pool *pgxpool.Pool, sql string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func tableExists(t *testing.T, pool *pgxpool.Pool, name string) bool {
	t.Helper()
	return count(t, pool, `SELECT count(*) FROM pg_tables WHERE schemaname = current_schema() AND tablename = '`+
		name+`'`) == 1
}

func TestMigrateAppliesInOrderThenNothing(t *testing.T) {
	t.Parallel()
	pool := storetest.EmptySchema(t)
	applied, err := migrate(t, pool, twoMigrations)
	if err != nil || !slices.Equal(applied, []int{1, 2}) {
		t.Fatalf("first run: applied %v, %v; want [1 2]", applied, err)
	}
	if got := count(t, pool, `SELECT count(*) FROM gadgets`); got != 2 {
		t.Fatalf("gadgets has %d rows, want 2", got)
	}
	applied, err = migrate(t, pool, twoMigrations)
	if err != nil || len(applied) != 0 {
		t.Fatalf("second run: applied %v, %v; want nothing", applied, err)
	}
	if got := count(t, pool, `SELECT count(*) FROM gadgets`); got != 2 {
		t.Fatalf("a second run re-ran a migration: gadgets has %d rows", got)
	}
}

func TestMigrateAppliesOnlyNewFiles(t *testing.T) {
	t.Parallel()
	pool := storetest.EmptySchema(t)
	first := map[string]string{"0001_widgets.sql": twoMigrations["0001_widgets.sql"]}
	if _, err := migrate(t, pool, first); err != nil {
		t.Fatal(err)
	}
	applied, err := migrate(t, pool, twoMigrations)
	if err != nil || !slices.Equal(applied, []int{2}) {
		t.Fatalf("applied %v, %v; want [2]", applied, err)
	}
}

func TestMigrateRefusesAnEditedMigration(t *testing.T) {
	t.Parallel()
	pool := storetest.EmptySchema(t)
	first := map[string]string{"0001_widgets.sql": twoMigrations["0001_widgets.sql"]}
	if _, err := migrate(t, pool, first); err != nil {
		t.Fatal(err)
	}
	edited := map[string]string{
		"0001_widgets.sql": `CREATE TABLE widgets (id bigint PRIMARY KEY, colour text);`,
		"0002_gadgets.sql": twoMigrations["0002_gadgets.sql"],
	}
	applied, err := migrate(t, pool, edited)
	if !errors.Is(err, store.ErrMigrations) || !strings.Contains(err.Error(), "0001_widgets.sql was changed") {
		t.Fatalf("got %v, %v; want an error naming the edited file", applied, err)
	}
	if tableExists(t, pool, "gadgets") {
		t.Fatal("the pending migration ran even though an applied one had changed")
	}
}

func TestMigrateRefusesADatabaseNewerThanTheFiles(t *testing.T) {
	t.Parallel()
	pool := storetest.EmptySchema(t)
	if _, err := migrate(t, pool, twoMigrations); err != nil {
		t.Fatal(err)
	}
	older := map[string]string{"0001_widgets.sql": twoMigrations["0001_widgets.sql"]}
	_, err := migrate(t, pool, older)
	if !errors.Is(err, store.ErrMigrations) || !strings.Contains(err.Error(), "version 0002") {
		t.Fatalf("got %v; want an error about version 0002", err)
	}
}

func TestMigrateFailureLeavesNothingBehind(t *testing.T) {
	t.Parallel()
	pool := storetest.EmptySchema(t)
	broken := map[string]string{
		"0001_widgets.sql": twoMigrations["0001_widgets.sql"],
		"0002_broken.sql":  `CREATE TABLE gadgets (id bigint PRIMARY KEY); SELEC 1;`,
	}
	_, err := migrate(t, pool, broken)
	if err == nil || !strings.Contains(err.Error(), "apply 0002_broken.sql") {
		t.Fatalf("got %v; want an error naming 0002_broken.sql", err)
	}
	// One transaction: the good migration before it, and even schema_migrations, were rolled back.
	for _, table := range []string{"widgets", "gadgets", "schema_migrations"} {
		if tableExists(t, pool, table) {
			t.Errorf("%s exists after a failed run", table)
		}
	}
}

func TestMigrateConcurrentRunnersApplyOnce(t *testing.T) {
	t.Parallel()
	pool := storetest.EmptySchema(t)
	// The sleep holds the lock long enough for every runner to be waiting on it.
	slow := map[string]string{
		"0001_widgets.sql": twoMigrations["0001_widgets.sql"] + ` SELECT pg_sleep(0.3);`,
		"0002_gadgets.sql": twoMigrations["0002_gadgets.sql"],
	}
	const runners = 8
	results := make([][]int, runners)
	errs := make([]error, runners)
	var wg sync.WaitGroup
	for i := range runners {
		wg.Go(func() { results[i], errs[i] = migrate(t, pool, slow) })
	}
	wg.Wait()
	appliedBy := 0
	for i := range runners {
		if errs[i] != nil {
			t.Errorf("runner %d: %v", i, errs[i])
		}
		if len(results[i]) > 0 {
			appliedBy++
			if !slices.Equal(results[i], []int{1, 2}) {
				t.Errorf("runner %d applied %v", i, results[i])
			}
		}
	}
	if appliedBy != 1 {
		t.Fatalf("%d runners applied migrations, want exactly 1", appliedBy)
	}
	if got := count(t, pool, `SELECT count(*) FROM gadgets`); got != 2 {
		t.Fatalf("gadgets has %d rows, want 2", got)
	}
}

func TestLoadMigrationsRejects(t *testing.T) {
	cases := map[string]map[string]string{
		"a gap":         {"0001_a.sql": "", "0003_c.sql": ""},
		"a repeat":      {"0001_a.sql": "", "0001_b.sql": ""},
		"starts at 2":   {"0002_b.sql": ""},
		"version 0":     {"0000_zero.sql": "", "0001_a.sql": ""},
		"no number":     {"init.sql": ""},
		"short number":  {"001_a.sql": ""},
		"capitals":      {"0001_Init.sql": ""},
		"not .sql":      {"0001_a.sql": "", "0002_b.sql.bak": ""},
		"a stray file":  {"0001_a.sql": "", "README": ""},
		"dash for name": {"0001-a.sql": ""},
	}
	for name, m := range cases {
		if _, err := store.LoadMigrations(files(m)); !errors.Is(err, store.ErrMigrations) {
			t.Errorf("%s: got %v, want ErrMigrations", name, err)
		}
	}
}

func TestLoadMigrationsOrdersByVersion(t *testing.T) {
	got, err := store.LoadMigrations(files(map[string]string{
		"0002_b.sql": "two", "0010_j.sql": "ten", "0001_a.sql": "one", "0003_c.sql": "", "0004_d.sql": "",
		"0005_e.sql": "", "0006_f.sql": "", "0007_g.sql": "", "0008_h.sql": "", "0009_i.sql": "",
	}))
	if err != nil {
		t.Fatal(err)
	}
	for i, m := range got {
		if m.Version != i+1 {
			t.Fatalf("position %d holds %s", i, m.Name)
		}
	}
	if got[0].SQL != "one" || got[9].Name != "0010_j.sql" || len(got[0].Checksum) != 64 {
		t.Fatalf("unexpected first or last migration: %+v, %+v", got[0], got[9])
	}
}
