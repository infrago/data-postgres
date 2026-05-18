package data_postgres

import (
	"database/sql/driver"
	"errors"
	"reflect"
	"testing"

	. "github.com/infrago/base"
	"github.com/infrago/data"
	"github.com/lib/pq"
)

func TestPostgresDialectBindArrayUsesDriverValuer(t *testing.T) {
	got, ok := (postgresDialect{}).BindValue(Var{Type: "[int]"}, [][]int64{{1, 2}, {3, 4}})
	if !ok {
		t.Fatalf("expected postgres array binding")
	}
	valuer, ok := got.(driver.Valuer)
	if !ok {
		t.Fatalf("expected driver.Valuer, got %T", got)
	}
	raw, err := valuer.Value()
	if err != nil {
		t.Fatalf("unexpected array value error: %v", err)
	}
	if raw != "{{1,2},{3,4}}" {
		t.Fatalf("unexpected array literal: %#v", raw)
	}
}

func TestPostgresDialectBindAndDecodeCommonTypes(t *testing.T) {
	jsonValue, ok := (postgresDialect{}).BindValue(Var{Type: "jsonb"}, Map{"name": "alice"})
	if !ok || jsonValue != `{"name":"alice"}` {
		t.Fatalf("expected json binding, got %#v ok=%v", jsonValue, ok)
	}

	binaryValue, ok := (postgresDialect{}).BindValue(Var{Type: "bytea"}, "hello")
	if !ok || !reflect.DeepEqual(binaryValue, []byte("hello")) {
		t.Fatalf("expected binary binding, got %#v ok=%v", binaryValue, ok)
	}

	decoded, ok := (postgresDialect{}).DecodeValue(Var{Type: "jsonb"}, []byte(`{"name":"alice"}`))
	if !ok {
		t.Fatalf("expected json decode")
	}
	decodedMap, ok := decoded.(map[string]Any)
	if !ok || decodedMap["name"] != "alice" {
		t.Fatalf("unexpected json decode: %#v", decoded)
	}

	arrayValue, ok := (postgresDialect{}).DecodeValue(Var{Type: "[int]"}, "{{1,2},{3,4}}")
	if !ok {
		t.Fatalf("expected array decode")
	}
	arrayItems, ok := arrayValue.([][]int64)
	if !ok || len(arrayItems) != 2 || arrayItems[1][1] != 4 {
		t.Fatalf("unexpected array decode: %T %#v", arrayValue, arrayValue)
	}
}

func TestPostgresDialectClassifiesErrors(t *testing.T) {
	err := &pq.Error{Code: "23505", Message: "duplicate key"}
	got := data.Error("insert", data.ErrInvalidUpdate, (postgresDialect{}).ClassifyError(err))
	if !errors.Is(got, data.ErrDuplicate) {
		t.Fatalf("expected duplicate classification, got %v", got)
	}
	if !errors.Is(got, data.ErrConflict) {
		t.Fatalf("duplicate should be conflict-compatible")
	}
}
