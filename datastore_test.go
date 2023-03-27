package datastore

import (
	"reflect"
	"strconv"
	"testing"
)

func TestCreateAndRead(t *testing.T) {
	tests := []struct {
		key   string
		value []byte
	}{
		{
			"abc",
			[]byte(`{"name": "apple"}`),
		},
		{
			"bcd",
			[]byte(`{"name": "orange"}`),
		},
		{
			"cde",
			[]byte(`{"name": "pear"}`),
		},
		{
			"def",
			[]byte(`{"name": "star fruit"}`),
		},
	}

	for _, v := range tests {
		_, err := Put(v.key, v.value)
		if err != nil {
			t.Fatal(err)
		}
		r, err := Get(v.key)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(v.value, r) {
			t.Errorf("Expected %v, Got %v", v.value, r)
		}
	}

	Stop()
}

func BenchmarkCreateAndRead(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := Put(strconv.Itoa(i), []byte(`{"name": "apple"}`))
		if err != nil {
			b.Fatal(err)
		}
		_, err = Get(strconv.Itoa(i))
		if err != nil {
			b.Fatal(err)
		}
	}
}
