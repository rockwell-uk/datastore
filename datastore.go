package datastore

import (
	"fmt"
)

type sAction struct {
	key    string
	value  interface{}
	action string
	rc     chan sResult
}

type sResult struct {
	val interface{}
	err error
}

const (
	DS_OP_STORE  = "PUT"
	DS_OP_READ   = "GET"
	DS_OP_DELETE = "DELETE"
)

var (
	ds   = make(map[string]interface{})
	ops  = make(chan sAction)
	ctrl = make(chan struct{}, 1)
)

func Stop() {
	// Signal close
	ctrl <- struct{}{}
}

func init() {
	go func() {
		for f := range ops {
			switch f.action {
			case DS_OP_STORE:
				f.rc <- store(f.key, f.value)
			case DS_OP_READ:
				f.rc <- read(f.key)
			case DS_OP_DELETE:
				f.rc <- del(f.key)
			}
		}
	}()
}

func Get(k string) (interface{}, error) {
	res := make(chan sResult, 1)

	ops <- sAction{
		key:    k,
		action: DS_OP_READ,
		rc:     res,
	}
	r := <-res

	return r.val, r.err
}

func Put(k string, v interface{}) (interface{}, error) {
	res := make(chan sResult, 1)

	ops <- sAction{
		key:    k,
		value:  v,
		action: DS_OP_STORE,
		rc:     res,
	}
	r := <-res

	return r.val, r.err
}

func Delete(k string) (interface{}, error) {
	res := make(chan sResult, 1)

	ops <- sAction{
		key:    k,
		action: DS_OP_DELETE,
		rc:     res,
	}
	r := <-res

	return r.val, r.err
}

func store(k string, v interface{}) sResult {
	ds[k] = v

	return sResult{val: v, err: nil}
}

func read(k string) sResult {
	v, ok := ds[k]
	if !ok {
		return sResult{
			val: v,
			err: fmt.Errorf("key '%v' does not exist", k),
		}
	}

	return sResult{val: v, err: nil}
}

func del(k string) sResult {
	delete(ds, k)

	return sResult{val: []byte(k), err: nil}
}
