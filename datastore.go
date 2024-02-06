package datastore

import (
	"fmt"
)

type Action int

const (
	OP_GET Action = iota
	OP_PUT
	OP_DEL
)

type op struct {
	key    string
	val    interface{}
	action Action
	rc     chan res
}

type res struct {
	val interface{}
	err error
}

var (
	ds   = make(map[string]interface{})
	ops  = make(chan op)
	ctrl = make(chan struct{}, 1)
)

func Stop() {
	ctrl <- struct{}{}
}

func init() {
	go func() {
		select {
		case <-ctrl:
			break
		default:
			for o := range ops {
				switch o.action {
				case OP_GET:
					o.rc <- get(o.key)
				case OP_PUT:
					o.rc <- put(o.key, o.val)
				case OP_DEL:
					o.rc <- del(o.key)
				}
			}
		}
	}()
}

func Get(k string) (interface{}, error) {
	res := make(chan res, 1)

	ops <- op{
		key:    k,
		action: OP_GET,
		rc:     res,
	}
	r := <-res

	return r.val, r.err
}

func Put(k string, v interface{}) (interface{}, error) {
	res := make(chan res, 1)

	ops <- op{
		key:    k,
		val:    v,
		action: OP_PUT,
		rc:     res,
	}
	r := <-res

	return r.val, r.err
}

func Delete(k string) (interface{}, error) {
	res := make(chan res, 1)

	ops <- op{
		key:    k,
		action: OP_DEL,
		rc:     res,
	}
	r := <-res

	return r.val, r.err
}

func get(k string) res {
	if v, ok := ds[k]; ok {
		return res{
			val: v,
		}
	}

	return res{
		err: fmt.Errorf("key %s not found", k),
	}
}

func put(k string, v interface{}) res {
	ds[k] = v
	return res{val: v}
}

func del(k string) res {
	delete(ds, k)
	return res{val: k}
}
