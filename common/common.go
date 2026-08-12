// Package common contains common utilities that are shared among other packages.
// See each sub-package for detail.
package common

import (
	"reflect"

	"github.com/xtls/xray-core/common/errors"
)

// ErrNoClue is for the situation that existing information is not enough to make a decision. For example, Router may return this error when there is no suitable route.
var ErrNoClue = errors.New("not enough information for making a decision")

// Must panics if err is not nil.
func Must(err error) {
	if err != nil {
		panic(err)
	}
}

// Must2 panics if the second parameter is not nil, otherwise returns the first parameter.
// This is useful when function returned "sth, err" and avoid many "if err != nil"
// Internal usage only, if user input can cause err, it must be handled
func Must2[T any](v T, err error) T {
	Must(err)
	return v
}

// Error2 returns the err from the 2nd parameter.
func Error2(v interface{}, err error) error {
	return err
}

// CloseIfExists call obj.Close() if obj is not nil.
func CloseIfExists(obj any) error {
	if obj != nil {
		v := reflect.ValueOf(obj)
		if !v.IsNil() {
			return Close(obj)
		}
	}
	return nil
}
