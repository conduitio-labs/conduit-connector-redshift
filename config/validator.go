// Copyright © 2022 Meroxa, Inc. & Yalantis
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/go-playground/validator/v10"
	"go.uber.org/multierr"
)

// keyStructTag is a tag which contains a field's key.
const keyStructTag = "key"

var (
	// validate is a singleton instance of the validator.
	validate *validator.Validate
	once     sync.Once
)

// validateStruct initializes and registers validation tags once and validates struct.
func validateStruct(data any) error {
	once.Do(func() {
		validate = validator.New()
	})

	var err error
	if validationErr := validate.Struct(data); validationErr != nil {
		if errors.Is(validationErr, (*validator.InvalidValidationError)(nil)) {
			return fmt.Errorf("validate struct: %w", validationErr)
		}

		var validationErrs validator.ValidationErrors
		if !errors.As(validationErr, &validationErrs) {
			return nil
		}

		for _, fieldErr := range validationErrs {
			fieldName := getFieldKey(data, fieldErr.StructField())

			switch fieldErr.ActualTag() {
			case "required":
				err = multierr.Append(err, requiredErr(fieldName))
			case "gte":
				err = multierr.Append(err, gteErr(fieldName, fieldErr.Param()))
			case "lte":
				err = multierr.Append(err, lteErr(fieldName, fieldErr.Param()))
			}
		}
	}

	return err //nolint:wrapcheck // since we use multierr here, we don't want to wrap the error
}

// requiredErr returns the formatted required error.
func requiredErr(name string) error {
	return fmt.Errorf("%q value must be set", name)
}

// gteErr returns the formatted gte error.
func gteErr(name, gte string) error {
	return fmt.Errorf("%q value must be greater than or equal to %s", name, gte)
}

// lteErr returns the formatted lte error.
func lteErr(name, lte string) error {
	return fmt.Errorf("%q value must be less than or equal to %s", name, lte)
}

// getFieldKey returns a key ("key" tag) for the provided fieldName.
// If the "key" tag is not present, the function will return a fieldName.
func getFieldKey(data any, fieldName string) string {
	// if the data is not pointer or is nil, return a fieldName.
	val := reflect.ValueOf(data)
	if val.Kind() == reflect.Ptr && !val.IsNil() {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return fieldName
	}

	structField, ok := val.Type().FieldByName(fieldName)
	if !ok {
		return fieldName
	}

	fieldKey := structField.Tag.Get(keyStructTag)
	if fieldKey == "" {
		return fieldName
	}

	return fieldKey
}
