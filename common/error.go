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

package common

import (
	"fmt"
)

type LowercaseError struct {
	fieldName string
}

func (e LowercaseError) Error() string {
	return fmt.Sprintf("%q value must be in lowercase", e.fieldName)
}

func NewLowercaseError(fieldName string) LowercaseError {
	return LowercaseError{fieldName: fieldName}
}

type ExcludesSpacesError struct {
	fieldName string
}

func (e ExcludesSpacesError) Error() string {
	return fmt.Sprintf("%q value must not contain spaces", e.fieldName)
}

func NewExcludesSpacesError(fieldName string) ExcludesSpacesError {
	return ExcludesSpacesError{fieldName: fieldName}
}

type LessThanError struct {
	fieldName string
	value     int
}

func (e LessThanError) Error() string {
	return fmt.Sprintf("%q value must be less than or equal to %d", e.fieldName, e.value)
}

func NewLessThanError(fieldName string, value int) LessThanError {
	return LessThanError{fieldName: fieldName, value: value}
}

type GreaterThanError struct {
	fieldName string
	value     int
}

func (e GreaterThanError) Error() string {
	return fmt.Sprintf("%q value must be greater than or equal to %d", e.fieldName, e.value)
}

func NewGreaterThanError(fieldName string, value int) GreaterThanError {
	return GreaterThanError{fieldName: fieldName, value: value}
}

type NoTablesOrColumnsError struct{}

func (e NoTablesOrColumnsError) Error() string {
	return "at least one table and one corresponding ordering column must be provided"
}

func NewNoTablesOrColumnsError() NoTablesOrColumnsError {
	return NoTablesOrColumnsError{}
}

type MismatchedTablesAndColumnsError struct {
	tablesCount          int
	orderingColumnsCount int
}

func (e MismatchedTablesAndColumnsError) Error() string {
	return fmt.Sprintf("the number of tables (%d) and ordering columns (%d) must match", e.tablesCount, e.orderingColumnsCount)
}

func NewMismatchedTablesAndColumnsError(tablesCount, orderingColumnsCount int) MismatchedTablesAndColumnsError {
	return MismatchedTablesAndColumnsError{tablesCount: tablesCount, orderingColumnsCount: orderingColumnsCount}
}
