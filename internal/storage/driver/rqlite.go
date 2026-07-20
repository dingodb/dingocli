/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the Licensele().
 */

package driver

import (
	"os"
	"strings"
	"sync"

	"github.com/rqlite/gorqlite"
	rqlite "github.com/rqlite/gorqlite"
)

type RQLiteDB struct {
	conn *rqlite.Connection
	sync.Mutex
}

type QueryResult struct {
	result rqlite.QueryResult
}

type WriteResult struct {
	result rqlite.WriteResult
}

var (
	_ IDataBaseDriver = (*RQLiteDB)(nil)
	_ IQueryResult    = (*QueryResult)(nil)
	_ IWriteResult    = (*WriteResult)(nil)
)

func NewRQLiteDB() *RQLiteDB {
	return &RQLiteDB{}
}

func (db *RQLiteDB) Open(url string) error {
	connURL := "http://" + strings.TrimPrefix(url, "rqlite://")

	// Temporarily unset proxy environment variables for rqlite connection
	// since rqlite is an internal service and should not go through proxy
	httpProxy := os.Getenv("http_proxy")
	httpsProxy := os.Getenv("https_proxy")
	noProxy := os.Getenv("no_proxy")

	os.Unsetenv("http_proxy")
	os.Unsetenv("https_proxy")
	os.Unsetenv("HTTP_PROXY")
	os.Unsetenv("HTTPS_PROXY")

	conn, err := gorqlite.Open(connURL)

	// Restore proxy environment variables
	if httpProxy != "" {
		os.Setenv("http_proxy", httpProxy)
	}
	if httpsProxy != "" {
		os.Setenv("https_proxy", httpsProxy)
	}
	if noProxy != "" {
		os.Setenv("no_proxy", noProxy)
	}

	if err != nil {
		return err
	}
	db.conn = conn
	return nil
}

func (db *RQLiteDB) Close() error {
	return nil
}

func (result *QueryResult) Next() bool {
	return result.result.Next()
}

func (result *QueryResult) Scan(dest ...any) error {
	return result.result.Scan(dest...)
}

func (result *QueryResult) Close() error {
	return nil
}

func (db *RQLiteDB) Query(query string, args ...any) (IQueryResult, error) {
	db.Lock()
	defer db.Unlock()

	result, err := db.conn.QueryOneParameterized(
		rqlite.ParameterizedStatement{
			Query:     query,
			Arguments: append([]interface{}{}, args...),
		},
	)
	return &QueryResult{result: result}, err
}

func (result *WriteResult) LastInsertId() (int64, error) {
	return result.result.LastInsertID, nil
}

func (db *RQLiteDB) Write(query string, args ...any) (IWriteResult, error) {
	db.Lock()
	defer db.Unlock()

	result, err := db.conn.WriteOneParameterized(
		rqlite.ParameterizedStatement{
			Query:     query,
			Arguments: append([]interface{}{}, args...),
		},
	)
	return &WriteResult{result: result}, err
}
