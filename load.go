package mydumper

import (
	"bytes"
	"fmt"
	"os/exec"
	"runtime"

	"github.com/juju/errors"
)

type (
	Loader struct {
		// execution path
		ExecutionPath string `json:"execution_path" db:"execution_path"`

		// mysql database information.
		Addr     string `json:"addr" db:"addr"`
		Port     uint64 `json:"port" db:"port"`
		User     string `json:"username" db:"username"`
		Password string `json:"password" db:"password"`

		Directory             string `json:"directory" db:"directory"`
		QueriesPerTransaction uint64 `json:"queries_per_transaction" db:"queries_per_transaction"`
		OverwriteTables       bool   `json:"overwrite_tables" db:"overwrite_tables"`
		Database              string `json:"database" db:"database"`
		SourceDB              string `json:"source_db" db:"source_db"`
		EnableBinlog          bool   `json:"enable_binlog" db:"enable_binlog"`
		Threads               uint64 `json:"threads" db:"threads"`
		CompressProtocol      bool   `json:"compress_protocol" db:"compress_protocol"`
	}
)

// new loader handler.
func NewLoader(execution_path string, addr string, port uint64, user string, password string) (*Loader, error) {
	if len(execution_path) == 0 {
		return nil, errors.NotFoundf("%s Not Exists\n", execution_path)
	}

	path, err := exec.LookPath(execution_path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	d := new(Loader)
	d.ExecutionPath = path
	d.Addr = addr
	d.Port = port
	d.User = user
	d.Password = password

	d.Directory = "/backup"
	d.QueriesPerTransaction = 1000
	d.OverwriteTables = true
	d.Database = ""
	d.SourceDB = ""
	d.EnableBinlog = false
	d.Threads = uint64(runtime.NumCPU())
	d.CompressProtocol = false

	return d, nil
}

// set source directory
func (l *Loader) SetSourceDirectory(directory string) {
	l.Directory = directory
}

// set Number of queries per transaction,default 1000
func (l *Loader) SetQueriesPerTrans(queries uint64) {
	l.QueriesPerTransaction = queries
}

// set overwrite tables
func (l *Loader) SetOverwriteTables(overwrite bool) {
	l.OverwriteTables = overwrite
}

// enable/disable binlog
func (l *Loader) SetBinLog(enable bool) {
	l.EnableBinlog = enable
}

// set alternative database to restore into
func (l *Loader) SetAlternativeDatabase(database string) {
	l.Database = database
}

// set database to restore
func (l *Loader) SetRestoreDatabase(database string) {
	l.SourceDB = database
}

//set threads
func (l *Loader) SetThreads(threads uint64) {
	l.Threads = threads
}

// set compression on the mysql connction
func (l *Loader) SetCompressProtocol(compress bool) {
	l.CompressProtocol = compress
}

// execute load
func (l *Loader) Load() error {

	var out bytes.Buffer
	var stderr bytes.Buffer

	// define arg
	args := make([]string, 0, 30)

	args = append(args, fmt.Sprintf("--host"))
	args = append(args, fmt.Sprintf("%s", l.Addr))
	args = append(args, fmt.Sprintf("--port"))
	args = append(args, fmt.Sprintf("%d", l.Port))
	args = append(args, fmt.Sprintf("--user"))
	args = append(args, fmt.Sprintf("%s", l.User))
	args = append(args, fmt.Sprintf("--password"))
	args = append(args, fmt.Sprintf("%s", l.Password))

	args = append(args, fmt.Sprintf("--directory"))
	args = append(args, fmt.Sprintf("%s", l.Directory))

	args = append(args, fmt.Sprintf("--queries-per-transaction"))
	args = append(args, fmt.Sprintf("%d", l.QueriesPerTransaction))

	if l.OverwriteTables {
		args = append(args, fmt.Sprintf("--overwrite-tables"))
	}

	if len(l.Database) > 0 {
		args = append(args, fmt.Sprintf("--database"))
		args = append(args, fmt.Sprintf("%s", l.Database))
	}

	if len(l.SourceDB) > 0 {
		args = append(args, fmt.Sprintf("--source-db"))
		args = append(args, fmt.Sprintf("%s", l.SourceDB))
	}

	if l.EnableBinlog {
		args = append(args, fmt.Sprintf("--enable-binlog"))
	}

	if l.CompressProtocol {
		args = append(args, fmt.Sprintf("--compress-protocol"))
	}

	args = append(args, fmt.Sprintf("--threads"))
	args = append(args, fmt.Sprintf("%d", l.Threads))

	cmd := exec.Command(l.ExecutionPath, args...)
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return errors.Trace(err)
	}
	return nil

}
