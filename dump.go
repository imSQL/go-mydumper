package mydumper

import (
	"os/exec"
	"runtime"

	"github.com/juju/errors"
)

type (
	Dumper struct {
		// mydumper path. default is /usr/bin/mydumper
		ExecutionPath string `json:"execution_path" db:"execution_path"`

		// mysql database information.
		Addr     string `json:"addr" db:"addr"`
		Port     uint64 `json:"port" db:"port"`
		User     string `json:"username" db:"username"`
		Password string `json:"password" db:"password"`
		// character default utf8,collation default is utf8_general_ci.
		Charset   string `json:"character" db:"character"`
		Collation string `json:"collation" db:"collation"`

		// object list.
		Databases []string `json:"databases" db:"databases"`
		TableDB   string   `json:"table_database" db:"table_database"`
		Tables    []string `json:"tables" db:"tables"`

		OutPutDir string `json:"output_dir" db:"output_dir"`
		// Attempted size of INSERT statement in bytes.default  1000000
		StatementSize uint64 `json:"statement_size" db:"statement_size"`
		// Try to split tables into chunks of this many rows.
		Rows uint64 `json:"rows" db:"rows"`
		// Split tables into chunks of this output file size. unit is MB.
		ChunkFilesize uint64 `json:"chunk_filesize" db:"chunk_filesize"`
		// compress output files. default disable
		Compress uint64 `json:"compress" db:"compress"`
		// enable daemon mode.
		Daemon uint64 `json:"daemon" db:"daemon"`
		// Set long query timer in seconds. default 60
		LongQueryGuard uint64 `json:"long_query_guard" db:"long_query_guard"`
		// kill long running queries.
		KillLongQueries uint64 `json:"kill_long_queries" db:"kill_long_queries"`
		// Interval between each dump snapshot( in minutes).requires --daemon,default 60
		SnapshotInterval uint64 `json:"snapshot_interval" db:"snapshot_interval"`
		// print messages to logfile.
		LogFile string `json:"log_file" db:"log_file"`
		// SET TIME_ZONE='+00:00'
		UtcTimeZone uint64 `json:"utc_timezone" db:"utc_timezone"`
		// Disable SET TIME_ZONE statement.
		SkipUtcTimeZone uint64 `json:"skip_utc_tz" db:"skip_utc_tz"`
		// Use savepoints to reduce metadata locking issues. needs SUPER privileges.
		UseSavePoints uint64 `json:"use_savepoints" db:"use_savepoints"`
		// Not increment error count and waring instead of critical in case of table doesn't exist.
		SuccessOn1146 uint64 `json:"success_on_1146" db:"success_on_1146"`
		// use lock table for all.
		LockAllTables uint64 `json:"lock_all_tables" db:"lock_all_tables"`
		// Use Update_time to dump only tables updated in the last n days.
		UpdatedSince uint64 `json:"update_since" db:"update_since"`
		// Transactional consistency only.
		TrxConsistencyOnly uint64 `json:"trx_consistency_only" db:"trx_consistency_only"`
		// Use complete INSERT statements that include column names.
		CompleteInsert uint64 `json:"complete_insert" db:"complete_insert"`
		// number of threads,default 4
		Threads uint64 `json:"threads" db:"threads"`
		// Use compress on the mysql connection.
		CompressProtocol uint64 `json:"compress_protocol" db:"compress_protocol"`

		// dump table schemas with the data
		ExportSchemas uint64 `json:"export_schemas" db:"export_schemas"`
		// dump table data
		ExportDatas uint64 `json:"export_datas" db:"export_datas"`
		// dump trigger
		ExportTriggers uint64 `json:"export_triggers" db:"export_triggers"`
		// dump events
		ExportEvents uint64 `json:"export_events" db:"export_events"`
		// dump routines
		ExportRoutines uint64 `json:"export_routines" db:"export_routines"`
		// dump views
		ExportViews uint64 `json:"export_views" db:"export_views"`

		NoLock       uint64 `json:"no_lock" db:"no_lock"`
		NoBackupLock uint64 `json:"no_backup_lock" db:"no_backup_lock"`
		LessLock     uint64 `json:"less_locking" db:"less_locking"`
	}
)

// new dumper handler.
func NewDumper(execution_path string, addr string, port uint64, user string, password string) (*Dumper, error) {
	if len(execution_path) == 0 {
		return nil, errors.NotFoundf("%s Not Exists\n", execution_path)
	}

	path, err := exec.LookPath(execution_path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	d := new(Dumper)
	d.ExecutionPath = path
	d.Addr = addr
	d.Port = port
	d.User = user
	d.Password = password

	d.StatementSize = 1000000
	d.Rows = 0
	d.ChunkFilesize = 0
	d.Compress = 0
	d.Daemon = 0
	d.LongQueryGuard = 600
	d.KillLongQueries = 0
	d.SnapshotInterval = 60
	d.LogFile = "/backup/backup.log"
	d.UtcTimeZone = 0
	d.SkipUtcTimeZone = 0
	d.UseSavePoints = 0
	d.SuccessOn1146 = 0
	d.LockAllTables = 0
	d.UpdatedSince = 0
	d.TrxConsistencyOnly = 1
	d.CompleteInsert = 1
	d.Threads = uint64(runtime.NumCPU())
	d.CompressProtocol = 0

	d.ExportSchemas = 1
	d.ExportDatas = 1
	d.ExportTriggers = 1
	d.ExportEvents = 1
	d.ExportRoutines = 1
	d.ExportViews = 1

	d.NoLock = 0
	d.NoBackupLock = 0
	d.LessLock = 0

	return d, nil
}

// set character set.
func (d *Dumper) SetCharacterSet(charset string) {
	d.Charset = charset
}

// set collation.
func (d *Dumper) SetCollation(collation string) {
	d.Collation = collation
}

//
