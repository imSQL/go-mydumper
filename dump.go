package mydumper

import (
	"os/exec"
	"runtime"
	"strconv"

	"github.com/juju/errors"
)

type (
	Dumper struct {
		// mydumper path. default is /usr/bin/mydumper
		ExecutionPath string `json:"execution_path" db:"execution_path"`

		// mysql database information.
		Addr     string `json:"addr" db:"addr"`
		Port     string `json:"port" db:"port"`
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
		StatementSize string `json:"statement_size" db:"statement_size"`
		// Try to split tables into chunks of this many rows.
		Rows string `json:"rows" db:"rows"`
		// Split tables into chunks of this output file size. unit is MB.
		ChunkFilesize string `json:"chunk_filesize" db:"chunk_filesize"`
		// compress output files. default disable
		Compress string `json:"compress" db:"compress"`
		// enable daemon mode.
		Daemon string `json:"daemon" db:"daemon"`
		// Set long query timer in seconds. default 60
		LongQueryGuard string `json:"long_query_guard" db:"long_query_guard"`
		// kill long running queries.
		KillLongQueries string `json:"kill_long_queries" db:"kill_long_queries"`
		// Interval between each dump snapshot( in minutes).requires --daemon,default 60
		SnapshotInterval string `json:"snapshot_interval" db:"snapshot_interval"`
		// print messages to logfile.
		LogFile string `json:"log_file" db:"log_file"`
		// SET TIME_ZONE='+00:00'
		UtcTimeZone string `json:"utc_timezone" db:"utc_timezone"`
		// Disable SET TIME_ZONE statement.
		SkipUtcTimeZone string `json:"skip_utc_tz" db:"skip_utc_tz"`
		// Use savepoints to reduce metadata locking issues. needs SUPER privileges.
		UseSavePoints string `json:"use_savepoints" db:"use_savepoints"`
		// Not increment error count and waring instead of critical in case of table doesn't exist.
		SuccessOn1146 string `json:"success_on_1146" db:"success_on_1146"`
		// use lock table for all.
		LockAllTables string `json:"lock_all_tables" db:"lock_all_tables"`
		// Use Update_time to dump only tables updated in the last n days.
		UpdatedSince string `json:"update_since" db:"update_since"`
		// Transactional consistency only.
		TrxConsistencyOnly string `json:"trx_consistency_only" db:"trx_consistency_only"`
		// Use complete INSERT statements that include column names.
		CompleteInsert string `json:"complete_insert" db:"complete_insert"`
		// number of threads,default 4
		Threads string `json:"threads" db:"threads"`
		// Use compress on the mysql connection.
		CompressProtocol string `json:"compress_protocol" db:"compress_protocol"`

		// dump table schemas with the data
		ExportSchemas string `json:"export_schemas" db:"export_schemas"`
		// dump table data
		ExportDatas string `json:"export_datas" db:"export_datas"`
		// dump trigger
		ExportTriggers string `json:"export_triggers" db:"export_triggers"`
		// dump events
		ExportEvents string `json:"export_events" db:"export_events"`
		// dump routines
		ExportRoutines string `json:"export_routines" db:"export_routines"`
		// dump views
		ExportViews string `json:"export_views" db:"export_views"`

		NoLock       string `json:"no_lock" db:"no_lock"`
		NoBackupLock string `json:"no_backup_lock" db:"no_backup_lock"`
		LessLock     string `json:"less_locking" db:"less_locking"`
	}
)

// new dumper handler.
func NewDumper(execution_path string, addr string, port string, user string, password string) (*Dumper, error) {
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

	d.OutPutDir = "/backup"

	d.StatementSize = "1000000"
	d.Rows = "0"
	d.ChunkFilesize = "0"
	d.Compress = "0"
	d.Daemon = "0"
	d.LongQueryGuard = "600"
	d.KillLongQueries = "0"
	d.SnapshotInterval = "60"
	d.LogFile = "/backup/backup.log"
	d.UtcTimeZone = "0"
	d.SkipUtcTimeZone = "0"
	d.UseSavePoints = "0"
	d.SuccessOn1146 = "0"
	d.LockAllTables = "0"
	d.UpdatedSince = "0"
	d.TrxConsistencyOnly = "1"
	d.CompleteInsert = "1"
	d.Threads = strconv.Itoa(runtime.NumCPU())
	d.CompressProtocol = "0"

	d.ExportSchemas = "1"
	d.ExportDatas = "1"
	d.ExportTriggers = "1"
	d.ExportEvents = "1"
	d.ExportRoutines = "1"
	d.ExportViews = "1"

	d.NoLock = "0"
	d.NoBackupLock = "0"
	d.LessLock = "0"

	return d, nil
}

// set output dir.
func (d *Dumper) SetOutPutDir(output_dir string) {
	d.OutPutDir = output_dir
}

// set character set.
func (d *Dumper) SetCharacterSet(charset string) {
	d.Charset = charset
}

// set collation.
func (d *Dumper) SetCollation(collation string) {
	d.Collation = collation
}

// set statement size
func (d *Dumper) SetStatementSize(statement_size string) {
	d.StatementSize = statement_size
}

// set rows
func (d *Dumper) SetRows(rows string) {
	d.Rows = rows
}

// set chunk file size
func (d *Dumper) SetChunkFielSize(size string) {
	d.ChunkFilesize = size
}

// set enable/disable compress
func (d *Dumper) SetCompress(enable string) {
	d.Compress = enable
}

// set enable/disable daemon
func (d *Dumper) SetDaemon(enable string) {
	d.Daemon = enable
}

// set long query guard
func (d *Dumper) SetLongQueryGuard(long_query_time string) {
	d.LongQueryGuard = long_query_time
}

// set kill long query
func (d *Dumper) SetKillLongQueries(kill string) {
	d.KillLongQueries = kill
}

// set snapshot interval
func (d *Dumper) SetSnapshotInterval(interval string) {
	d.SnapshotInterval = interval
}

// set log file.
func (d *Dumper) SetLogFile(logfile string) {
	d.LogFile = logfile
}

// set UTC timezone
func (d *Dumper) SetUTCTimeZone(timezone string) {
	d.UtcTimeZone = timezone
}

// set skip timezone
func (d *Dumper) SetSkipUTC(skip string) {
	d.SkipUtcTimeZone = skip
}

// set save points
func (d *Dumper) SetSavePoints(savepoints string) {
	d.UseSavePoints = savepoints
}

// set Success on 1146
func (d *Dumper) SetSuccess1146(success string) {
	d.SuccessOn1146 = success
}

// set Lock all tables.
func (d *Dumper) SetLockAllTables(locktables string) {
	d.LockAllTables = locktables
}

// set update since
func (d *Dumper) SetUpdateSince(update_since string) {
	d.UpdatedSince = update_since
}

// set Trx consistency only
func (d *Dumper) SetTrxConsistencyOnly(trx_consistency_only string) {
	d.TrxConsistencyOnly = trx_consistency_only
}

// set Complete insert
func (d *Dumper) SetCompleteInsert(complete_insert string) {
	d.TrxConsistencyOnly = complete_insert
}

// set threads
func (d *Dumper) SetThreads(threads string) {
	d.Threads = threads
}

// set compress protocol
func (d *Dumper) SetCompressProtocol(compress_protocol string) {
	d.CompressProtocol = compress_protocol
}

// set export schema
func (d *Dumper) SetExportSchema(export string) {
	d.ExportSchemas = export
}

// set export datas
func (d *Dumper) SetExportDatas(export string) {
	d.ExportDatas = export
}

// set export triggers
func (d *Dumper) SetExportTrigger(export string) {
	d.ExportTriggers = export
}

// set export events
func (d *Dumper) SetExportEvents(export string) {
	d.ExportEvents = export
}

// set export Routines
func (d *Dumper) SetExportRoutines(export string) {
	d.ExportRoutines = export
}

// set export Views
func (d *Dumper) SetExportViews(export string) {
	d.ExportViews = export
}

// set nolock
func (d *Dumper) SetNoLock(nolock string) {
	d.NoLock = nolock
}

// set no backup lock.
func (d *Dumper) SetNoBasckupLock(nobackuplock string) {
	d.NoBackupLock = nobackuplock
}

// set no less lock
func (d *Dumper) SetLessLock(lesslock string) {
	d.LessLock = lesslock
}

// execute dump
func (d *Dumper) Dump() error {

	// define arg
	args := make([]string, 0, 16)

	cmd := exec.Command(d.ExecutionPath, args...)
	err := cmd.Run()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
