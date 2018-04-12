package mydumper

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

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

		StartTimestamp time.Time `json:"start_timestamp" db:"start_timestamp"`
		LogFileName    string    `json:"log_filename" db:"log_filename"`
		LogFilePos     uint64    `json:"log_pos" db:"log_pos"`
		LogUuid        string    `json:"log_uuid" db:"log_uuid"`
		EndTimestamp   time.Time `json:"start_timestamp" db:"start_timestamp"`

		// object list.
		Databases []string `json:"databases" db:"databases"`
		Tables    []string `json:"tables" db:"tables"`

		OutPutDir string `json:"output_dir" db:"output_dir"`
		// Attempted size of INSERT statement in bytes.default  1000000
		StatementSize uint64 `json:"statement_size" db:"statement_size"`
		// Try to split tables into chunks of this many rows.
		Rows uint64 `json:"rows" db:"rows"`
		// Split tables into chunks of this output file size. unit is MB.
		ChunkFilesize uint64 `json:"chunk_filesize" db:"chunk_filesize"`
		// compress output files. default disable
		Compress bool `json:"compress" db:"compress"`
		// enable daemon mode.
		Daemon bool `json:"daemon" db:"daemon"`
		// Set long query timer in seconds. default 60
		LongQueryGuard uint64 `json:"long_query_guard" db:"long_query_guard"`
		// kill long running queries.
		KillLongQueries bool `json:"kill_long_queries" db:"kill_long_queries"`
		// Interval between each dump snapshot( in minutes).requires --daemon,default 60
		SnapshotInterval uint64 `json:"snapshot_interval" db:"snapshot_interval"`
		// print messages to logfile.
		LogFile string `json:"log_file" db:"log_file"`
		// SET TIME_ZONE='+00:00'
		UtcTimeZone bool `json:"utc_timezone" db:"utc_timezone"`
		// Disable SET TIME_ZONE statement.
		SkipUtcTimeZone bool `json:"skip_utc_tz" db:"skip_utc_tz"`
		// Use savepoints to reduce metadata locking issues. needs SUPER privileges.
		UseSavePoints bool `json:"use_savepoints" db:"use_savepoints"`
		// Not increment error count and waring instead of critical in case of table doesn't exist.
		SuccessOn1146 bool `json:"success_on_1146" db:"success_on_1146"`
		// use lock table for all.
		LockAllTables bool `json:"lock_all_tables" db:"lock_all_tables"`
		// Use Update_time to dump only tables updated in the last n days.
		UpdatedSince bool `json:"update_since" db:"update_since"`
		// Transactional consistency only.
		TrxConsistencyOnly bool `json:"trx_consistency_only" db:"trx_consistency_only"`
		// Use complete INSERT statements that include column names.
		CompleteInsert bool `json:"complete_insert" db:"complete_insert"`
		// number of threads,default 4
		Threads uint64 `json:"threads" db:"threads"`
		// Use compress on the mysql connection.
		CompressProtocol bool `json:"compress_protocol" db:"compress_protocol"`

		// dump table schemas with the data
		ExportSchemas bool `json:"export_schemas" db:"export_schemas"`
		// dump table data
		ExportDatas bool `json:"export_datas" db:"export_datas"`
		// dump trigger
		ExportTriggers bool `json:"export_triggers" db:"export_triggers"`
		// dump events
		ExportEvents bool `json:"export_events" db:"export_events"`
		// dump routines
		ExportRoutines bool `json:"export_routines" db:"export_routines"`
		// dump views
		ExportViews bool `json:"export_views" db:"export_views"`

		NoLock       bool `json:"no_lock" db:"no_lock"`
		NoBackupLock bool `json:"no_backup_lock" db:"no_backup_lock"`
		LessLock     bool `json:"less_locking" db:"less_locking"`

		//Regular expression for 'db.table' matching
		Regex string `json:"regex" db:"regex"`
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

	d.Databases = make([]string, 0, 16)
	d.Tables = make([]string, 0, 16)
	d.OutPutDir = "/backup"

	d.StatementSize = 1000000
	d.Rows = 1000000
	d.ChunkFilesize = 64
	d.Compress = true
	d.Daemon = false
	d.LongQueryGuard = 600
	d.KillLongQueries = false
	d.SnapshotInterval = 60
	d.LogFile = "stdout"
	d.UtcTimeZone = false
	d.SkipUtcTimeZone = true
	d.UseSavePoints = false
	d.SuccessOn1146 = false
	d.LockAllTables = false
	d.UpdatedSince = false
	d.TrxConsistencyOnly = true
	d.CompleteInsert = true
	d.Threads = uint64(runtime.NumCPU())
	d.CompressProtocol = false

	d.ExportSchemas = true
	d.ExportDatas = true
	d.ExportTriggers = true
	d.ExportEvents = true
	d.ExportRoutines = true
	d.ExportViews = true

	d.NoLock = false
	d.NoBackupLock = false
	d.LessLock = false

	d.Regex = "^(?!(sys))"

	return d, nil
}

// add databases to backup
func (d *Dumper) AddDatabase(dbs ...string) {
	d.Databases = append(d.Databases, dbs...)
}

// add tables to backup
func (d *Dumper) AddTables(tables ...string) error {
	if len(d.Databases) > 0 {
		d.Tables = append(d.Tables, tables...)
	} else {
		return errors.NotValidf("No Database")
	}
	return nil
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
func (d *Dumper) SetStatementSize(statement_size uint64) {
	d.StatementSize = statement_size
}

// set rows
func (d *Dumper) SetRows(rows uint64) {
	d.Rows = rows
}

// set chunk file size
func (d *Dumper) SetChunkFielSize(size uint64) {
	d.ChunkFilesize = size
}

// set enable/disable compress
func (d *Dumper) SetCompress(enable bool) {
	d.Compress = enable
}

// set enable/disable daemon
func (d *Dumper) SetDaemon(enable bool) {
	d.Daemon = enable
}

// set long query guard
func (d *Dumper) SetLongQueryGuard(long_query_time uint64) {
	d.LongQueryGuard = long_query_time
}

// set kill long query
func (d *Dumper) SetKillLongQueries(kill bool) {
	d.KillLongQueries = kill
}

// set snapshot interval
func (d *Dumper) SetSnapshotInterval(interval uint64) {
	d.SnapshotInterval = interval
}

// set log file.
func (d *Dumper) SetLogFile(logfile string) {
	d.LogFile = logfile
}

// set UTC timezone
func (d *Dumper) SetUTCTimeZone(timezone bool) {
	d.UtcTimeZone = timezone
}

// set skip timezone
func (d *Dumper) SetSkipUTC(skip bool) {
	d.SkipUtcTimeZone = skip
}

// set save points
func (d *Dumper) SetSavePoints(savepoints bool) {
	d.UseSavePoints = savepoints
}

// set Success on 1146
func (d *Dumper) SetSuccess1146(success bool) {
	d.SuccessOn1146 = success
}

// set Lock all tables.
func (d *Dumper) SetLockAllTables(locktables bool) {
	d.LockAllTables = locktables
}

// set update since
func (d *Dumper) SetUpdateSince(update_since bool) {
	d.UpdatedSince = update_since
}

// set Trx consistency only
func (d *Dumper) SetTrxConsistencyOnly(trx_consistency_only bool) {
	d.TrxConsistencyOnly = trx_consistency_only
}

// set Complete insert
func (d *Dumper) SetCompleteInsert(complete_insert bool) {
	d.TrxConsistencyOnly = complete_insert
}

// set threads
func (d *Dumper) SetThreads(threads uint64) {
	d.Threads = threads
}

// set compress protocol
func (d *Dumper) SetCompressProtocol(compress_protocol bool) {
	d.CompressProtocol = compress_protocol
}

// set export schema
func (d *Dumper) SetExportSchema(export bool) {
	d.ExportSchemas = export
}

// set export datas
func (d *Dumper) SetExportDatas(export bool) {
	d.ExportDatas = export
}

// set export triggers
func (d *Dumper) SetExportTrigger(export bool) {
	d.ExportTriggers = export
}

// set export events
func (d *Dumper) SetExportEvents(export bool) {
	d.ExportEvents = export
}

// set export Routines
func (d *Dumper) SetExportRoutines(export bool) {
	d.ExportRoutines = export
}

// set export Views
func (d *Dumper) SetExportViews(export bool) {
	d.ExportViews = export
}

// set nolock
func (d *Dumper) SetNoLock(nolock bool) {
	d.NoLock = nolock
}

// set no backup lock.
func (d *Dumper) SetNoBasckupLock(nobackuplock bool) {
	d.NoBackupLock = nobackuplock
}

// set no less lock
func (d *Dumper) SetLessLock(lesslock bool) {
	d.LessLock = lesslock
}

// set regex
func (d *Dumper) SetRegex(regex string) {
	d.Regex = regex
}

// execute dump
func (d *Dumper) Dump() error {

	var out bytes.Buffer
	var stderr bytes.Buffer

	// define arg
	args := make([]string, 0, 61)

	args = append(args, fmt.Sprintf("--host"))
	args = append(args, fmt.Sprintf("%s", d.Addr))
	args = append(args, fmt.Sprintf("--port"))
	args = append(args, fmt.Sprintf("%d", d.Port))
	args = append(args, fmt.Sprintf("--user"))
	args = append(args, fmt.Sprintf("%s", d.User))
	args = append(args, fmt.Sprintf("--password"))
	args = append(args, fmt.Sprintf("%s", d.Password))

	if len(d.Databases) > 0 {
		args = append(args, fmt.Sprintf("--database"))
		args = append(args, fmt.Sprintf("%s", strings.Join(d.Databases, ",")))
	}

	if len(d.Tables) > 0 {
		args = append(args, fmt.Sprintf("--tables-list"))
		args = append(args, fmt.Sprintf("%s", strings.Join(d.Tables, ",")))
	}

	if len(d.OutPutDir) > 0 {
		args = append(args, fmt.Sprintf("--outputdir"))
		args = append(args, fmt.Sprintf("%s", d.OutPutDir))
	} else {
		return errors.NotFoundf("%s", d.OutPutDir)
	}

	if len(d.LogFile) > 0 {
		if strings.Compare(d.LogFile, "stdout") != 0 {
			args = append(args, fmt.Sprintf("--logfile"))
			args = append(args, fmt.Sprintf("%s", d.LogFile))
		}
	}

	args = append(args, fmt.Sprintf("--statement-size"))
	args = append(args, fmt.Sprintf("%d", d.StatementSize))
	args = append(args, fmt.Sprintf("--rows"))
	args = append(args, fmt.Sprintf("%d", d.Rows))
	args = append(args, fmt.Sprintf("--chunk-filesize"))
	args = append(args, fmt.Sprintf("%d", d.ChunkFilesize))

	if d.Compress {
		args = append(args, fmt.Sprintf("--compress"))
	}

	if d.Daemon {
		args = append(args, fmt.Sprintf("--daemon"))
	}

	args = append(args, fmt.Sprintf("--long-query-guard"))
	args = append(args, fmt.Sprintf("%d", d.LongQueryGuard))
	if d.KillLongQueries {
		args = append(args, fmt.Sprintf("--kill-long-queries"))
	}

	args = append(args, fmt.Sprintf("--snapshot-interval"))
	args = append(args, fmt.Sprintf("%d", d.SnapshotInterval))

	if d.UtcTimeZone {
		args = append(args, fmt.Sprintf("--tz-utc"))
	}

	if d.SkipUtcTimeZone {
		args = append(args, fmt.Sprintf("--skip-tz-utc"))
	}

	if d.UseSavePoints {
		args = append(args, fmt.Sprintf("--use-savepoints"))
	}

	if d.SuccessOn1146 {
		args = append(args, fmt.Sprintf("--success-on-1146"))
	}

	if d.LockAllTables {
		args = append(args, fmt.Sprintf("--lock-all-tables"))
	}

	if d.UpdatedSince {
		args = append(args, fmt.Sprintf("--updated-since"))
	}

	if d.TrxConsistencyOnly {
		args = append(args, fmt.Sprintf("--trx-consistency-only"))
	}

	if d.CompleteInsert {
		args = append(args, fmt.Sprintf("--complete-insert"))
	}

	args = append(args, fmt.Sprintf("--threads"))
	args = append(args, fmt.Sprintf("%d", d.Threads))

	if d.CompressProtocol {
		args = append(args, fmt.Sprintf("--compress-protocol"))
	}

	if !d.ExportSchemas {
		args = append(args, fmt.Sprintf("--no-schemas"))
	}

	if !d.ExportDatas {
		args = append(args, fmt.Sprintf("--no-data"))
	}

	if d.ExportTriggers {
		args = append(args, fmt.Sprintf("--triggers"))
	}

	if d.ExportEvents {
		args = append(args, fmt.Sprintf("--events"))
	}

	if d.ExportRoutines {
		args = append(args, fmt.Sprintf("--routines"))
	}

	if !d.ExportViews {
		args = append(args, fmt.Sprintf("--no-views"))
	}

	if len(d.Regex) > 0 {
		args = append(args, fmt.Sprintf("--regex"))
		args = append(args, fmt.Sprintf("%s", d.Regex))
	}

	cmd := exec.Command(d.ExecutionPath, args...)
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// read metadata file
func (d *Dumper) ReadMetadata() error {
	// new buffer
	buf := new(bytes.Buffer)

	// metadata file name.
	meta := fmt.Sprintf("%s/metadata", d.OutPutDir)

	// open a file.
	MetaFd, err := os.Open(meta)
	if err != nil {
		return errors.Trace(err)
	}
	defer MetaFd.Close()

	MetaRd := bufio.NewReader(MetaFd)
	for {
		line, err := MetaRd.ReadBytes('\n')
		if err != nil {
			break
		}

		buf.Write(line)
		fmt.Println(string(buf.Bytes()))

		if strings.Index(string(buf.Bytes()), "Started") != -1 {
			splitbuf := strings.Split(string(buf.Bytes()), ":")
			fmt.Println("start->", splitbuf[1:])
		}
		if strings.Index(string(buf.Bytes()), "Log") != -1 {
			splitbuf := strings.Split(string(buf.Bytes()), ":")
			fmt.Println("log->", splitbuf[1:])
		}
		if strings.Index(string(buf.Bytes()), "Pos") != -1 {
			splitbuf := strings.Split(string(buf.Bytes()), ":")
			fmt.Println("pos->", splitbuf[1:])
		}

		if strings.Index(string(buf.Bytes()), "GTID") != -1 {
			splitbuf := strings.Split(string(buf.Bytes()), ":")
			fmt.Sprintln("gtid->", splitbuf[1:])
		} else {
			fmt.Println(string(buf.Bytes()))
		}
		if strings.Index(string(buf.Bytes()), "Finished") != -1 {
			splitbuf := strings.Split(string(buf.Bytes()), ":")
			fmt.Sprintln("end->", splitbuf[1:])
		} else {
			fmt.Println(string(buf.Bytes()))
		}
		buf.Reset()
	}

	return nil
}
