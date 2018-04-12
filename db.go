package mydumper

import "time"

// record operations into sqlite.
type (
	Recorder struct {
		Id             uint64    `json:"id" db:"id"`
		Type           uint64    `json:"type" db:"type"`
		Method         uint64    `json:"method" db:"method"`
		State          uint64    `json:"state" db:"state"`
		BackupDir      string    `json:"backup_dir" db:"backup_dir"`
		BinLogFileName string    `json:"binlog_filename" db:"binlog_filename"`
		BinLogFilePos  uint64    `json:"binlog_filepos" db:"binlog_filepos"`
		BinLogUuid     string    `json:"binlog_uuid" db:"binlog_uuid"`
		StartTimestamp time.Time `json:"start_timestamp" db:"start_timestamp"`
		EndTimestamp   time.Time `json:"end_timestamp" db:"end_timestamp"`
	}
)

const (
	StmtSchema = `
	CREATE TABLE IF NOT EXISTS t_data_backup (
		id integer NOT NULL,
		type int NOT NULL,
		method int NOT NULL,
		state int NOT NULL,
		backup_dir varchar(1024) NOT NULL DEFAULT '/backup',
		binlog_filename varchar(1024) NOT NULL DEFAULT 'archlog.000001',
		binlog_filepos int NOT NULL,
		binlog_uuid varchar(512),
		start_timestamp datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
		end_timestamp datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY ('id')
	)
	`
	StmtInsertRecord = `
	INSERT INTO 
		t_data_backup(id,type,method,state,backup_dir,binlog_filename,binlog_filepos,binlog_uuid,start_timestamp,end_timestamp)
	VALUE
		(%d,%d,%d,%d,'%s','%s',%d,'%s','%s','%s')
	`
	StmtDeleteRecord = `
	DELETE FROM t_data_backup WHERE id = %d
	`
	StmtUpdateRecord = `
	UPDATE t_data_backup SET 
		type = %d,
		method = %d,
		state = %d,
		backup_dir = '%s',
		binlog_filename = '%s',
		binlog_filepos = %d,
		binlog_uuid = '%s',
		start_timestamp = '%s',
		end_timestamp = '%s'
	WHERE id = %d
	`
	StmtQueryRecord = `
	SELECT 
		id,type,method,state,backup_dir,binlog_filename,binlog_filepos,binlog_uuid,start_timestamp,end_timestamp
	FROM
		t_data_backup
	`
)
