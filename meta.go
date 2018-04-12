package mydumper

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
)

type (
	MetaData struct {
		MetaDir        string    `json:"metadir" db:"metadir"`
		StartTimestamp time.Time `json:"start_timestamp" db:"start_timestamp"`
		BinLogFileName string    `json:"log_filename" db:"log_filename"`
		BinLogFilePos  uint64    `json:"log_pos" db:"log_pos"`
		BinLogUuid     string    `json:"log_uuid" db:"log_uuid"`
		EndTimestamp   time.Time `json:"start_timestamp" db:"start_timestamp"`
	}
)

// new metadata
func NewMeta(dir string) (*MetaData, error) {
	m := new(MetaData)

	m.MetaDir = dir
	m.BinLogFileName = "archlog.000001"
	m.BinLogFilePos = 0
	m.BinLogUuid = ""

	return m, nil
}

// read metadata file
func (m *MetaData) ReadMetadata() error {
	// new buffer
	buf := new(bytes.Buffer)

	// metadata file name.
	meta := fmt.Sprintf("%s/metadata", m.MetaDir)

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

		if len(line) > 2 {
			newline := bytes.TrimLeft(line, "")
			buf.Write(bytes.Trim(newline, "\n"))
			line = []byte{}
		}
		if strings.Contains(string(buf.Bytes()), "Started") == true {
			splitbuf := strings.Split(string(buf.Bytes()), ":")
			m.StartTimestamp, _ = time.ParseInLocation("2006-01-02 15:04:05", strings.TrimLeft(strings.Join(splitbuf[1:], ":"), " "), time.Local)
		}
		if strings.Contains(string(buf.Bytes()), "Log") == true {
			splitbuf := strings.Split(string(buf.Bytes()), ":")
			m.BinLogFileName = strings.TrimLeft(strings.Join(splitbuf[1:], ":"), " ")
		}
		if strings.Contains(string(buf.Bytes()), "Pos") == true {
			splitbuf := strings.Split(string(buf.Bytes()), ":")
			pos, _ := strconv.Atoi(strings.TrimLeft(strings.Join(splitbuf[1:], ":"), " "))

			m.BinLogFilePos = uint64(pos)
		}

		if strings.Contains(string(buf.Bytes()), "GTID") == true {
			splitbuf := strings.Split(string(buf.Bytes()), ":")
			m.BinLogUuid = strings.TrimLeft(strings.Join(splitbuf[1:], ":"), " ")
		}
		if strings.Contains(string(buf.Bytes()), "Finished") == true {
			splitbuf := strings.Split(string(buf.Bytes()), ":")
			m.EndTimestamp, _ = time.ParseInLocation("2006-01-02 15:04:05", strings.TrimLeft(strings.Join(splitbuf[1:], ":"), " "), time.Local)
		}
		buf.Reset()
	}

	return nil
}
