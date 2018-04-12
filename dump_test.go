package mydumper

import (
	"fmt"
	"testing"
)

func TestDumpAllDatabases(t *testing.T) {

	dumper, err := NewDumper("mydumper", "172.18.10.136", 3309, "root", "111111")
	if err != nil {
		t.Error(err)
	}

	err = dumper.Dump()
	if err != nil {
		t.Error(err)
	}

	meta, err := NewMeta("/backup")
	if err != nil {
		t.Error(err)
	}

	err = meta.ReadMetadata()
	if err != nil {
		t.Error(err)
	}

	fmt.Println(meta.StartTimestamp.String())
	fmt.Println(meta.BinLogFileName)
	fmt.Println(meta.BinLogFilePos)
	fmt.Println(meta.BinLogUuid)
	fmt.Println(meta.EndTimestamp.String())

}

func TestRegexDump(t *testing.T) {

	dumper, err := NewDumper("mydumper", "172.18.10.136", 3309, "root", "111111")
	if err != nil {
		t.Error(err)
	}

	dumper.SetRegex("^(?!(mysql|test))")

	err = dumper.Dump()
	if err != nil {
		t.Error(err)
	}

	meta, err := NewMeta("/backup")
	if err != nil {
		t.Error(err)
	}

	err = meta.ReadMetadata()
	if err != nil {
		t.Error(err)
	}

	fmt.Println(meta.StartTimestamp.String())
	fmt.Println(meta.BinLogFileName)
	fmt.Println(meta.BinLogFilePos)
	fmt.Println(meta.BinLogUuid)
	fmt.Println(meta.EndTimestamp.String())
}

func TestDumpOneDatabase(t *testing.T) {

	dumper, err := NewDumper("mydumper", "172.18.10.136", 3309, "root", "111111")
	if err != nil {
		t.Error(err)
	}

	dumper.AddDatabase("dev")

	err = dumper.Dump()
	if err != nil {
		t.Error(err)
	}

	meta, err := NewMeta("/backup")
	if err != nil {
		t.Error(err)
	}

	err = meta.ReadMetadata()
	if err != nil {
		t.Error(err)
	}

	fmt.Println(meta.StartTimestamp.String())
	fmt.Println(meta.BinLogFileName)
	fmt.Println(meta.BinLogFilePos)
	fmt.Println(meta.BinLogUuid)
	fmt.Println(meta.EndTimestamp.String())
}

func TestDumpSomeTables(t *testing.T) {

	dumper, err := NewDumper("mydumper", "172.18.10.136", 3309, "root", "111111")
	if err != nil {
		t.Error(err)
	}

	dumper.AddDatabase("dev")
	dumper.AddTables("t1", "t2", "t3")

	err = dumper.Dump()
	if err != nil {
		t.Error(err)
	}

	meta, err := NewMeta("/backup")
	if err != nil {
		t.Error(err)
	}

	err = meta.ReadMetadata()
	if err != nil {
		t.Error(err)
	}

	fmt.Println(meta.StartTimestamp.String())
	fmt.Println(meta.BinLogFileName)
	fmt.Println(meta.BinLogFilePos)
	fmt.Println(meta.BinLogUuid)
	fmt.Println(meta.EndTimestamp.String())
}
