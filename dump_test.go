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

	err = dumper.ReadMetadata()
	if err != nil {
		t.Error(err)
	}

	fmt.Println(dumper.StartTimestamp.String())
	fmt.Println(dumper.BinLogFileName)
	fmt.Println(dumper.BinLogFilePos)
	fmt.Println(dumper.BinLogUuid)
	fmt.Println(dumper.EndTimestamp.String())

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

	err = dumper.ReadMetadata()
	if err != nil {
		t.Error(err)
	}

	fmt.Println(dumper.StartTimestamp.String())
	fmt.Println(dumper.BinLogFileName)
	fmt.Println(dumper.BinLogFilePos)
	fmt.Println(dumper.BinLogUuid)
	fmt.Println(dumper.EndTimestamp.String())
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

	err = dumper.ReadMetadata()
	if err != nil {
		t.Error(err)
	}

	fmt.Println(dumper.StartTimestamp.String())
	fmt.Println(dumper.BinLogFileName)
	fmt.Println(dumper.BinLogFilePos)
	fmt.Println(dumper.BinLogUuid)
	fmt.Println(dumper.EndTimestamp.String())
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

	err = dumper.ReadMetadata()
	if err != nil {
		t.Error(err)
	}

	fmt.Println(dumper.StartTimestamp.String())
	fmt.Println(dumper.BinLogFileName)
	fmt.Println(dumper.BinLogFilePos)
	fmt.Println(dumper.BinLogUuid)
	fmt.Println(dumper.EndTimestamp.String())
}
