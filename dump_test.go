package mydumper

import "testing"

func TestDumpAllDatabases(t *testing.T) {

	dumper, err := NewDumper("mydumper", "172.18.10.136", 3309, "root", "111111")
	if err != nil {
		t.Error(err)
	}

	err = dumper.Dump()
	if err != nil {
		t.Error(err)
	}

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
}

func TestDumpSomeTables(t *testing.T) {

	dumper, err := NewDumper("mydumper", "172.18.10.136", 3309, "root", "111111")
	if err != nil {
		t.Error(err)
	}

	dumper.AddDatabase("test")
	dumper.AddTables("t1", "t2", "t3")

	err = dumper.Dump()
	if err != nil {
		t.Error(err)
	}

}
