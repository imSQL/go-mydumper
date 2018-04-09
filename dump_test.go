package mydumper

import "testing"

func TestDump(t *testing.T) {

	dumper, err := NewDumper("mydumper", "172.18.10.136", 3309, "root", "111111")
	if err != nil {
		t.Error(err)
	}

	err = dumper.Dump()
	if err != nil {
		t.Error(err)
	}

}
