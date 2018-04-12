package mydumper

import "testing"

func TestLoad(t *testing.T) {

	loader, err := NewLoader("myloader", "172.18.10.136", 3309, "root", "111111")
	if err != nil {
		t.Error(err)
	}

	loader.SetSourceDirectory("/backup")
	loader.SetRestoreDatabase("test")

	err = loader.Load()
	if err != nil {
		t.Error(err)
	}

}
