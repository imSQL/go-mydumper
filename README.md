go-mydumper
-----

a mydumper golang library.


###  Requirements

1. Go 1.9
1. CentOS 7
1. mydumper

### Usage

Please install mydumper on your OS before backup.

Execute backup.

	package main
	
	import (
		"log"
		mydumper "github.com/imSQL/go-mydumper"
	)
	
	func main() {
	
		dumper, err := mydumper.NewDumper("mydumper", "172.18.10.136", 3309, "root", "111111")
		if err != nil {
			log.Println(err)
		}
	
		err = dumper.Dump()
		if err != nil {
			log.Println(err)
		}
	}

Execute Restore

	package main
	
	import (
		"log"
		mydumper "github.com/imSQL/go-mydumper"
	)
	
	func main() {
	
		loader, err := mydumper.NewDumper("myloader", "172.18.10.136", 3309, "root", "111111")
		if err != nil {
			log.Println(err)
		}

        loader.SetRestoreDatabase("test")
	
		err = loader.Load()
		if err != nil {
			log.Println(err)
		}
	}

### Donate

-----

If you like the project and want to buy me a cola, you can through:

| PayPal                                                                                                               | 微信                                                                 |
| -------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| [![](https://www.paypalobjects.com/webstatic/paypalme/images/pp_logo_small.png)](https://www.paypal.me/taylor840326) | ![](https://github.com/taylor840326/blog/raw/master/imgs/weixin.png) |