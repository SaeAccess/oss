package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/qor/oss/ipfs"
)

func initBadgerIpfs() (*ipfs.Ipfs, error) {
	b := json.RawMessage(`{
		"StorageMax": "10GB",
		"StorageGCWatermark": 90,
		"GCPeriod": "1h",
		"Spec": {
			"type": "measure",
			"prefix": "badger.datastore",
			"child": {
				"type": "badgerds",
				"path": "badgerds",
				"syncWrites": false,
				"truncate": true
			}
		}
	}`)

	ipfs, err := ipfs.New(&ipfs.Config{
		RootPath:  "../tests/ipfs",
		TempDir:   "../tests/ipfs",
		DataStore: b,
	})

	if err != nil {
		return nil, err
	}

	return ipfs, nil
}

func main() {
	ipfs, err := initBadgerIpfs() //initIpfs()
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll("../tests/ipfs")

	oss, err := ipfs.Put("../ipfs/test.dat", nil)
	if err != nil {
		panic(err)
	}

	fmt.Println(oss)

}
