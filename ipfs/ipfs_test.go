package ipfs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/SaeAccess/oss"
	peer "github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"golang.org/x/net/context"
)

func initIpfs() (*Ipfs, error) {
	ipfs, err := New(&Config{
		RootPath: "../tests/ipfs",
		TempDir:  "../tests/ipfs",
	})

	if err != nil {
		return nil, err
	}

	return ipfs, nil
}

var badgerConfig = json.RawMessage(`{
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
var badgerConfig1 = json.RawMessage(`{
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

func initBadgerIpfs(path string,
	peers []string,
	storeCfg json.RawMessage,
	addr json.RawMessage) (*Ipfs, error) {
	ipfs, err := New(&Config{
		RootPath:  path,
		TempDir:   path,
		Peers:     peers,
		DataStore: storeCfg,
		Addresses: addr,
	})

	if err != nil {
		return nil, err
	}

	return ipfs, nil
}

func connectTo(ipfs *Ipfs, peerAddr string) error {
	addr, err := ma.NewMultiaddr(peerAddr)
	if err != nil {
		fmt.Printf("bad multiaddr, addr: %v", addr)
		return err
	}

	pai, err := peer.AddrInfoFromP2pAddr(addr)
	if err != nil {
		fmt.Printf("failed AddrInfoFromP2pAddr: p2p addr: %v", addr)
		return err
	}

	err = ipfs.coreAPI.Swarm().Connect(context.Background(), *pai)
	if err != nil {
		// TODO get logger
		fmt.Printf("failed to connect to %s: %s", pai.ID, err)
		return err
	}

	return nil
}

func putFile(ipfs *Ipfs, path string) (*oss.Object, []byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	c1, err := ioutil.ReadAll(f)
	f.Seek(0, 0)

	oss, err := ipfs.Put(path, f)
	if err != nil {
		return nil, nil, err
	}

	return oss, c1, nil
}

func getFile(ipfs *Ipfs, cid string) ([]byte, error) {
	f2, err := ipfs.Get(cid)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	defer f2.Close()
	return ioutil.ReadAll(f2)
}

func TestFile(t *testing.T) {
	ipfs, err := initBadgerIpfs("../tests/ipfs", []string{}, badgerConfig, nil) //initIpfs()
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll("../tests/ipfs")

	oss, c1, err := putFile(ipfs, "./test.dat")
	if err != nil {
		t.Error(err)
	}

	//	fmt.Println(oss)

	// Read the file back
	c2, err := getFile(ipfs, oss.Path)
	if err != nil {
		t.Error(err)
	}

	// Compare contents
	if bytes.Compare(c1, c2) != 0 {
		t.Errorf("Put file contents: %v != Get file contents: %v", c1, c2)
	}

	// Remove path
	if err = ipfs.Delete(oss.Path); err != nil {
		t.Errorf("Delete path: %v", err)
	}
}

func TestDir(t *testing.T) {
	ipfs, err := initIpfs()
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll("../tests/ipfs")

	// Add directory
	dir := "../tests/testdir"
	idir, err := ipfs.Put(dir, nil)
	if err != nil {
		t.Error(err)
	}

	fmt.Println(idir)

	// Add items to directory
	t1, err := ipfs.Put(filepath.Join(dir, "test1.txt"), nil)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(t1)

	t2, err := ipfs.Put(filepath.Join(dir, "test2.txt"), nil)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(t2)

	// List the directory
	items, err := ipfs.List(idir.Path)
	if err != nil {
		t.Error(err)
	}

	if len(items) != 2 {
		t.Errorf("Expected 2 directory items, got: %d", len(items))
	}

	fmt.Println("-------------------------------")
	for _, i := range items {
		fmt.Println(i)
	}
}

func TestPeer(t *testing.T) {
	ipfs, err := initBadgerIpfs("../tests/ipfs", []string{}, badgerConfig, nil) //initIpfs()
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll("../tests/ipfs")

	// Get peerId for ipfs node created above.
	nodeID := ipfs.NodeAddr()
	t.Logf("nodeID: %v", nodeID)
	//peers := []string{nodeID}
	peers := []string{}

	// Create another ipfs node (peerIpfs)
	var addr = json.RawMessage(`{
		"Swarm": [
			"/ip4/0.0.0.0/tcp/4002",
			"/ip6/::/tcp/4002",
			"/ip4/0.0.0.0/udp/4002/quic",
			"/ip6/::/udp/4002/quic"
		],
		"Announce": [],
		"NoAnnounce": [],
		"API": ["/ip4/127.0.0.1/tcp/5002"],
		"Gateway": []
	}`)

	peerIpfs, err := initBadgerIpfs("../peers/ipfs", peers, badgerConfig1, addr)
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll("../peers/ipfs")

	oss, c1, err := putFile(ipfs, "./test.dat")
	if err != nil {
		t.Error(err)
	}

	// Get the file from the connected peer
	c2, err := getFile(peerIpfs, oss.Path)
	if err != nil {
		t.Error(err)
	}

	// Compare contents
	if bytes.Compare(c1, c2) != 0 {
		t.Errorf("Put file contents: %v != Get file contents: %v", c1, c2)
	}
}
