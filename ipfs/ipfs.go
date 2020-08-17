package ipfs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	config "github.com/ipfs/go-ipfs-config"
	files "github.com/ipfs/go-ipfs-files"
	"github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/core/coreapi"
	"github.com/ipfs/go-ipfs/core/node/libp2p"
	"github.com/ipfs/go-ipfs/plugin/loader"
	"github.com/ipfs/go-ipfs/repo/fsrepo"
	icore "github.com/ipfs/interface-go-ipfs-core"
	ipath "github.com/ipfs/interface-go-ipfs-core/path"
	peer "github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/qor/oss"
)

// Config provides configuration for the Ipfs node.
type Config struct {
	RootPath              string `json:"root_path,omitempty"`
	Networking            bool   `json:"networking,omitempty"`
	EncryptedConnnections bool   `json:"encrypted_connections,omitempty"`

	// Default peers to connect to in multiaddr string format
	Peers []string `json:"peers,omitempty"`

	// Type of node (full, client, server)
	NodeType string `json:"node_type,omitempty"`

	// Temp dir for receiving files with Get() operation
	TempDir string `json:"temp_dir,omitempty"`

	// Datastore config
	DataStore json.RawMessage `json:"datastore,omitempty"`

	// Address config - same format as ipfs addresses config
	Addresses json.RawMessage `json:"addresses,omitempty"`

	// Private network flag, indicates will have to generate swarm.key and
	// put it into the repo path
	// TODO should this be the actual swarm key, or could be a file to load
	// or the key used to access vault or some other kind of secret store.
	// The key should be writen to file swarm.key in the ipfs repo path
	PrivateNetwork bool `json:"private_network,omitempty"`

	// The storage strategy 'fs' (filesystem) or 'os' (object store)
	// defaults to 'os' object store.
	//StorageType string `json:"store,omitempty"`

	// TODO other raw message types for ipfs specific configuration, eg. Addresses

}

// Ipfs provides storage interface using IPFS
type Ipfs struct {
	// ipfs core api
	coreAPI icore.CoreAPI

	// Configuration
	config *Config

	// ipfs node
	ipfsNode *core.IpfsNode
}

// New creates a new Ipfs instance
func New(cfg *Config) (*Ipfs, error) {
	ipfs := &Ipfs{config: cfg}

	// Check root path
	if err := ensureDir(cfg.RootPath); err != nil {
		return nil, err
	}

	// Check temp dir path
	if err := ensureDir(cfg.TempDir); err != nil {
		return nil, err
	}

	// Create ipfs node base on configuration
	if err := ipfs.setupPlugins(cfg.RootPath); err != nil {
		return nil, err
	}

	// Initialize the default ipfs config
	// Create a config with default options and a 2048 bit key
	defaultCfg, err := config.Init(ioutil.Discard, 2048)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	defaultCfg.Bootstrap = cfg.Peers

	// Addressess
	if ipfs.config.Addresses != nil {
		// Parse any overrides for datastore
		var addr config.Addresses
		err = json.Unmarshal(ipfs.config.Addresses, &addr)
		if err != nil {
			return nil, err
		}
		defaultCfg.Addresses = addr
	}

	// Write swarm key to repo path

	// Create a Temporary Repo
	err = ipfs.createRepo(ctx, cfg.RootPath, defaultCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create ipfs repo: %s", err)
	}

	// Create the IPFS node
	api, err := ipfs.createNode(ctx, cfg.RootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create ipfs node: %s", err)
	}

	ipfs.coreAPI = api

	// connect to configured peers
	/**
	err = ipfs.connectToPeers(ctx, defaultCfg)

	// TODO should only log err
	if err != nil {
		return nil, fmt.Errorf("failed to connect to bootstrap peers: %v", err)
	}
	*/

	// Create the storage strategy

	return ipfs, nil
}

// NodeAddr formats and returns the node identifier for the ipfs node
func (fs *Ipfs) NodeAddr() string {
	p := peer.AddrInfo{
		ID:    fs.ipfsNode.Identity,
		Addrs: fs.ipfsNode.PeerHost.Addrs(),
	}

	addr, err := peer.AddrInfoToP2pAddrs(&p)
	if err != nil {
		return ""
	}

	if len(addr) <= 0 {
		return ""
	}

	return addr[0].String()
}

// ensureDir ensures directory at path exist if not creates it.
func ensureDir(path string) (err error) {
	if _, err = os.Stat(path); os.IsNotExist(err) {
		// Try to make 0700
		if err = os.MkdirAll(path, os.ModePerm); err != nil {
			return fmt.Errorf("failed to create root dir: %s", path)
		}
	}

	return err
}

// StorageInterface impl

// Get retrieves object at path and returns as a os.File instance
// path should be ipfs cid. Caller should close file when done
func (fs *Ipfs) Get(path string) (f *os.File, err error) {
	// Create output filename from the CID path string
	var fname string
	fname, err = fs.makeFilename(path)
	if err != nil {
		return
	}

	// Return file if exist since content is same
	if _, err = os.Stat(fname); os.IsExist(err) {
		return os.Open(fname)
	}

	p := ipath.New(path)
	unixfs := fs.coreAPI.Unixfs()
	node, err := unixfs.Get(context.Background(), p)
	if err != nil {
		return nil, err
	}

	// Write content to filesystem
	if err = files.WriteTo(node, fname); err != nil {
		return nil, err
	}

	return os.Open(fname)
}

// GetStream provides a stream for the file at path which should be CID string
func (fs *Ipfs) GetStream(path string) (io.ReadCloser, error) {
	p := ipath.New(path)
	unixfs := fs.coreAPI.Unixfs()
	node, err := unixfs.Get(context.Background(), p)
	if err != nil {
		return nil, err
	}

	// node should be files.File
	file, ok := node.(files.File)
	if !ok {
		return nil, fmt.Errorf("path is not a file: '%s'", path)
	}

	return file, nil
}

// Put implies adding file to ipfs.  If reader is nil then assume path references
// the file or directory to add. The file is pinned to prevent GC, when deleted the
// pin is removed.
func (fs *Ipfs) Put(path string, reader io.Reader) (*oss.Object, error) {
	// The ipfs file
	var node files.Node
	if reader == nil {
		st, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		node, err = files.NewSerialFile(path, false, st)
		if err != nil {
			return nil, err
		}
	} else {
		node = files.NewReaderFile(reader)
	}

	res, err := fs.coreAPI.Unixfs().Add(context.Background(), node)
	if err != nil {
		return nil, err
	}

	// Pin the file
	p := res.String()
	now := time.Now()

	ipath := ipath.New(p)
	err = fs.coreAPI.Pin().Add(context.Background(), ipath)

	return &oss.Object{
		Path:             p,
		Name:             strings.Split(p, "/")[2],
		LastModified:     &now,
		StorageInterface: fs,
	}, err
}

// Delete removes pinned path so it maybe GC. path should be the
// CID to remove
func (fs *Ipfs) Delete(path string) error {
	// Remoe file if on disk and unpinn
	if fname, err := fs.makeFilename(path); err == nil {
		os.Remove(fname)
	}

	ipath := ipath.New(path)
	return fs.coreAPI.Pin().Rm(context.Background(), ipath)
}

// List the files at directory path (path should be ipfs cid for directory)
func (fs *Ipfs) List(path string) ([]*oss.Object, error) {
	dir := ipath.New(path)
	entries, err := fs.coreAPI.Unixfs().Ls(context.Background(), dir)
	if err != nil {
		return nil, err
	}

	dl := make([]*oss.Object, 0)
	now := time.Now()
loop:
	for {
		select {
		case entry, ok := <-entries:
			if !ok {
				break loop
			}

			var n string
			p := entry.Cid.String()
			if strings.HasPrefix(p, "/ipfs/") {
				n = strings.Split(p, "/")[2]
			} else {
				n = p
				p = "/ipfs/" + p
			}

			dl = append(dl, &oss.Object{
				Path:             p,
				Name:             n,
				LastModified:     &now,
				StorageInterface: fs,
			})
		}
	}

	return dl, nil
}

// GetURL no-op
func (fs *Ipfs) GetURL(path string) (string, error) {
	return path, nil
}

// GetEndpoint no-op
func (fs *Ipfs) GetEndpoint() string {
	return "/ipfs"
}

// Creates an IPFS node and returns its coreAPI
func (fs *Ipfs) createNode(ctx context.Context, repoPath string) (icore.CoreAPI, error) {
	// Open the repo
	repo, err := fsrepo.Open(repoPath)
	if err != nil {
		return nil, err
	}

	// Construct the node
	nodeOptions := &core.BuildCfg{
		Online: true,

		// This option sets the node to be a full DHT node
		// (both fetching and storing DHT Records)
		Routing: libp2p.DHTOption,

		// Routing: libp2p.DHTClientOption,
		// This option sets the node to be a client DHT node (only fetching records)

		Repo: repo,
	}

	node, err := core.NewNode(ctx, nodeOptions)
	if err != nil {
		return nil, err
	}

	fs.ipfsNode = node

	// Attach the Core API to the constructed node
	return coreapi.NewCoreAPI(node)
}

func (fs *Ipfs) setupPlugins(path string) error {
	// Load any external plugins if available
	plugins, err := loader.NewPluginLoader(path)
	if err != nil {
		return fmt.Errorf("error loading plugins: %s", err)
	}

	// Load preloaded and external plugins
	if err := plugins.Initialize(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	if err := plugins.Inject(); err != nil {
		// Dont fail on Inject()
		//		return fmt.Errorf("error initializing plugins: %s", err)
	}

	return nil
}

// TODO fix this, change default configuration values.
// Will need to add config options.
func (fs *Ipfs) createRepo(ctx context.Context, repoPath string, defaultCfg *config.Config) (err error) {
	// Provide specific config modifications from default init.
	if fs.config.DataStore != nil {
		// Parse any overrides for datastore
		var ds config.Datastore
		err = json.Unmarshal(fs.config.DataStore, &ds)
		if err != nil {
			return
		}
		defaultCfg.Datastore = ds
	}

	// Create the repo with the config
	err = fsrepo.Init(repoPath, defaultCfg)
	if err != nil {
		return fmt.Errorf("failed to init ipfs node: %s", err)
	}

	return nil
}

// makeFilename creates filename from ipfs path
func (fs *Ipfs) makeFilename(path string) (string, error) {
	paths := strings.Split(path, "/")
	if len(paths) == 0 {
		return "", fmt.Errorf("path specified does not specify a valid ipfs CID")
	}

	return filepath.Join(fs.config.TempDir, paths[len(paths)-1]), nil
}

// connectToPeers connects the ipfs node to its peers
func (fs *Ipfs) connectToPeers(ctx context.Context, defaultCfg *config.Config) error {
	addrInfos := make(map[peer.ID]*peer.AddrInfo, len(fs.config.Peers))
	for _, addrStr := range fs.config.Peers {
		addr, err := ma.NewMultiaddr(addrStr)
		if err != nil {
			return err
		}

		pii, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			return err
		}

		pi, ok := addrInfos[pii.ID]
		if !ok {
			pi = &peer.AddrInfo{ID: pii.ID}
			addrInfos[pi.ID] = pi
		}

		pi.Addrs = append(pi.Addrs, pii.Addrs...)
	}

	var wg sync.WaitGroup
	wg.Add(len(addrInfos))
	for _, addrInfo := range addrInfos {
		go func(addrInfo *peer.AddrInfo) {
			defer wg.Done()
			err := fs.coreAPI.Swarm().Connect(ctx, *addrInfo)
			if err != nil {
				// TODO get logger
				log.Printf("failed to connect to %s: %s", addrInfo.ID, err)
			}
		}(addrInfo)
	}

	wg.Wait()
	return nil
}
