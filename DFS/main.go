package main

import (
	"bytes"
	crypto "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"distributed-file-system/goraft"
)

type File struct {
	Name         string    `json:"name"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last_modified"`
}

type DFSStateMachine struct {
	files *sync.Map
}

func NewDFSStateMachine() *DFSStateMachine {
	return &DFSStateMachine{
		files: &sync.Map{},
	}
}

func (s *DFSStateMachine) Apply(cmd []byte) ([]byte, error) {
	c := decodeCommand(cmd)
	switch c.Kind {
	case CreateFile:
		s.files.Store(c.Path, &File{
			Name:         c.Path,
			Size:         c.Size,
			LastModified: time.Now(),
		})
		log.Printf("Applied CreateFile: %s (%d bytes)", c.Path, c.Size)

	case DeleteFile:
		s.files.Delete(c.Path)
		log.Printf("Applied DeleteFile: %s", c.Path)

	case RenameFile:
		s.files.Delete(c.OldPath)
		s.files.Store(c.NewPath, &File{
			Name:         c.NewPath,
			Size:         c.Size,
			LastModified: time.Now(),
		})
		log.Printf("Applied RenameFile: %s -> %s", c.OldPath, c.NewPath)

	default:
		return nil, fmt.Errorf("unknown command: %v", c.Kind)
	}
	return nil, nil
}

type commandKind uint8

const (
	CreateFile commandKind = iota
	DeleteFile
	RenameFile
)

type command struct {
	Kind    commandKind
	Path    string
	OldPath string
	NewPath string
	Size    int64
}

func encodeCommand(c command) []byte {
	msg := bytes.NewBuffer(nil)
	msg.WriteByte(uint8(c.Kind))

	binary.Write(msg, binary.LittleEndian, uint64(len(c.Path)))
	msg.WriteString(c.Path)

	binary.Write(msg, binary.LittleEndian, uint64(len(c.OldPath)))
	msg.WriteString(c.OldPath)

	binary.Write(msg, binary.LittleEndian, uint64(len(c.NewPath)))
	msg.WriteString(c.NewPath)

	binary.Write(msg, binary.LittleEndian, uint64(c.Size))

	return msg.Bytes()
}

func decodeCommand(msg []byte) command {
	var c command
	buf := bytes.NewBuffer(msg)

	c.Kind = commandKind(buf.Next(1)[0])

	var pathLen, oldPathLen, newPathLen, size uint64
	binary.Read(buf, binary.LittleEndian, &pathLen)
	c.Path = string(buf.Next(int(pathLen)))

	binary.Read(buf, binary.LittleEndian, &oldPathLen)
	c.OldPath = string(buf.Next(int(oldPathLen)))

	binary.Read(buf, binary.LittleEndian, &newPathLen)
	c.NewPath = string(buf.Next(int(newPathLen)))

	binary.Read(buf, binary.LittleEndian, &size)
	c.Size = int64(size)

	return c
}

type httpServer struct {
	raft         *goraft.Server
	stateMachine *DFSStateMachine
	dataDir      string
}

func (hs *httpServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	isLeader := hs.raft.IsLeader()
	status := map[string]interface{}{
		"node_id":   hs.raft.Id(),
		"is_leader": isLeader,
		"status":    "healthy",
		"timestamp": time.Now(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (hs *httpServer) listFilesHandler(w http.ResponseWriter, r *http.Request) {
	var files []File
	hs.stateMachine.files.Range(func(key, value interface{}) bool {
		files = append(files, *value.(*File))
		return true
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(files)
}

func (hs *httpServer) createFileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	if !hs.raft.IsLeader() {
		http.Error(w, "Not the leader - try another node", http.StatusServiceUnavailable)
		return
	}

	filePath := strings.TrimPrefix(r.URL.Path, "/upload/")
	log.Printf("Received CreateFile request for %s", filePath)

	dataFilePath := filepath.Join(hs.dataDir, filepath.Base(filePath))
	file, err := os.Create(dataFilePath)
	if err != nil {
		http.Error(w, "Failed to create local file", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// We need to read the body to a buffer first so we can both save it
	// and forward it to followers.
	body, err := io.ReadAll(r.Body)
	n, err := file.Write(body)

	if err != nil {
		http.Error(w, "Failed to write file content", http.StatusInternalServerError)
		return
	}

	cmd := command{
		Kind: CreateFile,
		Path: filePath,
		Size: int64(n),
	}

	_, err = hs.raft.Apply([][]byte{encodeCommand(cmd)})
	if err != nil {
		log.Printf("Raft Apply error: %s", err)
		http.Error(w, "Failed to replicate file metadata", http.StatusInternalServerError)
		return
	}

	// After metadata is committed, replicate the file content to followers
	hs.replicateToFollowers(filePath, body)

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "File '%s' created and replicated successfully (%d bytes)", filePath, n)
}

func (hs *httpServer) replicateToFollowers(filePath string, data []byte) {
	followers := hs.raft.Followers()
	if len(followers) == 0 {
		log.Println("No followers to replicate to.")
		return
	}

	var wg sync.WaitGroup
	for _, follower := range followers {
		wg.Add(1)
		go func(follower goraft.ClusterMember) {
			defer wg.Done()
			url := fmt.Sprintf("http://%s/replicate/%s", follower.HttpAddress, filePath)
			req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
			if err != nil {
				log.Printf("Error creating replication request for %s: %v", follower.HttpAddress, err)
				return
			}
			req.Header.Set("Content-Type", "application/octet-stream")

			client := &http.Client{Timeout: 10 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				log.Printf("Error replicating to %s: %v", follower.HttpAddress, err)
				return
			}
			defer resp.Body.Close()

			log.Printf("Successfully replicated %s to node %d at %s (Status: %s)", filepath.Base(filePath), follower.Id, follower.HttpAddress, resp.Status)
		}(follower)
	}

	wg.Wait()
}

func (hs *httpServer) getFileHandler(w http.ResponseWriter, r *http.Request) {
	filePath := strings.TrimPrefix(r.URL.Path, "/upload/")
	log.Printf("Received GetFile request for %s", filePath)

	_, ok := hs.stateMachine.files.Load(filePath)
	if !ok {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	dataFilePath := filepath.Join(hs.dataDir, filepath.Base(filePath))

	if _, err := os.Stat(dataFilePath); os.IsNotExist(err) {
		// This is a fallback. In a perfect scenario, this node should have the file.
		// But if it doesn't for some reason, it can try to find it elsewhere.
		// For this assignment, a simple error is sufficient.
		http.Error(w, "File content not found on this node", http.StatusNotFound)
		return
	}

	file, err := os.Open(dataFilePath)
	if err != nil {
		http.Error(w, "Could not open file on this node", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	io.Copy(w, file)
}

func (hs *httpServer) replicateFileHandler(w http.ResponseWriter, r *http.Request) {
	filePath := strings.TrimPrefix(r.URL.Path, "/replicate/")
	log.Printf("Received replication request for %s", filePath)

	dataFilePath := filepath.Join(hs.dataDir, filepath.Base(filePath))
	file, err := os.Create(dataFilePath)
	if err != nil {
		http.Error(w, "Failed to create local file for replication", http.StatusInternalServerError)
		return
	}
	defer file.Close()

	io.Copy(file, r.Body)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "File replicated successfully")
}

func (hs *httpServer) uploadHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		hs.getFileHandler(w, r)
	case http.MethodPost:
		hs.createFileHandler(w, r)
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

type config struct {
	cluster []goraft.ClusterMember
	index   int
	http    string
}

func getConfig() config {
	cfg := config{}
	var node string

	for i := 0; i < len(os.Args)-1; i++ {
		arg := os.Args[i]

		if arg == "--node" {
			var err error
			node = os.Args[i+1]
			cfg.index, err = strconv.Atoi(node)
			if err != nil {
				log.Fatalf("Expected integer for --node, got: %s", node)
			}
			i++
			continue
		}

		if arg == "--http" {
			cfg.http = os.Args[i+1]
			i++
			continue
		}

		if arg == "--cluster" {
			cluster := os.Args[i+1]
			for _, part := range strings.Split(cluster, ";") {
				details := strings.Split(part, ",")
				if len(details) != 3 {
					log.Fatalf("Invalid cluster format. Expected: id,rpc_address,http_address")
				}

				var clusterEntry goraft.ClusterMember
				var err error
				clusterEntry.Id, err = strconv.ParseUint(details[0], 10, 64)
				if err != nil {
					log.Fatalf("Expected integer for cluster ID, got: %s", details[0])
				}
				clusterEntry.Address = details[1]
				clusterEntry.HttpAddress = details[2]
				cfg.cluster = append(cfg.cluster, clusterEntry)
			}
			i++
			continue
		}
	}

	if node == "" {
		log.Fatal("Missing required parameter: --node <index>")
	}
	if cfg.http == "" {
		log.Fatal("Missing required parameter: --http <address>")
	}
	if len(cfg.cluster) == 0 {
		log.Fatal("Missing required parameter: --cluster <id1,addr1;id2,addr2;...>")
	}

	return cfg
}

func main() {
	var b [8]byte
	_, err := crypto.Read(b[:])
	if err != nil {
		panic("cannot seed math/rand package with cryptographically secure random number generator")
	}
	rand.Seed(int64(binary.LittleEndian.Uint64(b[:])))

	cfg := getConfig()

	// Create a unique data directory for each node
	dataDir := fmt.Sprintf("./data-%d", cfg.index)
	os.MkdirAll(dataDir, 0755)

	sm := NewDFSStateMachine()

	s := goraft.NewServer(cfg.cluster, sm, ".", cfg.index)
	s.Debug = true

	go s.Start()
	// Give Raft time to start up and elect a leader
	time.Sleep(500 * time.Millisecond)

	hs := &httpServer{
		raft:         s,
		stateMachine: sm,
		dataDir:      dataDir,
	}

	// Use a dedicated mux for each server instance to avoid global state conflicts.
	mux := http.NewServeMux()
	mux.HandleFunc("/status", hs.statusHandler)
	mux.HandleFunc("/files", hs.listFilesHandler)
	mux.HandleFunc("/replicate/", hs.replicateFileHandler)
	mux.HandleFunc("/upload/", hs.uploadHandler) // Combined handler for GET and POST

	log.Printf("Node %d starting HTTP server on %s", s.Id(), cfg.http)
	log.Printf("Cluster: %d nodes", len(cfg.cluster))

	err = http.ListenAndServe(cfg.http, mux)
	if err != nil {
		panic(err)
	}
}
