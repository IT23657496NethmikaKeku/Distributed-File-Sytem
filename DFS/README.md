# Simple Distributed File System with Raft

This project is a simple implementation of a distributed file system in Go. It uses the Raft consensus algorithm to replicate file metadata (creations, deletions) across a cluster of nodes. The actual file content is stored locally on the node that receives the upload.

The underlying Raft implementation is based on Phil Eaton's `goraft`.

## Prerequisites

*   **Go**: Version 1.20 or later.
*   **PowerShell**: Required for running the `start-cluster.ps1` script on Windows.
*   **curl**: Or any other tool for making HTTP requests to test the API.

## How to Run

Follow these steps from the project's root directory.

### 1. Build the Application

First, compile the Go application. This command creates a `dfsapi.exe` executable in the root directory.

```sh
go build -o dfsapi.exe .
```

### 2. Start the Cluster

Execute the provided PowerShell script to launch a 3-node cluster. This will open three new terminal windows, one for each server instance.

```powershell
.\start-cluster.ps1
```

The nodes will start and be available at the following HTTP addresses:
*   **Node 1**: `http://localhost:8081` (Raft RPC on `:3030`)
*   **Node 2**: `http://localhost:8082` (Raft RPC on `:3031`)
*   **Node 3**: `http://localhost:8083` (Raft RPC on `:3032`)

## Testing the File System

Once the cluster is running, you can interact with it using `curl`.

### 1. Check Node Status and Find the Leader

The nodes will automatically elect a leader. You can find out which node is the current leader by querying the `/status` endpoint on each one.

```sh
curl http://localhost:8081/status
curl http://localhost:8082/status
curl http://localhost:8083/status
```

One of the nodes will respond with `"is_leader": true`.

### 2. Upload a File

All write operations (like creating a file) **must be sent to the leader**. Let's assume Node 1 (`:8081`) is the leader.

First, create a sample file to upload:
```sh
echo "hello distributed world" > my-test-file.txt
```

Now, upload it using a `POST` request. The URL path determines the name of the file within the distributed system.

```sh
curl -X POST --data-binary @my-test-file.txt http://localhost:8081/upload/my-first-file.txt
```

You should receive a success message: `File '/upload/my-first-file.txt' created successfully (24 bytes)`.

### 3. List All Files

Because the file creation metadata was replicated via Raft, you can ask **any node** for the list of files. Let's query a follower (e.g., Node 2) to prove that the state was replicated.

```sh
curl http://localhost:8082/files
```

The response will be a JSON array containing the metadata for `my-first-file.txt`.

### 4. Download a File

You can download the file from any node that has the file's content stored locally. In this implementation, only the node that originally accepted the upload stores the content.

To download the file, send a `GET` request to the original node (Node 1 in our example):

```sh
curl http://localhost:8081/upload/my-first-file.txt
```

This will return the content of the file: `hello distributed world`.