package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"regexp"
	"time"

	api "nyxdb/pkg/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var leaderRe = regexp.MustCompile(`leader=([^\s]+)`)

type connManager struct {
	target   string
	dialOpts []grpc.DialOption
}

func newConnManager(addr string) *connManager {
	return &connManager{
		target: addr,
		dialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}
}

func (m *connManager) dial(ctx context.Context) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, m.target, m.dialOpts...)
}

func (m *connManager) leaderFromError(err error) (string, bool) {
	st, ok := status.FromError(err)
	if !ok {
		return "", false
	}
	if st.Code() != codes.FailedPrecondition {
		return "", false
	}
	subs := leaderRe.FindStringSubmatch(st.Message())
	if len(subs) == 2 && subs[1] != "" {
		return subs[1], true
	}
	return "", false
}

func (m *connManager) invokeWithRetry(ctx context.Context, method string, req, resp interface{}) error {
	backoff := 100 * time.Millisecond
	const maxAttempts = 3

	for attempt := 0; attempt < maxAttempts; attempt++ {
		conn, err := m.dial(ctx)
		if err != nil {
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		err = conn.Invoke(ctx, method, req, resp)
		_ = conn.Close()
		if err == nil {
			return nil
		}
		if leaderAddr, ok := m.leaderFromError(err); ok && leaderAddr != m.target {
			m.target = leaderAddr
			continue
		}
		return err
	}
	return fmt.Errorf("exceeded retry attempts, last target=%s", m.target)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "kv":
		kvCmd(os.Args[2:])
	case "admin":
		adminCmd(os.Args[2:])
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `NyxDB CLI

Usage:
  nyxdb-cli kv put    --addr <host:port> --key <k> --value <v>
  nyxdb-cli kv get    --addr <host:port> --key <k>
  nyxdb-cli kv delete --addr <host:port> --key <k>
  nyxdb-cli kv begin  --addr <host:port>
  nyxdb-cli kv read   --addr <host:port> --handle <h> --key <k>
  nyxdb-cli kv end    --addr <host:port> --handle <h>
  nyxdb-cli admin members --addr <host:port>
  nyxdb-cli admin join    --addr <host:port> --node <id> --peer <host:port>
  nyxdb-cli admin leave   --addr <host:port> --node <id>
  nyxdb-cli admin merge   --addr <host:port> [--force]
  nyxdb-cli admin snapshot --addr <host:port> [--force]
`)
}

func kvCmd(args []string) {
	if len(args) < 1 {
		usage()
		os.Exit(1)
	}
	sub := args[0]
	switch sub {
	case "put":
		kvPut(args[1:])
	case "get":
		kvGet(args[1:])
	case "delete":
		kvDelete(args[1:])
	case "begin":
		kvBegin(args[1:])
	case "read":
		kvRead(args[1:])
	case "end":
		kvEnd(args[1:])
	default:
		usage()
		os.Exit(1)
	}
}

func adminCmd(args []string) {
	if len(args) < 1 {
		usage()
		os.Exit(1)
	}
	sub := args[0]
	switch sub {
    case "members":
        adminMembers(args[1:])
    case "join":
        adminJoin(args[1:])
    case "leave":
        adminLeave(args[1:])
    case "merge":
        adminMerge(args[1:])
    case "snapshot":
        adminSnapshot(args[1:])
	default:
		usage()
		os.Exit(1)
	}
}

func kvPut(args []string) {
	fs := flag.NewFlagSet("kv put", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:10001", "gRPC address")
	key := fs.String("key", "", "key")
	value := fs.String("value", "", "value")
	_ = fs.Parse(args)
	if *key == "" {
		fmt.Fprintln(os.Stderr, "--key is required")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mgr := newConnManager(*addr)

	req := &api.PutRequest{Key: []byte(*key), Value: []byte(*value)}
	resp := new(api.PutResponse)
	if err := mgr.invokeWithRetry(ctx, "/nyxdb.api.KV/Put", req, resp); err != nil {
		fmt.Fprintf(os.Stderr, "put error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK")
}

func kvGet(args []string) {
	fs := flag.NewFlagSet("kv get", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:10001", "gRPC address")
	key := fs.String("key", "", "key")
	_ = fs.Parse(args)
	if *key == "" {
		fmt.Fprintln(os.Stderr, "--key is required")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mgr := newConnManager(*addr)

	req := &api.GetRequest{Key: []byte(*key)}
	resp := new(api.GetResponse)
	if err := mgr.invokeWithRetry(ctx, "/nyxdb.api.KV/Get", req, resp); err != nil {
		fmt.Fprintf(os.Stderr, "get error: %v\n", err)
		os.Exit(1)
	}
	if resp.Found {
		fmt.Printf("%s\n", string(resp.Value))
	} else {
		fmt.Println("(not found)")
	}
}

func kvDelete(args []string) {
	fs := flag.NewFlagSet("kv delete", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:10001", "gRPC address")
	key := fs.String("key", "", "key")
	_ = fs.Parse(args)
	if *key == "" {
		fmt.Fprintln(os.Stderr, "--key is required")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mgr := newConnManager(*addr)

	req := &api.DeleteRequest{Key: []byte(*key)}
	resp := new(api.DeleteResponse)
	if err := mgr.invokeWithRetry(ctx, "/nyxdb.api.KV/Delete", req, resp); err != nil {
		fmt.Fprintf(os.Stderr, "delete error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK")
}

func kvBegin(args []string) {
	fs := flag.NewFlagSet("kv begin", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:10001", "gRPC address")
	_ = fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mgr := newConnManager(*addr)

	req := &api.BeginReadTxnRequest{}
	resp := new(api.BeginReadTxnResponse)
	if err := mgr.invokeWithRetry(ctx, "/nyxdb.api.KV/BeginReadTxn", req, resp); err != nil {
		fmt.Fprintf(os.Stderr, "begin error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("handle=%x read_ts=%d\n", resp.Handle, resp.ReadTs)
}

func kvRead(args []string) {
	fs := flag.NewFlagSet("kv read", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:10001", "gRPC address")
	handleHex := fs.String("handle", "", "hex encoded read handle")
	key := fs.String("key", "", "key")
	_ = fs.Parse(args)
	if *handleHex == "" || *key == "" {
		fmt.Fprintln(os.Stderr, "--handle and --key are required")
		os.Exit(1)
	}
	handle, err := decodeHexString(*handleHex)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid handle: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mgr := newConnManager(*addr)

	req := &api.ReadTxnGetRequest{Handle: handle, Key: []byte(*key)}
	resp := new(api.ReadTxnGetResponse)
	if err := mgr.invokeWithRetry(ctx, "/nyxdb.api.KV/ReadTxnGet", req, resp); err != nil {
		fmt.Fprintf(os.Stderr, "read error: %v\n", err)
		os.Exit(1)
	}
	if resp.Found {
		fmt.Printf("%s\n", string(resp.Value))
	} else {
		fmt.Println("(not found)")
	}
}

func kvEnd(args []string) {
	fs := flag.NewFlagSet("kv end", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:10001", "gRPC address")
	handleHex := fs.String("handle", "", "hex encoded read handle")
	_ = fs.Parse(args)
	if *handleHex == "" {
		fmt.Fprintln(os.Stderr, "--handle is required")
		os.Exit(1)
	}
	handle, err := decodeHexString(*handleHex)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid handle: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mgr := newConnManager(*addr)

	req := &api.EndReadTxnRequest{Handle: handle}
	resp := new(api.EndReadTxnResponse)
	if err := mgr.invokeWithRetry(ctx, "/nyxdb.api.KV/EndReadTxn", req, resp); err != nil {
		fmt.Fprintf(os.Stderr, "end error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK")
}

func decodeHexString(s string) ([]byte, error) {
	if len(s)%2 == 1 {
		// if odd, try to pad with leading zero for convenience
		s = "0" + s
	}
	return hex.DecodeString(s)
}

func adminMembers(args []string) {
	fs := flag.NewFlagSet("admin members", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:10001", "gRPC address")
	_ = fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mgr := newConnManager(*addr)

	req := &api.MembersRequest{}
	resp := new(api.MembersResponse)
	if err := mgr.invokeWithRetry(ctx, "/nyxdb.api.Admin/Members", req, resp); err != nil {
		fmt.Fprintf(os.Stderr, "members error: %v\n", err)
		os.Exit(1)
	}
	if len(resp.Members) == 0 {
		fmt.Println("(no members)")
		return
	}
	for _, m := range resp.Members {
		fmt.Printf("node=%d addr=%s\n", m.NodeId, m.Address)
	}
}

func adminJoin(args []string) {
	fs := flag.NewFlagSet("admin join", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:10001", "gRPC address")
	node := fs.Uint64("node", 0, "node id")
	peer := fs.String("peer", "", "node gRPC address")
	_ = fs.Parse(args)
	if *node == 0 || *peer == "" {
		fmt.Fprintln(os.Stderr, "--node and --peer are required")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mgr := newConnManager(*addr)

	req := &api.JoinRequest{NodeId: *node, Address: *peer}
	resp := new(api.JoinResponse)
	if err := mgr.invokeWithRetry(ctx, "/nyxdb.api.Admin/Join", req, resp); err != nil {
		fmt.Fprintf(os.Stderr, "join error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK")
}

func adminLeave(args []string) {
	fs := flag.NewFlagSet("admin leave", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:10001", "gRPC address")
	node := fs.Uint64("node", 0, "node id")
	_ = fs.Parse(args)
	if *node == 0 {
		fmt.Fprintln(os.Stderr, "--node is required")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mgr := newConnManager(*addr)

	req := &api.LeaveRequest{NodeId: *node}
	resp := new(api.LeaveResponse)
	if err := mgr.invokeWithRetry(ctx, "/nyxdb.api.Admin/Leave", req, resp); err != nil {
		fmt.Fprintf(os.Stderr, "leave error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK")
}

func adminMerge(args []string) {
	fs := flag.NewFlagSet("admin merge", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:10001", "gRPC address")
	force := fs.Bool("force", false, "force merge even if ratio unmet")
	_ = fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mgr := newConnManager(*addr)

	req := &api.TriggerMergeRequest{Force: *force}
	resp := new(api.TriggerMergeResponse)
	if err := mgr.invokeWithRetry(ctx, "/nyxdb.api.Admin/TriggerMerge", req, resp); err != nil {
		fmt.Fprintf(os.Stderr, "merge error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("OK")
}

func adminSnapshot(args []string) {
    fs := flag.NewFlagSet("admin snapshot", flag.ExitOnError)
    addr := fs.String("addr", "127.0.0.1:10001", "gRPC address")
    force := fs.Bool("force", false, "force snapshot even if heuristics say no")
    _ = fs.Parse(args)

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    mgr := newConnManager(*addr)

    req := &api.TriggerSnapshotRequest{Force: *force}
    resp := new(api.TriggerSnapshotResponse)
    if err := mgr.invokeWithRetry(ctx, "/nyxdb.api.Admin/TriggerSnapshot", req, resp); err != nil {
        fmt.Fprintf(os.Stderr, "snapshot error: %v\n", err)
        os.Exit(1)
    }
    fmt.Println("OK")
}
