// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Test backup with exceeding GC safe point.

package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"io"
	"os"
	"time"

	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var (
	ca     = flag.String("ca", "", "CA certificate path for TLS connection")
	cert   = flag.String("cert", "", "certificate path for TLS connection")
	key    = flag.String("key", "", "private key path for TLS connection")
	pdAddr = flag.String("pd", "127.0.0.1:2379", "PD address")
)

func main1() {
	flag.Parse()
	if *pdAddr == "" {
		log.Panic("pd address is empty")
	}

	timeout := time.Second * 10
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	pdClient, err := pd.NewClientWithContext(ctx, []string{*pdAddr}, pd.SecurityOption{
		CAPath:   *ca,
		CertPath: *cert,
		KeyPath:  *key,
	})
	if err != nil {
		log.Panic("create pd client failed", zap.Error(err))
	}

	// -------- dump archive keyspace ---------
	filepath := "archive_keyspace.txt"
	dumpfile, err := OpenFile(filepath)
	if err != nil {
		log.Fatal(err.Error())
	}
	dumpArchiveKeyspaceList(ctx, pdClient, dumpfile)

}

func getAllKeyspace(ctx context.Context, pdClient pd.Client) []*keyspacepb.KeyspaceMeta {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchChan, err := pdClient.WatchKeyspaces(ctx)
	if err != nil {
		log.Error("WatchKeyspaces error")
	}
	initialLoaded := <-watchChan
	return initialLoaded
}

func dumpArchiveKeyspaceList(ctx context.Context, pdClient pd.Client, dumpfile *os.File) {

	ksMeta :=
		getAllKeyspace(ctx, pdClient)

	for i := range ksMeta {
		keyspace := ksMeta[i]
		// keyspaceName := keyspace.Name
		if keyspace.State == keyspacepb.KeyspaceState_ARCHIVED {
			idStr := fmt.Sprintf("%d", keyspace.Id)
			writeFile(dumpfile, idStr)
		}

	}
}

// OpenFile 判断文件是否存在  存在则OpenFile 不存在则Create
func OpenFile(filename string) (*os.File, error) {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		fmt.Println("文件不存在")
		return os.Create(filename) //创建文件
	}
	fmt.Println("文件存在")
	return os.OpenFile(filename, os.O_APPEND, 0666) //打开文件
}

func writeFile(file *os.File, text string) {

	n, err1 := io.WriteString(file, text) //写入文件(字符串)
	if err1 != nil {
		log.Fatal(err1.Error())
	}
	fmt.Printf("写入 %d 个字节\n", n)

}

func main() {

	filepath := "/Users/yaojingyi/workspace_keyspace_tools/serverless_keyspace_tools/test/test.txt"
	dumpfile, err := OpenFile(filepath)
	if err != nil {
		log.Error("WatchKeyspaces error", zap.Error(err))
	}
	writeFile(dumpfile, "aaa")

}
