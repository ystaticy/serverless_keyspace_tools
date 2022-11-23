package handle

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"github.com/ystaticy/serverless_keyspace_tools/common"
)

func DumpEnabledKeyspaceList(ctx context.Context, pdClient pd.Client, dumpfile *os.File) {

	ksMeta :=
		common.GetAllKeyspace(ctx, pdClient)

	w := bufio.NewWriter(dumpfile)
	for i := range ksMeta {
		keyspace := ksMeta[i]
		// keyspaceName := keyspace.Name
		if keyspace.State == keyspacepb.KeyspaceState_ENABLED {
			ksMsg := fmt.Sprintf("%d,%s,%d,%d", keyspace.Id, keyspace.Name, keyspace.CreatedAt, keyspace.StateChangedAt) // todo store keyspace all msg(done)
			common.WriteFile(dumpfile, ksMsg)
		}
	}
	w.Flush()
}
func ReformatEtcdPath(ctx context.Context, dumpfile *os.File, endpoint, ca string, interval time.Duration) {
	var keyspaceName string
	reader := bufio.NewReader(dumpfile)
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		arr := strings.Split(line, ",")
		if len(arr) < 1 {
			log.Error("failed to get keyspace name from dumped file")
			continue
		}
		keyspaceName = arr[1]
		if err = connectToKeyspace(ctx, keyspaceName, endpoint, ca); err != nil {
			log.Info(err.Error())
		}
		time.Sleep(interval)
	}
}
func connectToKeyspace(ctx context.Context, keyspaceName, endpoint, ca string) error {
	var dsn string
	if len(ca) == 0 {
		dsn = fmt.Sprintf("%s.root@tcp(%s)/test", keyspaceName, endpoint)
	} else {
		// Register TLS configs.
		rootCertPool := x509.NewCertPool()
		pem, err := os.ReadFile(ca)
		if err != nil {
			return err
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			return errors.Errorf("failed to append PEM.")
		}
		if err = mysql.RegisterTLSConfig("custom", &tls.Config{
			RootCAs: rootCertPool,
		}); err != nil {
			return err
		}
		dsn = fmt.Sprintf("%s.root@tcp(%s)/test?tls=custom", keyspaceName, endpoint)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	return db.Ping()
}
