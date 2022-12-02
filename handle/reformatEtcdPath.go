package handle

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/pingcap/log"
	"github.com/ystaticy/serverless_keyspace_tools/common"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	pathPrefix  = "/keyspaces/tidb/"
	target      = "//"
	replacement = "/"
)

type (
	task   []*mvccpb.KeyValue
	result int
)

var (
	cli *clientv3.Client
)

func ReformatEtcdPath(ctx context.Context, pdAddr string, workerCount int, targetKeyspaceID string, pathLimit int, run bool) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var err error
	// Connect to etcd.
	cli, err = clientv3.New(clientv3.Config{
		Endpoints: []string{pdAddr},
	})
	if err != nil {
		return err
	}

	// Get all keys that needs to be fixed and batch them into jobs.
	var res *clientv3.GetResponse
	if len(targetKeyspaceID) != 0 {
		res, err = cli.Get(ctx, pathPrefix+targetKeyspaceID+target, clientv3.WithPrefix())
	} else {
		res, err = cli.Get(ctx, pathPrefix, clientv3.WithPrefix())
	}

	if err != nil {
		return err
	}

	var (
		keysToReformat      int
		keyspacesToReformat int
		skippedKey          int
	)
	// A map used to group keyspaces with same ID to the group.
	jobs := make(map[string]task)
	for _, kv := range res.Kvs {
		before, _, exist := strings.Cut(string(kv.Key), target)
		// Skip keys that do not contain target substring.
		if !exist {
			skippedKey++
			continue
		}
		keyspaceID := strings.Trim(before, pathPrefix)
		keysToReformat++
		jobs[keyspaceID] = append(jobs[keyspaceID], kv)
	}
	keyspacesToReformat = len(jobs)

	// Split tasks inside job map by pathLimit, This is useful in the rare case where some keyspaces contains
	// more paths to reformat than what a single etcd transaction allows (128 ops i.e. 64 paths).
	// Notice that this break the atomicity of path reformat at keyspace level.
	if pathLimit > 0 {
		splitJobs(jobs, pathLimit)
	}

	log.Info("Start to reformat",
		zap.Int("total scanned key", len(res.Kvs)),
		zap.Int("keyspaces to reformat", keyspacesToReformat),
		zap.Int("total transactions", len(jobs)),
		zap.Int("keys to reformat", keysToReformat),
		zap.Int("skipped key", skippedKey),
	)

	for k, v := range jobs {
		fmt.Printf("-------------------- keyspaceID: %s --------------------\n", k)
		for _, path := range v {
			fmt.Printf("\tkey: %s\n\tvalue: %s\n\tlease: %d\n", string(path.Key), string(path.Value), path.Lease)
		}
	}
	if !run {
		log.Info("not operating in run mode, reformat skipped")
		return nil
	}

	if len(jobs) == 0 {
		log.Info("no keys to reformat, reformatting skipped")
		return nil
	}

	// Initialize workers.
	input, output, errChan := common.NewPool[task, result](ctx, workerCount, reformat)
	// Send jobs to workers.
	go func() {
		for _, job := range jobs {
			input <- job
		}
	}()
	var (
		successTxn int
		successKey int
		failedTxn  int
	)
	// Receive results.
	for _ = range jobs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case reformatCount := <-output:
			successTxn++
			successKey += int(reformatCount)
		case err = <-errChan:
			failedTxn++
			log.Error("failed to reformat the key", zap.Error(err))
		}
	}
	log.Info("reformatting complete",
		zap.Int("total reformatted keys", successKey),
		zap.Int("success transaction", successTxn),
		zap.Int("failed transaction", failedTxn),
	)
	return nil
}

// fixKeyPath replace first instance of target in key with newStr, if target not found, original string along with false will be returned.
func fixKeyPath(key, target, newStr string) (string, bool) {
	if !strings.Contains(key, target) {
		return key, false
	}
	return strings.Replace(key, target, newStr, 1), true
}

func reformat(ctx context.Context, input task) (result, error) {
	var (
		// Create a list of operations that first copy the keys (along with its lease) under the new prefix,
		// then remove the old keys.
		ops          []clientv3.Op
		newKey       string
		needReformat bool
	)
	totalReformatKey := 0
	for _, kv := range input {
		newKey, needReformat = fixKeyPath(string(kv.Key), target, replacement)
		if !needReformat {
			continue
		}
		ops = append(ops,
			clientv3.OpPut(
				newKey,
				string(kv.Value),
				// The original key's lease should be copied to the reformatted key.
				clientv3.WithLease(clientv3.LeaseID(kv.Lease)),
			),
		)
		ops = append(ops, clientv3.OpDelete(string(kv.Key)))
		totalReformatKey++
	}
	if totalReformatKey == 0 {
		return 0, nil
	}
	// Execute operations in a single transaction.
	resp, err := cli.Txn(ctx).
		Then(ops...).
		Commit()
	if err != nil {
		return 0, err
	}
	if !resp.Succeeded {
		return 0, errors.New("txn failed")
	}
	return result(totalReformatKey), nil
}

// split jobs split the tasks inside job map by batchSize.
func splitJobs(jobs map[string]task, batchSize int) {
	for k, v := range jobs {
		if len(v) <= batchSize {
			continue
		}
		batch := 0
		for start := 0; start < len(v); start += batchSize {
			end := start + batchSize
			if end > len(v) {
				end = len(v)
			}
			jobs[k+fmt.Sprintf("(batch: %d)", batch)] = v[start:end]
			batch++
		}
		delete(jobs, k)
	}
}
