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
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const (
	pathPrefix   = "/keyspaces/tidb/"
	reformatLock = "/keyspace_reformat_lock"
	target       = "//"
	replacement  = "/"
)

type (
	task   []*mvccpb.KeyValue
	result int
)

var (
	cli *clientv3.Client
	mu  *concurrency.Mutex
)

func ReformatEtcdPath(ctx context.Context, pdAddr string, workerCount int, targetKeyspaceID string, run bool) error {
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
		keysToReformat int
		skippedKey     int
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

	log.Info("Start to reformat",
		zap.Int("total scanned key", len(res.Kvs)),
		zap.Int("keys to reformat", keysToReformat),
		zap.Int("skipped key", skippedKey),
		zap.Int("keyspaces to reformat", len(jobs)),
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

	// Create mutex for concurrency control.
	session, err := concurrency.NewSession(cli, concurrency.WithContext(ctx))
	if err != nil {
		return err
	}
	defer session.Close()
	mu = concurrency.NewMutex(session, reformatLock)
	if err = mu.Lock(ctx); err != nil {
		return err
	}
	defer mu.Unlock(ctx)

	// Initialize workers.
	input, output, errChan := common.NewPool[task, result](ctx, workerCount, reformat)
	// Send jobs to workers.
	go func() {
		for _, job := range jobs {
			input <- job
		}
	}()
	var (
		successKeyspace int
		successKey      int
		failKeyspace    int
	)
	// Receive results.
	for _ = range jobs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case reformatCount := <-output:
			successKeyspace++
			successKey += int(reformatCount)
		case err = <-errChan:
			failKeyspace++
			log.Error("failed to reformat the key", zap.Error(err))
		}
	}
	log.Info("reformatting complete", zap.Int("total reformatted keys", successKey),
		zap.Int("success keyspaces", successKeyspace), zap.Int("failed keyspaces", failKeyspace))
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
	// Execute the operations if the mutex is still held.
	resp, err := cli.Txn(ctx).
		If(mu.IsOwner()).
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
