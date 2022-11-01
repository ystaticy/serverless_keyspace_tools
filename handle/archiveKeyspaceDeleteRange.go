package handle

import (
	"bufio"
	"context"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv"
	pd "github.com/tikv/pd/client"
	"github.com/ystaticy/serverless_keyspace_tools/common"
	"github.com/ystaticy/serverless_keyspace_tools/placement"
	"go.uber.org/zap"
	"io"
	"os"
	"sync"
	"time"
)

const (
	unsafeDestroyRangeTimeout = 5 * time.Minute
)

func LoadKeyspaceAndDeleteRange(file *os.File, ctx context.Context, pdClient pd.Client, client *txnkv.Client, isRun bool) {

	reader := bufio.NewReader(file)

	failedKsCt := 0
	successedKsCt := 0
	totalKsArchive := 0

	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		totalKsArchive++

		keyspaceidUint32, err := common.GetKeyspaceId(line)
		if err != nil {
			failedKsCt++
			log.Error("UnsafeDestroyRange parse keyspace id failed", zap.Error(err))
			continue
		}

		// deleteRanges
		rawLeftBound, rawRightBound, txnLeftBound, txnRightBound := common.GetRange(keyspaceidUint32)

		fmt.Println("Please confirm is't needs to be GC.(yes/no)", zap.Bool("isRun", isRun))
		var confirmMsg string
		fmt.Scanln(&confirmMsg)
		if confirmMsg == "yes" && isRun {

			// clean rawkv range
			err1 := UnsafeDestroyRange(ctx, pdClient, rawLeftBound, rawRightBound, client)
			if err1 != nil {
				log.Error("UnsafeDestroyRange raw mode range failed", zap.Error(err1))
				failedKsCt++
				continue
			}

			// clean txnkv range
			err2 := UnsafeDestroyRange(ctx, pdClient, txnLeftBound, txnRightBound, client)
			if err2 != nil {
				log.Error("UnsafeDestroyRange txn mode range failed", zap.Error(err1))
				failedKsCt++
				continue
			}
			successedKsCt++
			log.Info("UnsafeDestroyRange success.", zap.Uint32("keyspaceId", keyspaceidUint32))
		}
	}
	log.Info("GC data ranges end.",
		zap.Int("totalKsArchive", totalKsArchive),
		zap.Int("failedKsCt", failedKsCt),
		zap.Int("successedKsCt", successedKsCt))

}

// getStoresForGC gets the list of stores that needs to be processed during GC.
func getStoresForGC(ctx context.Context, pdClient pd.Client) ([]*metapb.Store, error) {
	stores, err := pdClient.GetAllStores(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	upStores := make([]*metapb.Store, 0, len(stores))
	for _, store := range stores {
		needsGCOp, err := needsGCOperationForStore(store)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if needsGCOp {
			upStores = append(upStores, store)
		}
	}
	return upStores, nil
}

// needsGCOperationForStore checks if the store-level requests related to GC needs to be sent to the store. The store-level
// requests includes UnsafeDestroyRange, PhysicalScanLock, etc.
func needsGCOperationForStore(store *metapb.Store) (bool, error) {
	// TombStone means the store has been removed from the cluster and there isn't any peer on the store, so needn't do GC for it.
	// Offline means the store is being removed from the cluster and it becomes tombstone after all peers are removed from it,
	// so we need to do GC for it.
	if store.State == metapb.StoreState_Tombstone {
		return false, nil
	}

	engineLabel := ""
	for _, label := range store.GetLabels() {
		if label.GetKey() == placement.EngineLabelKey {
			engineLabel = label.GetValue()
			break
		}
	}

	switch engineLabel {
	case placement.EngineLabelTiFlash:
		// For a TiFlash node, it uses other approach to delete dropped tables, so it's safe to skip sending
		// UnsafeDestroyRange requests; it has only learner peers and their data must exist in TiKV, so it's safe to
		// skip physical resolve locks for it.
		return false, nil

	case placement.EngineLabelTiKV, "":
		// If no engine label is set, it should be a TiKV node.
		return true, nil

	default:
		return true, errors.Errorf("unsupported store engine \"%v\" with storeID %v, addr %v",
			engineLabel,
			store.GetId(),
			store.GetAddress())
	}
}

// UnsafeDestroyRange Cleans up all keys in a range[startKey,endKey) and quickly free the disk space.
// The range might span over multiple regions, and the `ctx` doesn't indicate region. The request will be done directly
// on RocksDB, bypassing the Raft layer. User must promise that, after calling `UnsafeDestroyRange`,
// the range will never be accessed any more. However, `UnsafeDestroyRange` is allowed to be called
// multiple times on an single range.
func UnsafeDestroyRange(ctx context.Context, pdClient pd.Client, startKey []byte, endKey []byte, client *txnkv.Client) error {
	// Get all stores every time deleting a region. So the store list is less probably to be stale.
	stores, err := getStoresForGC(ctx, pdClient)
	if err != nil {
		metrics.TiKVUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("get_stores").Inc()
		return err
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdUnsafeDestroyRange, &kvrpcpb.UnsafeDestroyRangeRequest{
		StartKey: startKey,
		EndKey:   endKey,
	}, kvrpcpb.Context{DiskFullOpt: kvrpcpb.DiskFullOpt_AllowedOnAlmostFull})

	var wg sync.WaitGroup
	errChan := make(chan error, len(stores))

	for _, store := range stores {
		address := store.Address
		storeID := store.Id
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err1 := client.GetTiKVClient().SendRequest(ctx, address, req, unsafeDestroyRangeTimeout)
			if err1 == nil {
				if resp == nil || resp.Resp == nil {
					err1 = errors.Errorf("[unsafe destroy range] returns nil response from store %v", storeID)
				} else {
					errStr := (resp.Resp.(*kvrpcpb.UnsafeDestroyRangeResponse)).Error
					if len(errStr) > 0 {
						err1 = errors.Errorf("[unsafe destroy range] range failed on store %v: %s", storeID, errStr)
					}
				}
			}

			if err1 != nil {
				metrics.TiKVUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("send").Inc()
			}
			errChan <- err1
		}()
	}

	var errs []string
	for range stores {
		err1 := <-errChan
		if err1 != nil {
			errs = append(errs, err1.Error())
		}
	}

	wg.Wait()

	if len(errs) > 0 {
		return errors.Errorf("[unsafe destroy range] destroy range finished with errors: %v", errs)
	}
	return nil
}

func DumpArchiveKeyspaceList(ctx context.Context, pdClient pd.Client, dumpfile *os.File) {

	ksMeta :=
		common.GetAllKeyspace(ctx, pdClient)

	w := bufio.NewWriter(dumpfile)
	for i := range ksMeta {
		keyspace := ksMeta[i]
		// keyspaceName := keyspace.Name
		if keyspace.State == keyspacepb.KeyspaceState_ARCHIVED {
			ksMsg := fmt.Sprintf("%d,%s,%d,%d", keyspace.Id, keyspace.Name, keyspace.CreatedAt, keyspace.StateChangedAt) // todo store keyspace all msg(done)
			common.WriteFile(dumpfile, ksMsg)
		}
	}
	w.Flush()
}
