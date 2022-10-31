package handle

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/pingcap/errors"
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
	"strconv"
	"sync"
	"time"
)

const (
	unsafeDestroyRangeTimeout = 5 * time.Minute
)

func LoadKeyspaceAndArchive(file *os.File, ctx context.Context, pdClient pd.Client, client *txnkv.Client, pdAddrs []string) {

	reader := bufio.NewReader(file)

	// fetch all placement rules
	rules, err := common.GetPlacementRules(ctx, pdAddrs)
	if err != nil {
		log.Fatal(err.Error())
	}

	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		line = common.Substring(line, 0, len(line)-2)
		keyspaceId, err := strconv.ParseUint(line, 10, 32)
		if err != nil {
			panic(err)
		}
		keyspaceidUint32 := uint32(keyspaceId)
		fmt.Println("keyspaceidUint32:", keyspaceidUint32)
		rawLeftBound, rawRightBound, txnLeftBound, txnRightBound := common.GetRange(keyspaceidUint32)

		fmt.Println(hex.EncodeToString(rawLeftBound))
		fmt.Println(hex.EncodeToString(rawRightBound))
		fmt.Println(hex.EncodeToString(txnLeftBound))
		fmt.Println(hex.EncodeToString(txnRightBound))

		UnsafeDestroyRange(ctx, pdClient, client, rawLeftBound, rawRightBound)

		UnsafeDestroyRange(ctx, pdClient, client, txnLeftBound, txnRightBound)

		// filter all placement rules of this keyspace
		filterRules := common.FilterPlacementRulesByKeyspace(&rules, keyspaceidUint32)
		for _, rule := range filterRules {
			ruleMarshal, _ := json.Marshal(rule)

			log.Info("Delete placement rule", zap.String("rule", string(ruleMarshal)))

			if err = common.DeletePlacementRule(ctx, pdAddrs, rule); err != nil {
				log.Error("Delete placement rule failed", zap.String("rule", string(ruleMarshal)), zap.Error(err))
			}
		}

	}
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
func UnsafeDestroyRange(ctx context.Context, pdClient pd.Client, client *txnkv.Client, startKey []byte, endKey []byte) error {
	// Get all stores every time deleting a region. So the store list is less probably to be stale.
	stores, err := getStoresForGC(ctx, pdClient)
	if err != nil {
		metrics.TiKVUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("get_stores").Inc()
		return err
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdUnsafeDestroyRange, &kvrpcpb.UnsafeDestroyRangeRequest{
		StartKey: startKey,
		EndKey:   endKey,
	})

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
