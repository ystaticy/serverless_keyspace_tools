package handle

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	pd "github.com/tikv/pd/client"
	"github.com/ystaticy/serverless_keyspace_tools/common"
	"os"
)

func DumpArchiveKeyspaceList(ctx context.Context, pdClient pd.Client, rules *[]common.Rule, dumpfile, dumpFilePdRulesToDelete *os.File) {

	ksMeta :=
		common.GetAllKeyspace(ctx, pdClient)

	w := bufio.NewWriter(dumpfile)
	w1 := bufio.NewWriter(dumpFilePdRulesToDelete)
	for i := range ksMeta {
		keyspace := ksMeta[i]
		// keyspaceName := keyspace.Name
		if keyspace.State == keyspacepb.KeyspaceState_ARCHIVED {
			idStr := fmt.Sprintf("%d", keyspace.Id)
			common.WriteFile(dumpfile, idStr)

			// dump placement rules related to this keyspace
			filterRules := common.FilterPlacementRulesByKeyspace(rules, keyspace.Id)
			for _, rule := range filterRules {
				w1.WriteString(rule.ID)
				w1.WriteByte(byte('\n'))
			}
		}
	}
	w.Flush()
	w1.Flush()
}

func DumpAllPdRules(file *os.File, rules []common.Rule) {
	w := bufio.NewWriter(file)

	marshalRules, _ := json.Marshal(rules)
	w.Write(marshalRules)
	w.Flush()
}
