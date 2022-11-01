package handle

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/pingcap/log"
	"github.com/ystaticy/serverless_keyspace_tools/common"
	"go.uber.org/zap"
	"io"
	"os"
	"strings"
)

func cachePlacementRules(placementRulesFile *os.File) []common.Rule {
	var rules []common.Rule
	reader := bufio.NewReader(placementRulesFile)
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		err = json.Unmarshal([]byte(line), &rules)
	}
	return rules
}

func LoadPlacementRulesAndGC(placementRulesFile *os.File, archiveKeyspaceFile *os.File, ctx context.Context, pdAddrs []string, isRun bool, isSkipConfirm bool) {

	keyspaceIds := common.CacheArchiveKeyspaceId(archiveKeyspaceFile)
	placementRules := cachePlacementRules(placementRulesFile)

	failedKsCt := 0
	successedKsCt := 0
	totalKsArchive := len(keyspaceIds)

	for _, placementRule := range placementRules {
		ksIdInPlacementRule := parsePlacementRule(placementRule.ID)
		_, isArichiveKeyspace := keyspaceIds[ksIdInPlacementRule]
		if ksIdInPlacementRule != "" && isArichiveKeyspace {
			log.Info("[BEGIN] gc placement rules", zap.String("keyspaceId", ksIdInPlacementRule))
			if err := common.DeletePlacementRule(ctx, pdAddrs, placementRule, isRun, isSkipConfirm); err != nil {
				log.Error("Delete placement rule failed", zap.String("placementRule", placementRule.ID), zap.Error(err))
				failedKsCt++
			} else {
				if isRun {
					log.Info("Delete placement rule success.", zap.String("keyspaceID", ksIdInPlacementRule))
					successedKsCt++
				}
			}
		}
	}

	log.Info("GC placement rules end.",
		zap.Int("totalKsArchive", totalKsArchive),
		zap.Int("failedKsCt", failedKsCt),
		zap.Int("successedKsCt", successedKsCt))
}

func parsePlacementRule(placementRuleID string) string {
	//rule.ID, fmt.Sprintf("keyspace-id-%d-table", keyspaceId)
	arr := strings.Split(placementRuleID, "-")
	if len(arr) >= 3 {
		return arr[2]
	} else {
		return ""
	}

}

func DumpAllPdRules(file *os.File, rules []common.Rule) {
	w := bufio.NewWriter(file)

	marshalRules, _ := json.Marshal(rules)
	w.Write(marshalRules)
	w.Write([]byte("\n"))
	w.Flush()
}
