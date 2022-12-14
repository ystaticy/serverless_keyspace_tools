package handle

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pingcap/log"
	"github.com/ystaticy/serverless_keyspace_tools/common"
	"go.uber.org/zap"
	"io"
	"os"
	"strings"
)

const (
	ConfigRegionRules = "/pd/api/v1/config/region-label/rules"
	UrlRegionRules    = "/pd/api/v1/config/region-label/rule/"
)

// Labels is a slice of Label.
type Labels []Label

// Label is used to describe attributes
type Label struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// Rule is used to establish the relationship between labels and a key range.
type RegionRule struct {
	ID       string        `json:"id"`
	Index    int           `json:"index"`
	Labels   Labels        `json:"labels"`
	RuleType string        `json:"rule_type"`
	Data     []interface{} `json:"data"`
}

type KeyspaceInfo struct {
	KeyspaceId   uint32
	KeyspaceName string
}

// GetAllLabelRules implements GetAllLabelRules
func GetAllLabelRules(ctx context.Context, pdAddrs []string) ([]RegionRule, error) {
	var rules []RegionRule
	res, err := common.DoRequestForNoNeedConfirm(ctx, pdAddrs, ConfigRegionRules, "GET", nil, true)
	if err == nil && res != nil {
		err = json.Unmarshal(res, &rules)
	}
	return rules, err
}

func DumpAllRegionLabelRules(file *os.File, regionRules []RegionRule) {
	w := bufio.NewWriter(file)

	marshalRules, _ := json.Marshal(regionRules)
	w.Write(marshalRules)
	w.Write([]byte("\n"))
	w.Flush()
}

func LoadRegionLablesAndGC(regionLabelfile *os.File, archiveKsIdFile *os.File, ctx context.Context, pdAddrs []string, isRun bool, isSkipConfirm bool) {
	regionRules := cacheRegionLabel(regionLabelfile)
	keyspaceIds := common.CacheArchiveKeyspaceId(archiveKsIdFile)
	failedKsCt := 0
	successedKsCt := 0
	totalKsArchive := len(keyspaceIds)
	for _, regionLabel := range regionRules {
		// regionLabel.ID is like "keyspaces/1"
		isKeyspaceRegionLabel := strings.HasPrefix(regionLabel.ID, "keyspaces")
		if isKeyspaceRegionLabel {
			ksIdInRegionLabel := getKsIdInRegionLabel(regionLabel.ID)
			_, isArichiveKeyspace := keyspaceIds[ksIdInRegionLabel]
			if isArichiveKeyspace {
				log.Info("[BEGIN] gc region labels", zap.String("keyspaceId", ksIdInRegionLabel))
				ruleId := strings.Replace(regionLabel.ID, "/", "%2F", -1)
				if err := DeleteRegionLabel(ctx, pdAddrs, ruleId, isRun, isSkipConfirm); err != nil {
					log.Error("Delete regionLabel failed", zap.String("regionLabelID", regionLabel.ID), zap.Error(err))
					failedKsCt++
				} else {
					if isRun {
						log.Info("Delete region label success", zap.String("keyspaceID", ksIdInRegionLabel))
						successedKsCt++
					}
				}
			}
		}

	}

	log.Info("GC region labels end.",
		zap.Int("totalKsArchive", totalKsArchive),
		zap.Int("failedKsCt", failedKsCt),
		zap.Int("successedKsCt", successedKsCt))

}

// regionLabelID is like "keyspaces/1"
func getKsIdInRegionLabel(regionLabelID string) string {
	arr := strings.Split(regionLabelID, "/")
	return arr[1]
}

func cacheRegionLabel(regionLabelfile *os.File) []RegionRule {
	var rules []RegionRule
	reader := bufio.NewReader(regionLabelfile)
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		err = json.Unmarshal([]byte(line), &rules)
	}
	return rules
}

// regionLabelId = "keyspaces%2F1"
func DeleteRegionLabel(ctx context.Context, addrs []string, regionLabelId string, isRun bool, isSkipConfirm bool) error {
	uri := fmt.Sprintf(UrlRegionRules+"%s", regionLabelId)
	res, err := common.DoRequestForExecute(ctx, addrs, uri, "DELETE", nil, isRun, isSkipConfirm)
	if err != nil {
		return err
	}
	if res == nil {
		fmt.Errorf("returns error in DeleteRegionLabel")
	}

	return nil
}
