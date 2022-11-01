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
	res, err := common.DoRequest(ctx, pdAddrs, ConfigRegionRules, "GET", nil)
	if err == nil && res != nil {
		err = json.Unmarshal(res, &rules)
	}
	return rules, err
}

func DumpAllRegionLabelRules(file *os.File, regionRules []RegionRule) {
	w := bufio.NewWriter(file)

	marshalRules, _ := json.Marshal(regionRules)
	w.Write(marshalRules)
	w.Flush()
}

func LoadRegionLablesAndGC(regionLabelfile *os.File, archiveKsIdFile *os.File, ctx context.Context, pdAddrs []string) {

	regionRules := cacheRegionLabel(regionLabelfile)
	keyspaceIds := common.CacheArchiveKeyspaceId(archiveKsIdFile)
	for _, regionLabel := range regionRules {
		// regionLabel.ID is like "keyspace/1"
		isKeyspaceRegionLabel := strings.HasPrefix(regionLabel.ID, "keyspaces")
		if isKeyspaceRegionLabel {
			ksIdInRegionLabel := getKsIdInRegionLabel(regionLabel.ID)
			_, isArichiveKeyspace := keyspaceIds[ksIdInRegionLabel]
			if isArichiveKeyspace {

				ruleId := strings.Replace(regionLabel.ID, "/", "%2F", -1)
				log.Info("To Delete regionLabel:", zap.String("regionLabel.ID", regionLabel.ID))
				if err := DeleteRegionLabel(ctx, pdAddrs, ruleId); err != nil {
					log.Error("Delete regionLabel failed", zap.String("regionLabelID", regionLabel.ID), zap.Error(err))
				} else {
					log.Error("Delete region label success", zap.String("keyspaceID", ksIdInRegionLabel))
				}

			}
		}

	}
}

// regionLabelID is like "keyspace/1"
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

// regionLabelId = "keyspace%2F1"
func DeleteRegionLabel(ctx context.Context, addrs []string, regionLabelId string) error {
	uri := fmt.Sprintf(UrlRegionRules+"%s", regionLabelId)
	log.Info("uri", zap.String("uri", uri))
	res, err := common.DoRequest(ctx, addrs, uri, "DELETE", nil)
	if err != nil {
		return err
	}
	if res == nil {
		fmt.Errorf("returns error in DeleteRegionLabel")
	}

	return nil
}
