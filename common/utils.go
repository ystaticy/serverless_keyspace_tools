package common

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	pads = make([]byte, encGroupSize)
)

const (
	encGroupSize = 8
	encMarker    = byte(0xFF)
)

type PeerRoleType string
type ConstraintOp string
type Constraint struct {
	Key    string       `json:"key,omitempty"`
	Op     ConstraintOp `json:"op,omitempty"`
	Values []string     `json:"values,omitempty"`
}
type Constraints []Constraint

type Rule struct {
	GroupID     string       `json:"group_id"`
	ID          string       `json:"id"`
	Index       int          `json:"index,omitempty"`
	Override    bool         `json:"override,omitempty"`
	StartKeyHex string       `json:"start_key"`
	EndKeyHex   string       `json:"end_key"`
	Role        PeerRoleType `json:"role"`
	Count       int          `json:"count"`
	Constraints Constraints  `json:"label_constraints,omitempty"`
}

func WriteFile(file *os.File, text string) {

	write := bufio.NewWriter(file)
	write.WriteString(text + "\n")
	write.Flush()

}

// EncodeBytes guarantees the encoded value is in ascending order for comparison,
// encoding with the following rule:
//
//	[group1][marker1]...[groupN][markerN]
//	group is 8 bytes slice which is padding with 0.
//	marker is `0xFF - padding 0 count`
//
// For example:
//
//	[] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]
//	[1, 2, 3] -> [1, 2, 3, 0, 0, 0, 0, 0, 250]
//	[1, 2, 3, 0] -> [1, 2, 3, 0, 0, 0, 0, 0, 251]
//	[1, 2, 3, 4, 5, 6, 7, 8] -> [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
//
// Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
func EncodeBytes(data []byte) []byte {
	// Allocate more space to avoid unnecessary slice growing.
	// Assume that the byte slice size is about `(len(data) / encGroupSize + 1) * (encGroupSize + 1)` bytes,
	// that is `(len(data) / 8 + 1) * 9` in our implement.
	dLen := len(data)
	result := make([]byte, 0, (dLen/encGroupSize+1)*(encGroupSize+1))
	for idx := 0; idx <= dLen; idx += encGroupSize {
		remain := dLen - idx
		padCount := 0
		if remain >= encGroupSize {
			result = append(result, data[idx:idx+encGroupSize]...)
		} else {
			padCount = encGroupSize - remain
			result = append(result, data[idx:]...)
			result = append(result, pads[:padCount]...)
		}

		marker := encMarker - byte(padCount)
		result = append(result, marker)
	}
	return result
}

func GetAllKeyspace(ctx context.Context, pdClient pd.Client) []*keyspacepb.KeyspaceMeta {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchChan, err := pdClient.WatchKeyspaces(ctx)
	if err != nil {
		log.Error("WatchKeyspaces error")
		panic(err)
	}
	initialLoaded := <-watchChan
	return initialLoaded
}

func GetRange(id uint32) ([]byte, []byte, []byte, []byte) {

	keyspaceIDBytes := make([]byte, 4)
	nextKeyspaceIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyspaceIDBytes, id)
	binary.BigEndian.PutUint32(nextKeyspaceIDBytes, id+1)

	rawLeftBound := append([]byte{'r'}, keyspaceIDBytes[1:]...)
	rawRightBound := append([]byte{'r'}, nextKeyspaceIDBytes[1:]...)
	txnLeftBound := append([]byte{'x'}, keyspaceIDBytes[1:]...)
	txnRightBound := append([]byte{'x'}, nextKeyspaceIDBytes[1:]...)

	// todo log print encode(done)

	log.Info("[CHECK deleteRange]", zap.ByteString("rawLeftBound", rawLeftBound), zap.ByteString("rawRightBound", rawRightBound))
	log.Info("[CHECK deleteRange]", zap.ByteString("txnLeftBound", txnLeftBound), zap.ByteString("txnRightBound", txnRightBound))
	log.Info("[CHECK deleteRange(encode)]", zap.String("rawLeftBound", hex.EncodeToString(EncodeBytes(rawLeftBound))), zap.String("rawRightBound", hex.EncodeToString(EncodeBytes(rawRightBound))))
	log.Info("[CHECK deleteRange(encode)]", zap.String("txnLeftBound", hex.EncodeToString(EncodeBytes(txnLeftBound))), zap.String("txnRightBound", hex.EncodeToString(EncodeBytes(txnRightBound))))

	return rawLeftBound, rawRightBound, txnLeftBound, txnRightBound

}

func Substring(source string, start int, end int) string {
	var r = []rune(source)
	length := len(r)

	if start < 0 || end > length || start > end {
		return ""
	}

	if start == 0 && end == length {
		return source
	}

	var substring = ""
	for i := start; i < length; i++ {
		if i == end {
			substring += string(r[i])
			break
		}
	}

	return substring
}

// ComposeURL adds HTTP schema if missing and concats address with path
func ComposeURL(address, path string) string {
	if strings.HasPrefix(address, "http://") || strings.HasPrefix(address, "https://") {
		return fmt.Sprintf("%s%s", address, path)
	}
	return fmt.Sprintf("http://%s%s", address, path)
}

func DoRequestForNoNeedConfirm(ctx context.Context, addrs []string, route, method string, body io.Reader, isRun bool) ([]byte, error) {
	return DoRequest(ctx, addrs, route, method, body, isRun, true)
}

func DoRequestForExecute(ctx context.Context, addrs []string, route, method string, body io.Reader, isRun bool, isSkipConfirm bool) ([]byte, error) {
	return DoRequest(ctx, addrs, route, method, body, isRun, isSkipConfirm)
}

func DoRequest(ctx context.Context, addrs []string, route, method string, body io.Reader, isRun bool, isSkipConfirm bool) ([]byte, error) {
	var err error
	var req *http.Request
	var res *http.Response
	for _, addr := range addrs {
		url := ComposeURL(addr, route)
		log.Info("url", zap.String("url", url))
		req, err = http.NewRequestWithContext(ctx, method, url, body)
		if err != nil {
			return nil, err
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		var confirmMsg string
		if !isSkipConfirm {
			fmt.Println("Please confirm is't needs to be GC.(yes/no)")
			fmt.Scanln(&confirmMsg)
		} else {
			confirmMsg = "yes"
		}

		if confirmMsg == "yes" {
			if isRun {
				log.Info("[REAL RUN]", zap.String("url", url), zap.String("method", method))
				httpClient := http.Client{Timeout: time.Duration(60) * time.Second}
				res, err = httpClient.Do(req)
				if err == nil {
					bodyBytes, err := io.ReadAll(res.Body)
					if err != nil {
						log.Fatal("io error", zap.Error(err))
						return nil, err
					}
					if res.StatusCode != http.StatusOK {
						log.Fatal("response not 200", zap.Int("status", res.StatusCode))
					}

					return bodyBytes, err
				}
			} else {
				log.Info("confirm yes but '-isrun' is not set, skip the operator.")
			}
		} else {
			log.Info("confirm: NO.")
		}

	}
	return nil, err
}

const ConfigRules = "/pd/api/v1/config/rules"
const ConfigRule = "/pd/api/v1/config/rule"

func DeletePlacementRule(ctx context.Context, addrs []string, rule Rule, isRun bool, isSkipConfirm bool) error {
	uri := fmt.Sprintf("%s/%s/%s", ConfigRule, rule.GroupID, rule.ID)
	res, err := DoRequestForExecute(ctx, addrs, uri, "DELETE", nil, isRun, isSkipConfirm)
	if err != nil {
		return err
	}
	if res == nil {
		fmt.Errorf("returns error in DeletePlacementRule")
	}
	return nil
}

func GetPlacementRules(ctx context.Context, addrs []string) ([]Rule, error) {
	res, err := DoRequestForNoNeedConfirm(ctx, addrs, ConfigRules, "GET", nil, true)
	if err != nil {
		return nil, err
	}

	var rules []Rule
	err = json.Unmarshal(res, &rules)
	if err != nil {
		return nil, err
	}

	return rules, nil
}

func FilterPlacementRulesByKeyspace(rules *[]Rule, keyspaceId uint32) []Rule {
	var filterRules []Rule

	for _, rule := range *rules {
		if strings.HasPrefix(rule.ID, fmt.Sprintf("keyspace-id-%d-table", keyspaceId)) {
			filterRules = append(filterRules, rule)
		}
	}

	return filterRules
}

func CacheArchiveKeyspaceId(archiveKeyspaceFile *os.File) map[string]bool {
	keyspaceIds := make(map[string]bool)
	reader := bufio.NewReader(archiveKeyspaceFile)
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		// substring "1\n" to "1"
		keyspaceIdStr, err := GetKeyspaceIdStr(line)
		if err != nil {
			panic(err)
		}

		keyspaceIds[keyspaceIdStr] = false
	}
	return keyspaceIds
}

func GetKeyspaceId(line string) (uint32, error) {
	arr := strings.Split(line, ",")
	if len(arr) == 0 {
		return 0, errors.New("There is no keyspace id.")
	}
	keyspaceId, err := strconv.ParseUint(arr[0], 10, 32)
	return uint32(keyspaceId), err
}

func GetKeyspaceIdStr(line string) (string, error) {
	arr := strings.Split(line, ",")
	if len(arr) == 0 {
		return "", errors.New("There is no keyspace id.")
	}
	return arr[0], nil
}
