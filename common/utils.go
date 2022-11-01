package common

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

var pads = make([]byte, encGroupSize)

const (
	signMask uint64 = 0x8000000000000000

	encGroupSize              = 8
	encMarker                 = byte(0xFF)
	encPad                    = byte(0x0)
	unsafeDestroyRangeTimeout = 5 * time.Minute
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

// OpenFile 判断文件是否存在  存在则OpenFile 不存在则Create
func OpenFile(filename string) (*os.File, error) {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		fmt.Println("file not exists. Create a new file.")
		return os.Create(filename)
	}
	return os.OpenFile(filename, os.O_APPEND, 0666) //打开文件
}

func WriteFile(file *os.File, text string) {

	write := bufio.NewWriter(file)
	write.WriteString(text + "\n")
	fmt.Println("write test")
	//Flush将缓存的文件真正写入到文件中
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
	}
	initialLoaded := <-watchChan
	return initialLoaded
}

func GetRange(id uint32) ([]byte, []byte, []byte, []byte) {

	keyspaceIDBytes := make([]byte, 4)
	nextKeyspaceIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyspaceIDBytes, id)
	binary.BigEndian.PutUint32(nextKeyspaceIDBytes, id+1)

	rawLeftBound := EncodeBytes(append([]byte{'r'}, keyspaceIDBytes[1:]...))
	rawRightBound := EncodeBytes(append([]byte{'r'}, nextKeyspaceIDBytes[1:]...))
	txnLeftBound := EncodeBytes(append([]byte{'x'}, keyspaceIDBytes[1:]...))
	txnRightBound := EncodeBytes(append([]byte{'x'}, nextKeyspaceIDBytes[1:]...))
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

func doRequest(ctx context.Context, addrs []string, route, method string, body io.Reader) ([]byte, error) {
	var err error
	var req *http.Request
	var res *http.Response
	for _, addr := range addrs {
		url := ComposeURL(addr, route)
		req, err = http.NewRequestWithContext(ctx, method, url, body)
		if err != nil {
			return nil, err
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
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
	}
	return nil, err
}

const ConfigRules = "/pd/api/v1/config/rules"
const ConfigRule = "/pd/api/v1/config/rule"

func DeletePlacementRule(ctx context.Context, addrs []string, rule Rule) error {
	uri := fmt.Sprintf("%s/%s/%s", ConfigRule, rule.GroupID, rule.ID)
	res, err := doRequest(ctx, addrs, uri, "DELETE", nil)
	if err != nil {
		return err
	}
	if res == nil {
		fmt.Errorf("returns error in DeletePlacementRule")
	}
	return nil
}

func GetPlacementRules(ctx context.Context, addrs []string) ([]Rule, error) {
	res, err := doRequest(ctx, addrs, ConfigRules, "GET", nil)
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
