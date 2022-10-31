package common

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"os"
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

	rawLeftBound := (append([]byte{'r'}, keyspaceIDBytes[1:]...))
	rawRightBound := (append([]byte{'r'}, nextKeyspaceIDBytes[1:]...))
	txnLeftBound := (append([]byte{'x'}, keyspaceIDBytes[1:]...))
	txnRightBound := (append([]byte{'x'}, nextKeyspaceIDBytes[1:]...))
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
