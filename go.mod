module github.com/ystaticy/serverless_keyspace_tools

go 1.19

require (
	github.com/pingcap/kvproto v0.0.0-20220808072825-3692dfb0dad7
	github.com/pingcap/log v0.0.0-20211215031037-e024ba4eb0ee
	github.com/tikv/client-go/v2 v2.0.1-0.20221010081954-bea9aefb633b
	github.com/tikv/pd/client v0.0.0-20220307081149-841fa61e9710
	go.uber.org/zap v1.20.0
)

require github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c

require (
	github.com/benbjohnson/clock v1.1.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548 // indirect
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/uuid v1.1.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pingcap/failpoint v0.0.0-20210918120811-547c13e3eb00 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_golang v1.11.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/stathat/consistent v1.0.0 // indirect
	github.com/twmb/murmur3 v1.1.3 // indirect
	go.etcd.io/etcd/api/v3 v3.5.2 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.2 // indirect
	go.etcd.io/etcd/client/v3 v3.5.2 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.7.0 // indirect
	golang.org/x/net v0.0.0-20210428140749-89ef3d95e781 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	golang.org/x/sys v0.0.0-20211216021012-1d35b9e2eb4e // indirect
	golang.org/x/text v0.3.6 // indirect
	google.golang.org/genproto v0.0.0-20210602131652-f16073e35f0c // indirect
	google.golang.org/grpc v1.43.0 // indirect
	google.golang.org/protobuf v1.26.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect

)

// Use github.com/tidbcloud/pd-cse/client
replace github.com/tikv/pd/client => github.com/tidbcloud/pd-cse/client v0.0.0-20220905061235-4cfe0db56daa

// Use github.com/tikv/client-go/v2@api-v2
replace github.com/tikv/client-go/v2 => github.com/tikv/client-go/v2 v2.0.1-0.20221026074015-239ac1b2b7fc

// Use github.com/tidbcloud/kvproto/release-6.2-dev-tier
replace github.com/pingcap/kvproto => github.com/tidbcloud/kvproto v0.0.0-20221012070951-7a91ab2a4c7e
