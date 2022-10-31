// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Test backup with exceeding GC safe point.

package main

//
//import (
//	"context"
//	"flag"
//	"math"
//	"time"
//
//	"github.com/pingcap/log"
//	"github.com/tikv/client-go/v2/oracle"
//	pd "github.com/tikv/pd/client"
//	"go.uber.org/zap"
//)
//
//var (
//	ca       = flag.String("ca", "", "CA certificate path for TLS connection")
//	cert     = flag.String("cert", "", "certificate path for TLS connection")
//	key      = flag.String("key", "", "private key path for TLS connection")
//	pdAddr   = flag.String("pd", "10.2.103.3:2379", "PD address")
//	gcOffset = flag.Duration("gc-offset", time.Second*1,
//		"Set GC safe point to current time - gc-offset, default: 10s")
//	updateService = flag.Bool("update-service", false, "use new service to update min SafePoint")
//)
//
//func main() {
//	flag.Parse()
//	if *pdAddr == "" {
//		log.Panic("pd address is empty")
//	}
//	if *gcOffset == time.Duration(0) {
//		log.Panic("zero gc-offset is not allowed")
//	}
//
//	timeout := time.Second * 10
//	ctx, cancel := context.WithTimeout(context.Background(), timeout)
//	defer cancel()
//	pdclient, err := pd.NewClientWithContext(ctx, []string{*pdAddr}, pd.SecurityOption{
//		CAPath:   *ca,
//		CertPath: *cert,
//		KeyPath:  *key,
//	})
//	if err != nil {
//		log.Panic("create pd client failed", zap.Error(err))
//	}
//	p, l, err := pdclient.GetTS(ctx)
//	if err != nil {
//		log.Panic("get ts failed", zap.Error(err))
//	}
//
//	q, err := pdclient.GetAllStores(ctx)
//	address := q[0].Address
//	log.Info("store :", zap.String("address", address))
//
//	currentTime := uint64(time.Now().Unix())
//	log.Info("test----- :", zap.Uint64("currentTime", currentTime))
//
//	now := oracle.ComposeTS(p, l)
//	nowMinusOffset := oracle.GetTimeFromTS(now).Add(-*gcOffset)
//	newSP := oracle.ComposeTS(oracle.GetPhysical(nowMinusOffset), 0)
//	if *updateService {
//		minSafepoint, err := pdclient.UpdateServiceGCSafePoint(ctx, "gc_worker", math.MaxInt64, newSP)
//		log.Info("minSafepoint:", zap.Uint64("minSafepoint", minSafepoint))
//		if err != nil {
//			log.Panic("update service safe point failed", zap.Error(err))
//		}
//		log.Info("update service GC safe point", zap.Uint64("SP", newSP), zap.Uint64("now", now))
//	} else {
//		_, err = pdclient.UpdateGCSafePoint(ctx, newSP)
//
//		if err != nil {
//			log.Panic("update safe point failed", zap.Error(err))
//		}
//		log.Info("update GC safe point", zap.Uint64("SP", newSP), zap.Uint64("now", now))
//
//		time.Sleep(time.Duration(5) * time.Second)
//
//	}
//
//}
