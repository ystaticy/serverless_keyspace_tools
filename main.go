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

import (
	"context"
	"flag"
	"fmt"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/ystaticy/serverless_keyspace_tools/common"
	"github.com/ystaticy/serverless_keyspace_tools/handle"
	"time"

	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var (
	ca                      = flag.String("ca", "", "CA certificate path for TLS connection")
	cert                    = flag.String("cert", "", "certificate path for TLS connection")
	key                     = flag.String("key", "", "private key path for TLS connection")
	dumpFilepath            = flag.String("dumpfile-ks", "dumpfile_ks.txt", "file to store archive keyspace list")
	dumpFilePdRulePath      = flag.String("dumpfile-pd-rules", "dumpfile_pd_rules.txt", "file to store all placement rules")
	dumpRegionLabelFilepath = flag.String("dumpfile-region-labels", "dumpfile_region_labels.txt", "file to store archive keyspace list")
	pdAddr                  = flag.String("pd", "127.0.0.1:2379", "")
	opType                  = flag.String("op", "dump_archive_ks", "dump_archive_ks,archive_ks,dump_pd_rules,archive_pd_rules,dump_region_labels,archive_region_labels")
)

func main() {
	flag.Parse()
	if *pdAddr == "" {
		log.Panic("pd address is empty")
	}

	timeout := time.Second * 10
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	pdClient, err := pd.NewClientWithContext(ctx, []string{*pdAddr}, pd.SecurityOption{
		CAPath:   *ca,
		CertPath: *cert,
		KeyPath:  *key,
	})
	if err != nil {
		log.Panic("create pd client failed", zap.Error(err))
	}

	switch *opType {
	case "dump_archive_ks": // Get archive keyspace id list
		{
			dumpfilePath, err := common.OpenFile(*dumpFilepath)
			if err != nil {
				log.Fatal(err.Error())
			}

			rules, err := common.GetPlacementRules(ctx, []string{*pdAddr})
			if err != nil {
				log.Fatal(err.Error())
			}

			handle.DumpArchiveKeyspaceList(ctx, pdClient, &rules, dumpfilePath)

			dumpfilePath.Close()
		}
	case "archive_ks": // Delete Range by keyspace
		{
			dumpfilePath, err := common.OpenFile(*dumpFilepath)
			if err != nil {
				log.Fatal(err.Error())
			}
			client, err := txnkv.NewClient([]string{*pdAddr})
			if err != nil {
				log.Fatal(err.Error())
			}

			fmt.Println("Please confirm is't needs to be GC.(yes/no)")
			var confirmMsg string
			fmt.Scanln(&confirmMsg)
			if confirmMsg == "yes" {
				handle.LoadKeyspaceAndDeleteRange(dumpfilePath, ctx, pdClient, client, true)
			} else {
				handle.LoadKeyspaceAndDeleteRange(dumpfilePath, ctx, pdClient, client, false)
			}

			client.Close()
			dumpfilePath.Close()
		}

	case "dump_pd_rules": // Dump all placement rules list
		{
			dumpFilePdRule, err := common.OpenFile(*dumpFilePdRulePath)
			if err != nil {
				log.Fatal(err.Error())
			}
			rules, err := common.GetPlacementRules(ctx, []string{*pdAddr})
			if err != nil {
				log.Fatal(err.Error())
			}
			handle.DumpAllPdRules(dumpFilePdRule, rules)
			dumpFilePdRule.Close()
		}
	case "archive_pd_rules": // Archive placement rules by keyspace id.
		{
			dumpFilePdRule, err := common.OpenFile(*dumpFilePdRulePath)
			if err != nil {
				log.Fatal(err.Error())
			}
			dumpfilePath, err := common.OpenFile(*dumpFilepath)
			if err != nil {
				log.Fatal(err.Error())
			}

			fmt.Println("Please confirm is't needs to be GC.(yes/no)")
			var confirmMsg string
			fmt.Scanln(&confirmMsg)
			if confirmMsg == "yes" {
				handle.LoadPlacementRulesAndGC(dumpFilePdRule, dumpfilePath, ctx, []string{*pdAddr}, true)
			} else {
				handle.LoadPlacementRulesAndGC(dumpFilePdRule, dumpfilePath, ctx, []string{*pdAddr}, false)
			}

			dumpFilePdRule.Close()
			dumpfilePath.Close()
		}
	case "dump_region_labels": // Dump all region labels.
		{
			dumpFileRegionLabelRule, err := common.OpenFile(*dumpRegionLabelFilepath)
			if err != nil {
				log.Fatal(err.Error())
			}
			rules, err := handle.GetAllLabelRules(ctx, []string{*pdAddr})
			if err != nil {
				log.Fatal(err.Error())
			}
			handle.DumpAllRegionLabelRules(dumpFileRegionLabelRule, rules)

			dumpFileRegionLabelRule.Close()
		}
	case "archive_region_labels": // Archive region labels by keyspace id.
		{
			dumpfilePath, err := common.OpenFile(*dumpFilepath)
			if err != nil {
				log.Fatal(err.Error())
			}
			dumpFileRegionLabelRule, err2 := common.OpenFile(*dumpRegionLabelFilepath)
			if err2 != nil {
				log.Fatal(err2.Error())
			}

			fmt.Println("Please confirm is't needs to be GC.(yes/no)")
			var confirmMsg string
			fmt.Scanln(&confirmMsg)
			if confirmMsg == "yes" {
				handle.LoadRegionLablesAndGC(dumpFileRegionLabelRule, dumpfilePath, ctx, []string{*pdAddr}, true)
			} else {
				handle.LoadRegionLablesAndGC(dumpFileRegionLabelRule, dumpfilePath, ctx, []string{*pdAddr}, false)
			}

			dumpfilePath.Close()
			dumpFileRegionLabelRule.Close()
		}
	}

}
