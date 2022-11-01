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
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/ystaticy/serverless_keyspace_tools/common"
	"github.com/ystaticy/serverless_keyspace_tools/handle"
	"time"

	"github.com/pingcap/log"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var (
	ca                         = flag.String("ca", "", "CA certificate path for TLS connection")
	cert                       = flag.String("cert", "", "certificate path for TLS connection")
	key                        = flag.String("key", "", "private key path for TLS connection")
	dumpFilepath               = flag.String("dumpfile", "aaa", "file to store archive keyspace list")
	dumpFilePdRulePath         = flag.String("dumpfile-pd-rules", "dumpfile_pd_rules", "file to store all placement rules")
	dumpFilePdRuleToDeletePath = flag.String("dumpfile-pd-rule-to-delete", "dumpfile_pd_rules_to_delete", "file to store placement rules will be deleted")
	pdAddr                     = flag.String("pd", "127.0.0.1:2379", "")
	opType                     = flag.String("optype", "readfile", "")
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
	case "dumpfile":
		{
			dumpfilePath, err := common.OpenFile(*dumpFilepath)
			if err != nil {
				log.Fatal(err.Error())
			}

			dumpFilePdRuleToDelete, err := common.OpenFile(*dumpFilePdRuleToDeletePath)
			if err != nil {
				log.Fatal(err.Error())
			}

			rules, err := common.GetPlacementRules(ctx, []string{*pdAddr})
			if err != nil {
				log.Fatal(err.Error())
			}

			handle.DumpArchiveKeyspaceList(ctx, pdClient, &rules, dumpfilePath, dumpFilePdRuleToDelete)

			dumpfilePath.Close()
			dumpFilePdRuleToDelete.Close()
		}
	case "archive_keyspace":
		{
			dumpfilePath, err := common.OpenFile(*dumpFilepath)
			if err != nil {
				log.Fatal(err.Error())
			}
			client, err := txnkv.NewClient([]string{*pdAddr})
			if err != nil {
				log.Fatal(err.Error())
			}
			handle.LoadKeyspaceAndArchive(dumpfilePath, ctx, pdClient, client, []string{*pdAddr})

			client.Close()
			dumpfilePath.Close()
		}

	case "dump_pd_rules":
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

}

func main1() {

	filepath := "/Users/yaojingyi/workspace_keyspace_tools/serverless_keyspace_tools/test/test.txt"
	dumpfile, err := common.OpenFile(filepath)
	if err != nil {
		log.Error("open file error", zap.Error(err))
	}
	common.WriteFile(dumpfile, "aaa")

}
