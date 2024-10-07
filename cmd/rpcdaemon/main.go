package main

import (
	"context"
	"os"
	"strings"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/erigon/zk/contracts"
	"github.com/ledgerwatch/erigon/zk/sequencer"
	"github.com/ledgerwatch/erigon/zk/syncer"
	"github.com/ledgerwatch/erigon/zkevm/etherman"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func main() {
	cmd, cfg := cli.RootCommand()
	rootCtx, rootCancel := common.RootContext()
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		logging.SetupLoggerCmd("rpcdaemon", cmd)
		db, borDb, backend, txPool, mining, stateCache, blockReader, ff, agg, err := cli.RemoteServices(ctx, *cfg, log.Root(), rootCancel)
		if err != nil {
			log.Error("Could not connect to DB", "err", err)
			return nil
		}
		defer db.Close()
		if borDb != nil {
			defer borDb.Close()
		}

		ethConfig := ethconfig.Defaults
		ethConfig.L2RpcUrl = cfg.L2RpcUrl

		// TODO: Replace with correct consensus Engine
		engine := ethash.NewFaker()

		// zkevm: the raw pool needed for limbo calls will not work if rpcdaemon is running as a standalone process.  Only the sequencer would have this detail
		// so we pass a nil raw pool here
		apiList := commands.APIList(db, borDb, backend, txPool, nil, mining, ff, stateCache, blockReader, agg, *cfg, engine, &ethconfig.Defaults, getL1Syncer(ctx, cfg))
		if err := cli.StartRpcServer(ctx, *cfg, apiList, nil); err != nil {
			log.Error(err.Error())
			return nil
		}

		return nil
	}

	if err := cmd.ExecuteContext(rootCtx); err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
}

func getL1Syncer(ctx context.Context, cfg *httpcfg.HttpCfg) *syncer.L1Syncer {
	var l1Contracts []common.Address
	var l1Topics [][]common.Hash

	ethConfig := ethconfig.Defaults
	ethConfig.L2RpcUrl = cfg.L2RpcUrl

	isSequencer := sequencer.IsSequencer()
	if isSequencer {
		l1Topics = [][]common.Hash{{
			contracts.InitialSequenceBatchesTopic,
			contracts.AddNewRollupTypeTopic,
			contracts.CreateNewRollupTopic,
			contracts.UpdateRollupTopic,
		}}
		l1Contracts = []common.Address{ethConfig.AddressZkevm, ethConfig.AddressRollup}
	} else {
		l1Topics = [][]common.Hash{{
			contracts.SequencedBatchTopicPreEtrog,
			contracts.SequencedBatchTopicEtrog,
			contracts.VerificationTopicPreEtrog,
			contracts.VerificationTopicEtrog,
			contracts.VerificationValidiumTopicEtrog,
		}}
		l1Contracts = []common.Address{ethConfig.AddressRollup, ethConfig.AddressAdmin, ethConfig.AddressZkevm}
	}

	l1Urls := strings.Split(cfg.L1RpcUrl, ",")
	etherManClients := make([]*etherman.Client, len(l1Urls))
	for i, url := range l1Urls {
		etherManClients[i] = newEtherMan(&ethConfig, cfg.ChainName, url)
	}

	ethermanClients := make([]syncer.IEtherman, len(etherManClients))
	for i, c := range etherManClients {
		ethermanClients[i] = c.EthClient
	}

	return syncer.NewL1Syncer(
		ctx,
		ethermanClients,
		l1Contracts,
		l1Topics,
		ethConfig.L1BlockRange,
		ethConfig.L1QueryDelay,
		ethConfig.L1HighestBlockType,
	)
}

// creates an EtherMan instance with default parameters
// TODO: abstract this method cause is repeated at backend.go, hach_zkevm.go and here
func newEtherMan(cfg *ethconfig.Config, l2ChainName, url string) *etherman.Client {
	ethmanConf := etherman.Config{
		URL:                       url,
		L1ChainID:                 cfg.L1ChainId,
		L2ChainID:                 cfg.L2ChainId,
		L2ChainName:               l2ChainName,
		PoEAddr:                   cfg.AddressRollup,
		MaticAddr:                 cfg.L1MaticContractAddress,
		GlobalExitRootManagerAddr: cfg.AddressGerManager,
	}

	em, err := etherman.NewClient(ethmanConf)
	//panic on error
	if err != nil {
		panic(err)
	}
	return em
}
