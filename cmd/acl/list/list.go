package list

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/zk/txpool"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

var Command = cli.Command{
	Action: run,
	Name:   "list",
	Usage:  "List the content at the ACL",
	Flags: []cli.Flag{
		&utils.DataDirFlag,
	},
}

func run(cliCtx *cli.Context) error {

	dataDir := cliCtx.String(utils.DataDirFlag.Name)
	fmt.Println("Listing ", "dataDir:", dataDir)
	log.Info("at ACL listing command")

	aclDB, err := txpool.OpenACLDB(cliCtx.Context, dataDir)
	if err != nil {
		log.Error("Failed to open ACL database", "err", err)
		return err
	}

	txpool.ListContentAtACL(cliCtx.Context, aclDB)

	return nil
}
