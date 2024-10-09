package da

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/client"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/types"
)

const retryDelay = 500 * time.Millisecond

func GetOffChainData(ctx context.Context, url string, hash common.Hash) ([]byte, error) {
	var err error
	var response types.Response
	for {
		select {
		case <-ctx.Done():
			log.Error(fmt.Sprintf("GetOffChainData hash:%v, context done", hash.String()))
			if response.Error != nil {
				return nil, fmt.Errorf("%v %v", response.Error.Code, response.Error.Message)
			}
			return nil, err
		default:
		}

		response, err = client.JSONRPCCall(url, "sync_getOffChainData", hash)
		if err != nil || response.Error != nil {
			log.Error(fmt.Sprintf("GetOffChainData hash:%v, error:%v, response err:%v", hash.String(), err, response.Error))
			time.Sleep(retryDelay)
			continue
		}

		return hexutil.Decode(strings.Trim(string(response.Result), "\""))
	}
}
