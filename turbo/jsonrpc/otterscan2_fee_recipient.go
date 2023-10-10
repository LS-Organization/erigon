package jsonrpc

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/turbo/services"
)

type FeeRecipientListResult struct {
	BlocksSummary map[hexutil.Uint64]*BlockSummary `json:"blocksSummary"`
	Results       []*FeeRecipientMatch             `json:"results"`
}

type FeeRecipientMatch struct {
	BlockNum hexutil.Uint64 `json:"blockNumber"`
	// Amount    hexutil.Uint64 `json:"amount"`
}

type feeRecipientSearchResultMaterializer struct {
	blockReader services.FullBlockReader
}

func NewFeeRecipientSearchResultMaterializer(tx kv.Tx, blockReader services.FullBlockReader) (*feeRecipientSearchResultMaterializer, error) {
	return &feeRecipientSearchResultMaterializer{blockReader}, nil
}

func (w *feeRecipientSearchResultMaterializer) Convert(ctx context.Context, tx kv.Tx, idx uint64) (*FeeRecipientMatch, error) {
	// hash, err := w.blockReader.CanonicalHash(ctx, tx, blockNum)
	// if err != nil {
	// 	return nil, err
	// }
	// TODO: replace by header
	// body, _, err := w.blockReader.Body(ctx, tx, hash, blockNum)
	// if err != nil {
	// 	return nil, err
	// }

	result := &FeeRecipientMatch{
		BlockNum: hexutil.Uint64(idx),
	}
	return result, nil
}

func (api *Otterscan2APIImpl) GetFeeRecipientList(ctx context.Context, addr common.Address, idx, count uint64) (*FeeRecipientListResult, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	srm, err := NewFeeRecipientSearchResultMaterializer(tx, api._blockReader)
	if err != nil {
		return nil, err
	}

	ret, err := genericResultList(ctx, tx, addr, idx, count, kv.OtsFeeRecipientIndex, kv.OtsFeeRecipientCounter, (SearchResultMaterializer[FeeRecipientMatch])(srm))
	if err != nil {
		return nil, err
	}

	blocks := make([]hexutil.Uint64, 0, len(ret))
	for _, r := range ret {
		blocks = append(blocks, hexutil.Uint64(r.BlockNum))
	}

	blocksSummary, err := api.newBlocksSummaryFromResults(ctx, tx, blocks)
	if err != nil {
		return nil, err
	}
	return &FeeRecipientListResult{
		BlocksSummary: blocksSummary,
		Results:       ret,
	}, nil
}

func (api *Otterscan2APIImpl) GetFeeRecipientCount(ctx context.Context, addr common.Address) (uint64, error) {
	return api.genericGetCount(ctx, addr, kv.OtsFeeRecipientCounter)
}
