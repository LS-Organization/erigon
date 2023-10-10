package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ots/indexer"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

func FeeRecipientExecutor(ctx context.Context, db kv.RoDB, tx kv.RwTx, isInternalTx bool, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine consensus.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, s *StageState, logger log.Logger) (uint64, error) {
	feeRecipientHandler := NewFeeRecipientIndexerHandler(tmpDir, s, logger)
	defer feeRecipientHandler.Close()

	return runIncrementalHeaderIndexerExecutor(db, tx, blockReader, startBlock, endBlock, isShortInterval, logEvery, ctx, s, feeRecipientHandler)
}

// Implements BlockIndexerHandler interface in order to index block fee recipients
type FeeRecipientIndexerHandler struct {
	indexBucket   string
	counterBucket string
	collector     *etl.Collector
	bitmaps       map[string]*roaring64.Bitmap
}

func NewFeeRecipientIndexerHandler(tmpDir string, s *StageState, logger log.Logger) HeaderIndexerHandler {
	collector := etl.NewCollector(s.LogPrefix(), tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	bitmaps := map[string]*roaring64.Bitmap{}

	return &FeeRecipientIndexerHandler{kv.OtsFeeRecipientIndex, kv.OtsFeeRecipientCounter, collector, bitmaps}
}

// Add log's ethTx index to from/to addresses indexes
func (h *FeeRecipientIndexerHandler) HandleMatch(header *types.Header) {
	h.touchIndex(header.Coinbase, header.Number)
}

func (h *FeeRecipientIndexerHandler) touchIndex(addr common.Address, blockNum *big.Int) {
	bm, ok := h.bitmaps[string(addr.Bytes())]
	if !ok {
		bm = roaring64.NewBitmap()
		h.bitmaps[string(addr.Bytes())] = bm
	}
	bm.Add(blockNum.Uint64())
}

func (h *FeeRecipientIndexerHandler) Flush(force bool) error {
	if force || needFlush64(h.bitmaps, bitmapsBufLimit) {
		if err := flushBitmaps64(h.collector, h.bitmaps); err != nil {
			return err
		}
		h.bitmaps = map[string]*roaring64.Bitmap{}
	}

	return nil
}

func (h *FeeRecipientIndexerHandler) Load(ctx context.Context, tx kv.RwTx) error {
	transferCounter, err := tx.RwCursorDupSort(h.counterBucket)
	if err != nil {
		return err
	}
	defer transferCounter.Close()

	buf := bytes.NewBuffer(nil)
	addrBm := roaring64.NewBitmap()

	loadFunc := func(k []byte, value []byte, tableReader etl.CurrentTableReader, next etl.LoadNextFunc) error {
		// Bitmap for address key
		if _, err := addrBm.ReadFrom(bytes.NewBuffer(value)); err != nil {
			return err
		}

		// Last chunk for address key
		addr := k[:length.Addr]

		// Read last chunk from DB (may not exist)
		// Chunk already exists; merge it
		if err := mergeLastChunk(addrBm, addr, tableReader); err != nil {
			return err
		}

		// Recover and delete the last counter (may not exist); will be replaced after this chunk write
		prevCounter := uint64(0)
		isUniqueChunk := false
		counterK, _, err := transferCounter.SeekExact(addr)
		if err != nil {
			return err
		}
		if counterK != nil {
			counterV, err := transferCounter.LastDup()
			if err != nil {
				return err
			}
			if len(counterV) == 1 {
				// Optimized counter; prevCounter must remain 0
				c, err := transferCounter.CountDuplicates()
				if err != nil {
					return err
				}
				if c != 1 {
					return fmt.Errorf("db possibly corrupted: bucket=%s addr=%s has optimized counter with duplicates", h.counterBucket, hexutility.Encode(addr))
				}

				isUniqueChunk = true
			} else {
				// Regular counter
				chunk := counterV[8:]
				chunkAsNumber := binary.BigEndian.Uint64(chunk)
				if chunkAsNumber != ^uint64(0) {
					return fmt.Errorf("db possibly corrupted: bucket=%s addr=%s last chunk is not 0xffffffffffffffff: %s", h.counterBucket, hexutility.Encode(addr), hexutility.Encode(chunk))
				}
			}

			// Delete last counter, optimized or not; it doesn't matter, it'll be
			// rewriten below
			if err := transferCounter.DeleteCurrent(); err != nil {
				return err
			}

			// Regular chunk, rewind to previous counter
			if !isUniqueChunk {
				prevK, prevV, err := transferCounter.PrevDup()
				if err != nil {
					return err
				}
				if prevK != nil {
					prevCounter = binary.BigEndian.Uint64(prevV[:8])
				}
			}
		}

		// Write the index chunk; cut it if necessary to fit under page restrictions
		if (counterK == nil || isUniqueChunk) && prevCounter+addrBm.GetCardinality() <= 256 {
			buf.Reset()
			b := make([]byte, 8)
			for it := addrBm.Iterator(); it.HasNext(); {
				ethTx := it.Next()
				binary.BigEndian.PutUint64(b, ethTx)
				buf.Write(b)
			}

			_, err := h.writeOptimizedChunkAndCounter(tx, k, buf, addr, next, prevCounter)
			if err != nil {
				return err
			}
		} else {
			buf.Reset()
			b := make([]byte, 8)
			for it := addrBm.Iterator(); it.HasNext(); {
				ethTx := it.Next()
				binary.BigEndian.PutUint64(b, ethTx)
				buf.Write(b)

				// cut?
				if !it.HasNext() || buf.Len() >= int(bitmapdb.ChunkLimit) {
					updatedCounter, err := h.writeRegularChunkAndCounter(tx, k, buf, addr, next, ethTx, !it.HasNext(), prevCounter)
					if err != nil {
						return err
					}
					prevCounter = updatedCounter

					// Cleanup buffer for next chunk
					buf.Reset()
				}
			}
		}

		return nil
	}
	if err := h.collector.Load(tx, h.indexBucket, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	return nil
}

func (h *FeeRecipientIndexerHandler) writeOptimizedChunkAndCounter(tx kv.RwTx, k []byte, buf *bytes.Buffer, addr []byte, next etl.LoadNextFunc, prevCounter uint64) (uint64, error) {
	// Write solo chunk
	chunkKey := chunkKey(k, true, 0)
	if err := next(k, chunkKey, buf.Bytes()); err != nil {
		return 0, err
	}

	// Write optimized counter
	prevCounter += uint64(buf.Len()) / 8
	v := indexer.OptimizedCounterSerializer(prevCounter)
	if err := tx.Put(h.counterBucket, addr, v); err != nil {
		return 0, err
	}

	return prevCounter, nil
}

func (h *FeeRecipientIndexerHandler) writeRegularChunkAndCounter(tx kv.RwTx, k []byte, buf *bytes.Buffer, addr []byte, next etl.LoadNextFunc, ethTx uint64, isLast bool, prevCounter uint64) (uint64, error) {
	chunkKey := chunkKey(k, isLast, ethTx)
	if err := next(k, chunkKey, buf.Bytes()); err != nil {
		return 0, err
	}

	// Write updated counter
	prevCounter += uint64(buf.Len()) / 8
	v := indexer.RegularCounterSerializer(prevCounter, chunkKey[length.Addr:])
	if err := tx.Put(h.counterBucket, addr, v); err != nil {
		return 0, err
	}

	return prevCounter, nil
}

func (h *FeeRecipientIndexerHandler) Close() {
	h.collector.Close()
}
