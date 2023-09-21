package main

import (
	"context"
	"sync"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/flashbots/mempool-dumpster/common"
	"go.uber.org/zap"
)

type BlockCache struct {
	blocks      map[string]bool
	txs         map[string]*types.Header
	lock        sync.RWMutex
	cacheHits   int
	cacheMisses int
}

func NewBlockCache() *BlockCache {
	return &BlockCache{ //nolint:exhaustruct
		blocks: make(map[string]bool),
		txs:    make(map[string]*types.Header),
	}
}

func (bc *BlockCache) addBlock(block *types.Block) {
	bc.lock.Lock()
	defer bc.lock.Unlock()
	bc.blocks[block.Hash().Hex()] = true
	for _, tx := range block.Transactions() {
		bc.txs[tx.Hash().Hex()] = block.Header()
	}
}

func (bc *BlockCache) getHeaderForTx(txHash string) *types.Header {
	bc.lock.RLock()
	defer bc.lock.RUnlock()
	header, ok := bc.txs[txHash]
	if ok {
		bc.cacheHits += 1
		return header
	}
	bc.cacheMisses += 1
	return nil
}

type TxUpdateWorker struct {
	log          *zap.SugaredLogger
	checkNodeURI string
	ethClient    *ethclient.Client
	txC          chan *common.TxSummaryEntry
	respC        chan error
	blockCache   *BlockCache
}

func NewTxUpdateWorker(log *zap.SugaredLogger, checkNodeURI string, txC chan *common.TxSummaryEntry, respC chan error, blockCache *BlockCache) (p *TxUpdateWorker) {
	return &TxUpdateWorker{ //nolint:exhaustruct
		log:          log,
		checkNodeURI: checkNodeURI,
		txC:          txC,
		respC:        respC,
		blockCache:   blockCache,
	}
}

func (p *TxUpdateWorker) start() {
	var err error

	log.Infof("- conecting worker to %s ...", p.checkNodeURI)
	p.ethClient, err = ethclient.Dial(p.checkNodeURI)
	if err != nil {
		p.log.Fatal("ethclient.Dial", "error", err)
		return
	}

	for tx := range p.txC {
		err = p.updateTx(tx)
		p.respC <- err
	}
}

func (p *TxUpdateWorker) updateTx(tx *common.TxSummaryEntry) error {
	header := p.blockCache.getHeaderForTx(tx.Hash)
	if header != nil {
		tx.IncludedAtBlockHeight = header.Number.Int64()
		tx.IncludedBlockTimestamp = int64(header.Time * 1000)
		tx.InclusionDelayMs = tx.IncludedBlockTimestamp - tx.Timestamp
		return nil
	}

	receipt, err := p.ethClient.TransactionReceipt(context.Background(), ethcommon.HexToHash(tx.Hash))
	if err != nil {
		if err.Error() == "not found" {
			// not yet included
			return nil
		} else {
			return err
		}
	} else if receipt != nil {
		// already included
		tx.IncludedAtBlockHeight = receipt.BlockNumber.Int64()
	}

	// Update timestamp
	block, err := p.ethClient.BlockByHash(context.Background(), receipt.BlockHash)
	if err != nil {
		return err
	}
	p.blockCache.addBlock(block)
	tx.IncludedBlockTimestamp = int64(block.Time() * 1000)
	tx.InclusionDelayMs = tx.IncludedBlockTimestamp - tx.Timestamp
	return nil
}
