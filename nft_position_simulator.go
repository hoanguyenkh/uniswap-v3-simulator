package uniswap_v3_simulator

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// NFTPositionSimulator extends the simulator to handle NonfungiblePositionManager events
type NFTPositionSimulator struct {
	simulator            *Simulator
	tokenPositionManager *TokenPositionManager
	nftAddress           common.Address
	client               *ethclient.Client
	pools                map[string]*CorePool // Direct access to pool map for lookups

	// Event IDs for the NonfungiblePositionManager
	MintID              common.Hash
	IncreaseLiquidityID common.Hash
	DecreaseLiquidityID common.Hash
	CollectID           common.Hash
	TransferID          common.Hash
}

// NewNFTPositionSimulator creates a new simulator extension for NonfungiblePositionManager
func NewNFTPositionSimulator(simulator *Simulator, client *ethclient.Client, nftAddress common.Address, pools map[string]*CorePool) *NFTPositionSimulator {
	return &NFTPositionSimulator{
		simulator:            simulator,
		tokenPositionManager: NewTokenPositionManager(),
		nftAddress:           nftAddress,
		client:               client,
		pools:                pools,
		MintID:               NonfungiblePositionManagerMintSig,
		IncreaseLiquidityID:  NonfungiblePositionManagerIncreaseLiquiditySig,
		DecreaseLiquidityID:  NonfungiblePositionManagerDecreaseLiquiditySig,
		CollectID:            NonfungiblePositionManagerCollectSig,
		TransferID:           NonfungiblePositionManagerTransferSig,
	}
}

// GetTokenPositionManager returns the token position manager
func (nps *NFTPositionSimulator) GetTokenPositionManager() *TokenPositionManager {
	return nps.tokenPositionManager
}

// GetPool gets a pool by its address
func (nps *NFTPositionSimulator) GetPool(address common.Address) (*CorePool, error) {
	pool, exists := nps.pools[address.Hex()]
	if !exists {
		return nil, fmt.Errorf("pool not found: %s", address.Hex())
	}
	return pool, nil
}

// SyncEvents synchronizes NFT position events from the blockchain
func (nps *NFTPositionSimulator) SyncEvents(ctx context.Context, startBlock, endBlock uint64) error {
	// Query for all NonfungiblePositionManager events in the given block range
	logs, err := nps.client.FilterLogs(ctx, ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(startBlock)),
		ToBlock:   big.NewInt(int64(endBlock)),
		Addresses: []common.Address{nps.nftAddress},
		Topics: [][]common.Hash{
			{
				nps.MintID,
				nps.IncreaseLiquidityID,
				nps.DecreaseLiquidityID,
				nps.CollectID,
				nps.TransferID,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to filter logs: %w", err)
	}

	// Process events in order
	for _, log := range logs {
		if err := nps.processEvent(&log); err != nil {
			logrus.Warnf("failed to process NFT event: %v", err)
		}
	}

	return nil
}

// processEvent processes a single NFT event
func (nps *NFTPositionSimulator) processEvent(log *types.Log) error {
	topic0 := log.Topics[0]

	switch topic0 {
	case nps.MintID:
		return nps.processMintEvent(log)
	case nps.IncreaseLiquidityID:
		return nps.processIncreaseLiquidityEvent(log)
	case nps.DecreaseLiquidityID:
		return nps.processDecreaseLiquidityEvent(log)
	case nps.CollectID:
		return nps.processCollectEvent(log)
	case nps.TransferID:
		return nps.processTransferEvent(log)
	default:
		return fmt.Errorf("unknown event type: %s", topic0.Hex())
	}
}

// processMintEvent processes an NFT mint event
func (nps *NFTPositionSimulator) processMintEvent(log *types.Log) error {
	event, err := parseNFTMintEvent(log)
	if err != nil {
		return fmt.Errorf("failed to parse NFT mint event: %w", err)
	}

	// Get the pool for this position
	pool, err := nps.GetPool(common.HexToAddress(event.Pool))
	if err != nil {
		return fmt.Errorf("failed to get pool: %w", err)
	}

	// Calculate fee growth inside
	feeGrowthInside0X128, feeGrowthInside1X128, err := pool.TickManager.GetFeeGrowthInside(
		event.TickLower,
		event.TickUpper,
		pool.TickCurrent,
		pool.FeeGrowthGlobal0X128,
		pool.FeeGrowthGlobal1X128,
	)
	if err != nil {
		return fmt.Errorf("failed to get fee growth inside: %w", err)
	}

	// Add position to the token position manager
	err = nps.tokenPositionManager.HandleMint(
		event.TokenID,
		event.Owner,
		event.Pool,
		event.TickLower,
		event.TickUpper,
		event.Amount,
		feeGrowthInside0X128,
		feeGrowthInside1X128,
	)
	if err != nil {
		return fmt.Errorf("failed to handle mint: %w", err)
	}

	return nil
}

// processIncreaseLiquidityEvent processes an NFT increase liquidity event
func (nps *NFTPositionSimulator) processIncreaseLiquidityEvent(log *types.Log) error {
	event, err := parseNFTIncreaseLiquidityEvent(log)
	if err != nil {
		return fmt.Errorf("failed to parse NFT increase liquidity event: %w", err)
	}

	// Get position details
	position, exists := nps.tokenPositionManager.GetPosition(event.TokenID)
	if !exists {
		return fmt.Errorf("position not found for token ID %d", event.TokenID)
	}

	// Get the pool for this position
	pool, err := nps.GetPool(common.HexToAddress(position.Pool))
	if err != nil {
		return fmt.Errorf("failed to get pool: %w", err)
	}

	// Calculate fee growth inside
	feeGrowthInside0X128, feeGrowthInside1X128, err := pool.TickManager.GetFeeGrowthInside(
		position.TickLower,
		position.TickUpper,
		pool.TickCurrent,
		pool.FeeGrowthGlobal0X128,
		pool.FeeGrowthGlobal1X128,
	)
	if err != nil {
		return fmt.Errorf("failed to get fee growth inside: %w", err)
	}

	// Update position
	err = nps.tokenPositionManager.HandleIncreaseLiquidity(
		event.TokenID,
		event.Liquidity,
		feeGrowthInside0X128,
		feeGrowthInside1X128,
	)
	if err != nil {
		return fmt.Errorf("failed to handle increase liquidity: %w", err)
	}

	return nil
}

// processDecreaseLiquidityEvent processes an NFT decrease liquidity event
func (nps *NFTPositionSimulator) processDecreaseLiquidityEvent(log *types.Log) error {
	event, err := parseNFTDecreaseLiquidityEvent(log)
	if err != nil {
		return fmt.Errorf("failed to parse NFT decrease liquidity event: %w", err)
	}

	// Get position details
	position, exists := nps.tokenPositionManager.GetPosition(event.TokenID)
	if !exists {
		return fmt.Errorf("position not found for token ID %d", event.TokenID)
	}

	// Get the pool for this position
	pool, err := nps.GetPool(common.HexToAddress(position.Pool))
	if err != nil {
		return fmt.Errorf("failed to get pool: %w", err)
	}

	// Calculate fee growth inside
	feeGrowthInside0X128, feeGrowthInside1X128, err := pool.TickManager.GetFeeGrowthInside(
		position.TickLower,
		position.TickUpper,
		pool.TickCurrent,
		pool.FeeGrowthGlobal0X128,
		pool.FeeGrowthGlobal1X128,
	)
	if err != nil {
		return fmt.Errorf("failed to get fee growth inside: %w", err)
	}

	// Update position
	err = nps.tokenPositionManager.HandleDecreaseLiquidity(
		event.TokenID,
		event.Liquidity.Neg(), // Negate as we're decreasing
		feeGrowthInside0X128,
		feeGrowthInside1X128,
		event.Amount0,
		event.Amount1,
	)
	if err != nil {
		return fmt.Errorf("failed to handle decrease liquidity: %w", err)
	}

	return nil
}

// processCollectEvent processes an NFT collect event
func (nps *NFTPositionSimulator) processCollectEvent(log *types.Log) error {
	event, err := parseNFTCollectEvent(log)
	if err != nil {
		return fmt.Errorf("failed to parse NFT collect event: %w", err)
	}

	// Update position
	_, _, err = nps.tokenPositionManager.HandleCollect(
		event.TokenID,
		event.Amount0,
		event.Amount1,
	)
	if err != nil {
		return fmt.Errorf("failed to handle collect: %w", err)
	}

	return nil
}

// processTransferEvent processes an NFT transfer event
func (nps *NFTPositionSimulator) processTransferEvent(log *types.Log) error {
	event, err := parseNFTTransferEvent(log)
	if err != nil {
		return fmt.Errorf("failed to parse NFT transfer event: %w", err)
	}

	// Skip minting and burning (transfers from/to zero address)
	zeroAddress := common.HexToAddress("0x0000000000000000000000000000000000000000").Hex()
	if event.From == zeroAddress || event.To == zeroAddress {
		return nil
	}

	// Update ownership
	err = nps.tokenPositionManager.HandleTransfer(
		event.TokenID,
		event.From,
		event.To,
	)
	if err != nil {
		return fmt.Errorf("failed to handle transfer: %w", err)
	}

	return nil
}

// Flush saves the token position manager state to the database
func (nps *NFTPositionSimulator) Flush(db *gorm.DB) error {
	// Save the token position manager to the database
	// This is just a placeholder - you'll need to implement the actual storage
	// mechanism based on your application's needs
	return nil
}
