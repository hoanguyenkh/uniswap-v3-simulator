package uniswap_v3_simulator

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/shopspring/decimal"
)

// Events from NonfungiblePositionManager
type NFTMintEvent struct {
	RawEvent  *types.Log      `json:"raw_event"`
	TokenID   uint64          `json:"token_id"`
	Owner     string          `json:"owner"`
	TickLower int             `json:"tick_lower"`
	TickUpper int             `json:"tick_upper"`
	Amount    decimal.Decimal `json:"amount"`
	Pool      string          `json:"pool"`
}

type NFTIncreaseLiquidityEvent struct {
	RawEvent  *types.Log      `json:"raw_event"`
	TokenID   uint64          `json:"token_id"`
	Liquidity decimal.Decimal `json:"liquidity"`
	Amount0   decimal.Decimal `json:"amount0"`
	Amount1   decimal.Decimal `json:"amount1"`
}

type NFTDecreaseLiquidityEvent struct {
	RawEvent  *types.Log      `json:"raw_event"`
	TokenID   uint64          `json:"token_id"`
	Liquidity decimal.Decimal `json:"liquidity"`
	Amount0   decimal.Decimal `json:"amount0"`
	Amount1   decimal.Decimal `json:"amount1"`
}

type NFTCollectEvent struct {
	RawEvent *types.Log      `json:"raw_event"`
	TokenID  uint64          `json:"token_id"`
	Amount0  decimal.Decimal `json:"amount0"`
	Amount1  decimal.Decimal `json:"amount1"`
}

type NFTTransferEvent struct {
	RawEvent *types.Log `json:"raw_event"`
	TokenID  uint64     `json:"token_id"`
	From     string     `json:"from"`
	To       string     `json:"to"`
}

// Event signature constants
var (
	// NonfungiblePositionManager events
	NonfungiblePositionManagerMintSig              = common.HexToHash("0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde")
	NonfungiblePositionManagerIncreaseLiquiditySig = common.HexToHash("0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f")
	NonfungiblePositionManagerDecreaseLiquiditySig = common.HexToHash("0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4")
	NonfungiblePositionManagerCollectSig           = common.HexToHash("0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01")
	NonfungiblePositionManagerTransferSig          = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

	// ABI types for event parsing
	uint256, _ = abi.NewType("uint256", "", nil)
)

// Parse NFTMintEvent - event Mint(tokenId, owner, tickLower, tickUpper, pool, amount)
func parseNFTMintEvent(log *types.Log) (*NFTMintEvent, error) {
	if len(log.Topics) < 2 {
		return nil, fmt.Errorf("not enough topics for NFT Mint event")
	}

	data := log.Data

	// Parse tokenID from topics
	tokenIDRaw, err := abi.ReadInteger(uint256, log.Topics[1].Bytes())
	if err != nil {
		return nil, err
	}
	tokenID, ok := tokenIDRaw.(*big.Int)
	if !ok {
		return nil, fmt.Errorf("failed to parse token ID")
	}

	// Parse owner, tickLower, tickUpper, pool, amount from data
	owner := common.BytesToAddress(data[:32])

	tickLowerRaw := big.NewInt(0).SetBytes(data[32:64])
	tickUpperRaw := big.NewInt(0).SetBytes(data[64:96])
	pool := common.BytesToAddress(data[96:128])
	amount := decimal.NewFromBigInt(big.NewInt(0).SetBytes(data[128:160]), 0)

	return &NFTMintEvent{
		RawEvent:  log,
		TokenID:   tokenID.Uint64(),
		Owner:     strings.ToLower(owner.Hex()),
		TickLower: int(tickLowerRaw.Int64()),
		TickUpper: int(tickUpperRaw.Int64()),
		Amount:    amount,
		Pool:      strings.ToLower(pool.Hex()),
	}, nil
}

// Parse NFTIncreaseLiquidityEvent - event IncreaseLiquidity(tokenId, liquidity, amount0, amount1)
func parseNFTIncreaseLiquidityEvent(log *types.Log) (*NFTIncreaseLiquidityEvent, error) {
	if len(log.Topics) < 2 {
		return nil, fmt.Errorf("not enough topics for NFT IncreaseLiquidity event")
	}

	data := log.Data

	// Parse tokenID from topics
	tokenIDRaw, err := abi.ReadInteger(uint256, log.Topics[1].Bytes())
	if err != nil {
		return nil, err
	}
	tokenID, ok := tokenIDRaw.(*big.Int)
	if !ok {
		return nil, fmt.Errorf("failed to parse token ID")
	}

	// Parse liquidity, amount0, amount1 from data
	liquidity := decimal.NewFromBigInt(big.NewInt(0).SetBytes(data[:32]), 0)
	amount0 := decimal.NewFromBigInt(big.NewInt(0).SetBytes(data[32:64]), 0)
	amount1 := decimal.NewFromBigInt(big.NewInt(0).SetBytes(data[64:96]), 0)

	return &NFTIncreaseLiquidityEvent{
		RawEvent:  log,
		TokenID:   tokenID.Uint64(),
		Liquidity: liquidity,
		Amount0:   amount0,
		Amount1:   amount1,
	}, nil
}

// Parse NFTDecreaseLiquidityEvent - event DecreaseLiquidity(tokenId, liquidity, amount0, amount1)
func parseNFTDecreaseLiquidityEvent(log *types.Log) (*NFTDecreaseLiquidityEvent, error) {
	if len(log.Topics) < 2 {
		return nil, fmt.Errorf("not enough topics for NFT DecreaseLiquidity event")
	}

	data := log.Data

	// Parse tokenID from topics
	tokenIDRaw, err := abi.ReadInteger(uint256, log.Topics[1].Bytes())
	if err != nil {
		return nil, err
	}
	tokenID, ok := tokenIDRaw.(*big.Int)
	if !ok {
		return nil, fmt.Errorf("failed to parse token ID")
	}

	// Parse liquidity, amount0, amount1 from data
	liquidity := decimal.NewFromBigInt(big.NewInt(0).SetBytes(data[:32]), 0)
	amount0 := decimal.NewFromBigInt(big.NewInt(0).SetBytes(data[32:64]), 0)
	amount1 := decimal.NewFromBigInt(big.NewInt(0).SetBytes(data[64:96]), 0)

	return &NFTDecreaseLiquidityEvent{
		RawEvent:  log,
		TokenID:   tokenID.Uint64(),
		Liquidity: liquidity,
		Amount0:   amount0,
		Amount1:   amount1,
	}, nil
}

// Parse NFTCollectEvent - event Collect(tokenId, amount0, amount1)
func parseNFTCollectEvent(log *types.Log) (*NFTCollectEvent, error) {
	if len(log.Topics) < 2 {
		return nil, fmt.Errorf("not enough topics for NFT Collect event")
	}

	data := log.Data

	// Parse tokenID from topics
	tokenIDRaw, err := abi.ReadInteger(uint256, log.Topics[1].Bytes())
	if err != nil {
		return nil, err
	}
	tokenID, ok := tokenIDRaw.(*big.Int)
	if !ok {
		return nil, fmt.Errorf("failed to parse token ID")
	}

	// Parse amount0, amount1 from data
	amount0 := decimal.NewFromBigInt(big.NewInt(0).SetBytes(data[:32]), 0)
	amount1 := decimal.NewFromBigInt(big.NewInt(0).SetBytes(data[32:64]), 0)

	return &NFTCollectEvent{
		RawEvent: log,
		TokenID:  tokenID.Uint64(),
		Amount0:  amount0,
		Amount1:  amount1,
	}, nil
}

// Parse NFTTransferEvent - event Transfer(from, to, tokenId)
func parseNFTTransferEvent(log *types.Log) (*NFTTransferEvent, error) {
	if len(log.Topics) < 4 {
		return nil, fmt.Errorf("not enough topics for NFT Transfer event")
	}

	// Parse from, to, tokenID from topics
	from := common.BytesToAddress(log.Topics[1].Bytes())
	to := common.BytesToAddress(log.Topics[2].Bytes())

	tokenIDRaw, err := abi.ReadInteger(uint256, log.Topics[3].Bytes())
	if err != nil {
		return nil, err
	}
	tokenID, ok := tokenIDRaw.(*big.Int)
	if !ok {
		return nil, fmt.Errorf("failed to parse token ID")
	}

	return &NFTTransferEvent{
		RawEvent: log,
		TokenID:  tokenID.Uint64(),
		From:     strings.ToLower(from.Hex()),
		To:       strings.ToLower(to.Hex()),
	}, nil
}
