package uniswap_v3_simulator

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/shopspring/decimal"
)

// TokenPosition is similar to Position but includes a tokenID for NonfungiblePositionManager
type TokenPosition struct {
	TokenID                  uint64          // NFT tokenID
	Owner                    string          // Owner address
	Pool                     string          // Pool address
	TickLower                int             // Lower tick boundary
	TickUpper                int             // Upper tick boundary
	Liquidity                decimal.Decimal // Current liquidity
	FeeGrowthInside0LastX128 decimal.Decimal
	FeeGrowthInside1LastX128 decimal.Decimal
	TokensOwed0              decimal.Decimal
	TokensOwed1              decimal.Decimal
}

func NewTokenPosition(tokenID uint64, owner string, pool string, tickLower int, tickUpper int) *TokenPosition {
	return &TokenPosition{
		TokenID:                  tokenID,
		Owner:                    owner,
		Pool:                     pool,
		TickLower:                tickLower,
		TickUpper:                tickUpper,
		Liquidity:                ZERO,
		FeeGrowthInside0LastX128: ZERO,
		FeeGrowthInside1LastX128: ZERO,
		TokensOwed0:              ZERO,
		TokensOwed1:              ZERO,
	}
}

func (p *TokenPosition) Clone() *TokenPosition {
	return &TokenPosition{
		TokenID:                  p.TokenID,
		Owner:                    p.Owner,
		Pool:                     p.Pool,
		TickLower:                p.TickLower,
		TickUpper:                p.TickUpper,
		Liquidity:                p.Liquidity,
		FeeGrowthInside0LastX128: p.FeeGrowthInside0LastX128,
		FeeGrowthInside1LastX128: p.FeeGrowthInside1LastX128,
		TokensOwed0:              p.TokensOwed0,
		TokensOwed1:              p.TokensOwed1,
	}
}

// IncreaseLiquidity adds liquidity to the position
func (p *TokenPosition) IncreaseLiquidity(
	liquidityDelta decimal.Decimal,
	feeGrowthInside0X128 decimal.Decimal,
	feeGrowthInside1X128 decimal.Decimal,
) error {
	if liquidityDelta.IsZero() || liquidityDelta.IsNegative() {
		return errors.New("liquidity delta must be positive")
	}

	// Calculate fees earned up to this point
	tokensOwed0 := feeGrowthInside0X128.Sub(p.FeeGrowthInside0LastX128).Mul(p.Liquidity).Div(Q128).RoundDown(0)
	tokensOwed1 := feeGrowthInside1X128.Sub(p.FeeGrowthInside1LastX128).Mul(p.Liquidity).Div(Q128).RoundDown(0)

	// Update position
	liquidityNext, err := LiquidityAddDelta(p.Liquidity, liquidityDelta)
	if err != nil {
		return err
	}
	p.Liquidity = liquidityNext
	p.FeeGrowthInside0LastX128 = feeGrowthInside0X128
	p.FeeGrowthInside1LastX128 = feeGrowthInside1X128

	// Add fees to accumulated fees
	if tokensOwed0.GreaterThan(ZERO) || tokensOwed1.GreaterThan(ZERO) {
		p.TokensOwed0 = p.TokensOwed0.Add(tokensOwed0)
		p.TokensOwed1 = p.TokensOwed1.Add(tokensOwed1)
	}
	return nil
}

// DecreaseLiquidity removes liquidity from the position
func (p *TokenPosition) DecreaseLiquidity(
	liquidityDelta decimal.Decimal,
	feeGrowthInside0X128 decimal.Decimal,
	feeGrowthInside1X128 decimal.Decimal,
	amount0 decimal.Decimal,
	amount1 decimal.Decimal,
) error {
	if liquidityDelta.IsZero() || liquidityDelta.IsPositive() {
		return errors.New("liquidity delta must be negative")
	}

	if p.Liquidity.LessThan(liquidityDelta.Abs()) {
		return errors.New("liquidity underflow")
	}

	// Calculate fees earned up to this point
	tokensOwed0 := feeGrowthInside0X128.Sub(p.FeeGrowthInside0LastX128).Mul(p.Liquidity).Div(Q128).RoundDown(0)
	tokensOwed1 := feeGrowthInside1X128.Sub(p.FeeGrowthInside1LastX128).Mul(p.Liquidity).Div(Q128).RoundDown(0)

	// Update position
	liquidityNext, err := LiquidityAddDelta(p.Liquidity, liquidityDelta)
	if err != nil {
		return err
	}
	p.Liquidity = liquidityNext
	p.FeeGrowthInside0LastX128 = feeGrowthInside0X128
	p.FeeGrowthInside1LastX128 = feeGrowthInside1X128

	// Add fees and withdrawn amounts to owed tokens
	p.TokensOwed0 = p.TokensOwed0.Add(tokensOwed0).Add(amount0)
	p.TokensOwed1 = p.TokensOwed1.Add(tokensOwed1).Add(amount1)

	return nil
}

// Collect withdraws tokens owed from the position
func (p *TokenPosition) Collect(amount0Requested, amount1Requested decimal.Decimal) (decimal.Decimal, decimal.Decimal) {
	var amount0 decimal.Decimal
	if amount0Requested.GreaterThan(p.TokensOwed0) {
		amount0 = p.TokensOwed0
	} else {
		amount0 = amount0Requested
	}

	var amount1 decimal.Decimal
	if amount1Requested.GreaterThan(p.TokensOwed1) {
		amount1 = p.TokensOwed1
	} else {
		amount1 = amount1Requested
	}

	// Update tokens owed
	p.TokensOwed0 = p.TokensOwed0.Sub(amount0)
	p.TokensOwed1 = p.TokensOwed1.Sub(amount1)

	return amount0, amount1
}

func (p *TokenPosition) IsEmpty() bool {
	return p.Liquidity.IsZero() && p.TokensOwed0.IsZero() && p.TokensOwed1.IsZero()
}

// TokenPositionManager manages positions by tokenID for the NonfungiblePositionManager
type TokenPositionManager struct {
	Positions map[uint64]*TokenPosition // Map tokenID -> Position
	// Index to lookup tokens by owner
	OwnerTokens map[string][]uint64 // Map owner -> list of tokenIDs
	// Index to lookup tokens by pool
	PoolTokens map[string][]uint64 // Map pool -> list of tokenIDs
}

func NewTokenPositionManager() *TokenPositionManager {
	return &TokenPositionManager{
		Positions:   map[uint64]*TokenPosition{},
		OwnerTokens: map[string][]uint64{},
		PoolTokens:  map[string][]uint64{},
	}
}

func (tpm *TokenPositionManager) Clone() *TokenPositionManager {
	newTpm := NewTokenPositionManager()

	// Clone positions
	positions := make(map[uint64]*TokenPosition, len(tpm.Positions))
	for tokenID, position := range tpm.Positions {
		positions[tokenID] = position.Clone()
	}
	newTpm.Positions = positions

	// Clone owner index
	ownerTokens := make(map[string][]uint64, len(tpm.OwnerTokens))
	for owner, tokens := range tpm.OwnerTokens {
		tokensCopy := make([]uint64, len(tokens))
		copy(tokensCopy, tokens)
		ownerTokens[owner] = tokensCopy
	}
	newTpm.OwnerTokens = ownerTokens

	// Clone pool index
	poolTokens := make(map[string][]uint64, len(tpm.PoolTokens))
	for pool, tokens := range tpm.PoolTokens {
		tokensCopy := make([]uint64, len(tokens))
		copy(tokensCopy, tokens)
		poolTokens[pool] = tokensCopy
	}
	newTpm.PoolTokens = poolTokens

	return newTpm
}

// CreatePosition creates a new position with the given tokenID
func (tpm *TokenPositionManager) CreatePosition(tokenID uint64, owner string, pool string, tickLower int, tickUpper int) *TokenPosition {
	position := NewTokenPosition(tokenID, owner, pool, tickLower, tickUpper)
	tpm.Positions[tokenID] = position

	// Update owner index
	tpm.OwnerTokens[owner] = append(tpm.OwnerTokens[owner], tokenID)

	// Update pool index
	tpm.PoolTokens[pool] = append(tpm.PoolTokens[pool], tokenID)

	return position
}

// GetPosition returns the position with the given tokenID
func (tpm *TokenPositionManager) GetPosition(tokenID uint64) (*TokenPosition, bool) {
	position, exists := tpm.Positions[tokenID]
	return position, exists
}

// GetPositionByOwner returns all positions owned by the given address
func (tpm *TokenPositionManager) GetPositionsByOwner(owner string) []*TokenPosition {
	tokenIDs, exists := tpm.OwnerTokens[owner]
	if !exists {
		return []*TokenPosition{}
	}

	positions := make([]*TokenPosition, 0, len(tokenIDs))
	for _, tokenID := range tokenIDs {
		if position, exists := tpm.Positions[tokenID]; exists {
			positions = append(positions, position)
		}
	}

	return positions
}

// GetPositionsByPool returns all positions for a given pool
func (tpm *TokenPositionManager) GetPositionsByPool(pool string) []*TokenPosition {
	tokenIDs, exists := tpm.PoolTokens[pool]
	if !exists {
		return []*TokenPosition{}
	}

	positions := make([]*TokenPosition, 0, len(tokenIDs))
	for _, tokenID := range tokenIDs {
		if position, exists := tpm.Positions[tokenID]; exists {
			positions = append(positions, position)
		}
	}

	return positions
}

// HandleMint processes a mint event from the NonfungiblePositionManager
func (tpm *TokenPositionManager) HandleMint(tokenID uint64, owner string, pool string, tickLower int, tickUpper int, amount decimal.Decimal, feeGrowthInside0X128 decimal.Decimal, feeGrowthInside1X128 decimal.Decimal) error {
	// Check if position already exists
	if position, exists := tpm.Positions[tokenID]; exists {
		return position.IncreaseLiquidity(amount, feeGrowthInside0X128, feeGrowthInside1X128)
	}

	// Create new position
	position := tpm.CreatePosition(tokenID, owner, pool, tickLower, tickUpper)
	return position.IncreaseLiquidity(amount, feeGrowthInside0X128, feeGrowthInside1X128)
}

// HandleIncreaseLiquidity processes an increase liquidity event
func (tpm *TokenPositionManager) HandleIncreaseLiquidity(tokenID uint64, amount decimal.Decimal, feeGrowthInside0X128 decimal.Decimal, feeGrowthInside1X128 decimal.Decimal) error {
	position, exists := tpm.Positions[tokenID]
	if !exists {
		return fmt.Errorf("position with tokenID %d does not exist", tokenID)
	}

	return position.IncreaseLiquidity(amount, feeGrowthInside0X128, feeGrowthInside1X128)
}

// HandleDecreaseLiquidity processes a decrease liquidity event
func (tpm *TokenPositionManager) HandleDecreaseLiquidity(tokenID uint64, liquidityDelta decimal.Decimal, feeGrowthInside0X128 decimal.Decimal, feeGrowthInside1X128 decimal.Decimal, amount0 decimal.Decimal, amount1 decimal.Decimal) error {
	position, exists := tpm.Positions[tokenID]
	if !exists {
		return fmt.Errorf("position with tokenID %d does not exist", tokenID)
	}

	return position.DecreaseLiquidity(liquidityDelta.Neg(), feeGrowthInside0X128, feeGrowthInside1X128, amount0, amount1)
}

// HandleCollect processes a collect event
func (tpm *TokenPositionManager) HandleCollect(tokenID uint64, amount0Requested decimal.Decimal, amount1Requested decimal.Decimal) (decimal.Decimal, decimal.Decimal, error) {
	position, exists := tpm.Positions[tokenID]
	if !exists {
		return ZERO, ZERO, fmt.Errorf("position with tokenID %d does not exist", tokenID)
	}

	amount0, amount1 := position.Collect(amount0Requested, amount1Requested)

	// If position is now empty, we could remove it, but we keep it for history

	return amount0, amount1, nil
}

// HandleTransfer processes a transfer event (change of ownership)
func (tpm *TokenPositionManager) HandleTransfer(tokenID uint64, from string, to string) error {
	position, exists := tpm.Positions[tokenID]
	if !exists {
		return fmt.Errorf("position with tokenID %d does not exist", tokenID)
	}

	// Update owner in position
	oldOwner := position.Owner
	position.Owner = to

	// Update owner index
	// Remove from old owner
	if oldOwner != from {
		return fmt.Errorf("token owner mismatch: expected %s, got %s", oldOwner, from)
	}

	oldOwnerTokens := tpm.OwnerTokens[from]
	for i, tid := range oldOwnerTokens {
		if tid == tokenID {
			// Remove by swapping with the last element and truncating
			oldOwnerTokens[i] = oldOwnerTokens[len(oldOwnerTokens)-1]
			tpm.OwnerTokens[from] = oldOwnerTokens[:len(oldOwnerTokens)-1]
			break
		}
	}

	// Add to new owner
	tpm.OwnerTokens[to] = append(tpm.OwnerTokens[to], tokenID)

	return nil
}

// GormDataType for GORM integration
func (tpm *TokenPositionManager) GormDataType() string {
	return "LONGTEXT"
}

// Scan for GORM integration
func (tpm *TokenPositionManager) Scan(value interface{}) error {
	var err error
	switch v := value.(type) {
	case []byte:
		err = json.Unmarshal(v, tpm)
	case string:
		err = json.Unmarshal([]byte(v), tpm)
	case nil:
		return nil
	default:
		err = errors.New(fmt.Sprint("Failed to unmarshal TokenPositionManager value:", value))
	}
	return err
}

// Value for GORM integration
func (tpm *TokenPositionManager) Value() (driver.Value, error) {
	bs, err := json.Marshal(tpm)
	if err != nil {
		return nil, err
	}
	return string(bs), nil
}
