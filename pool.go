package uniswap_v3_simulator

import (
	"errors"
	"fmt"
	"github.com/daoleno/uniswapv3-sdk/constants"
	"github.com/daoleno/uniswapv3-sdk/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"time"
)

type FeeAmount int

// pool config
type PoolConfig struct {
	TickSpacing int64
	Token0      common.Address
	Token1      common.Address
	Fee         FeeAmount
}

func NewPoolConfig(
	TickSpacing int64,
	Token0 common.Address,
	Token1 common.Address,
	Fee FeeAmount,
) *PoolConfig {
	return &PoolConfig{
		TickSpacing: TickSpacing,
		Token0:      Token0,
		Token1:      Token1,
		Fee:         Fee,
	}
}

// core pool
type CorePool struct {
	gorm.Model
	PoolAddress          string `gorm:"index"`
	HasCreated           bool   // has created in db, Flush will set to true
	Token0               string
	Token1               string
	Fee                  FeeAmount
	TickSpacing          int
	MaxLiquidityPerTick  decimal.Decimal
	CurrentBlockNum      uint64 `gorm:"index"`
	DeployBlockNum       uint64 `gorm:"index"`
	Token0Balance        decimal.Decimal
	Token1Balance        decimal.Decimal
	SqrtPriceX96         decimal.Decimal
	Liquidity            decimal.Decimal
	TickCurrent          int
	FeeGrowthGlobal0X128 decimal.Decimal
	FeeGrowthGlobal1X128 decimal.Decimal
	TickManager          *TickManager
	PositionManager      *PositionManager
}

func (p *CorePool) Clone() *CorePool {
	newPool := &CorePool{
		PoolAddress:          p.PoolAddress,
		HasCreated:           p.HasCreated,
		Token0:               p.Token0,
		Token1:               p.Token1,
		Fee:                  p.Fee,
		TickSpacing:          p.TickSpacing,
		MaxLiquidityPerTick:  p.MaxLiquidityPerTick,
		CurrentBlockNum:      p.CurrentBlockNum,
		DeployBlockNum:       p.DeployBlockNum,
		Token0Balance:        p.Token0Balance,
		Token1Balance:        p.Token1Balance,
		SqrtPriceX96:         p.SqrtPriceX96,
		Liquidity:            p.Liquidity,
		TickCurrent:          p.TickCurrent,
		FeeGrowthGlobal0X128: p.FeeGrowthGlobal0X128,
		FeeGrowthGlobal1X128: p.FeeGrowthGlobal1X128,
		TickManager:          p.TickManager.Clone(),
		PositionManager:      p.PositionManager.Clone(),
	}
	return newPool
}

func NewCorePoolFromConfig(addr string, config PoolConfig) *CorePool {
	return &CorePool{
		PoolAddress:          addr,
		Token0:               config.Token0.String(),
		Token1:               config.Token1.String(),
		Fee:                  config.Fee,
		TickSpacing:          int(config.TickSpacing),
		MaxLiquidityPerTick:  TickSpacingToMaxLiquidityPerTick(int(config.TickSpacing)),
		Token0Balance:        ZERO,
		Token1Balance:        ZERO,
		SqrtPriceX96:         ZERO,
		Liquidity:            ZERO,
		TickCurrent:          0,
		FeeGrowthGlobal0X128: ZERO,
		FeeGrowthGlobal1X128: ZERO,
		TickManager:          NewTickManager(),
		PositionManager:      NewPositionManager(),
	}
}

func (p *CorePool) Initialize(sqrtPriceX96 decimal.Decimal) error {
	if !p.SqrtPriceX96.IsZero() {
		return errors.New("Already initialized!")
	}
	var err error
	p.TickCurrent, err = GetTickAtSqrtRatio(sqrtPriceX96)
	if err != nil {
		return err
	}
	p.SqrtPriceX96 = sqrtPriceX96
	return nil
}

// 从链上同步数据， 并保存snapshot到数据库(覆盖上一个snapshot)
// 从数据库加载snapshot， 然后检查和最新区块的差距, 并同步到最新区块
// 如果数据库中没有snapshot，则从initialize开始同步所有event
func (p *CorePool) Load() error {
	if p.DeployBlockNum == 0 {
		// todo etherscan api 获取 部署blockNum
	}
	return nil
}

func (p *CorePool) Mint(recipient string, tickLower, tickUpper int, amount decimal.Decimal) (decimal.Decimal, decimal.Decimal, error) {
	if !amount.GreaterThan(ZERO) {
		return ZERO, ZERO, errors.New("Mint amount should greater than 0")
	}

	_, amount0, amount1, err := p.modifyPosition(recipient, tickLower, tickUpper, amount)
	if err != nil {
		return ZERO, ZERO, err
	}
	return amount0, amount1, nil
}
func (p *CorePool) Burn(owner string, tickLower, tickUpper int, amount decimal.Decimal) (decimal.Decimal, decimal.Decimal, error) {
	position, amount0, amount1, err := p.modifyPosition(owner, tickLower, tickUpper, amount.Neg())
	if err != nil {
		return ZERO, ZERO, err
	}
	amount0 = amount0.Neg()
	amount1 = amount1.Neg()
	if amount0.IsPositive() || amount1.IsPositive() {
		newTokensOwed0 := position.TokensOwed0.Add(amount0)
		newTokensOwed1 := position.TokensOwed1.Add(amount1)
		position.UpdateBurn(newTokensOwed0, newTokensOwed1)
	}
	return amount0, amount1, nil
}

func (p *CorePool) Collect(recipient string, tickLower, tickUpper int, amount0Req, amount1Req decimal.Decimal) (decimal.Decimal, decimal.Decimal, error) {
	err := p.checkTicks(tickLower, tickUpper)
	if err != nil {
		return ZERO, ZERO, err
	}
	return p.PositionManager.CollectPosition(recipient, tickLower, tickUpper, amount0Req, amount1Req)
}

type swapState struct {
	amountSpecifiedRemaining decimal.Decimal
	amountCalculated         decimal.Decimal
	sqrtPriceX96             decimal.Decimal
	tick                     int
	liquidity                decimal.Decimal
	feeGrowthGlobalX128      decimal.Decimal
}
type StepComputations struct {
	sqrtPriceStartX96 decimal.Decimal
	tickNext          int
	initialized       bool
	sqrtPriceNextX96  decimal.Decimal
	amountIn          decimal.Decimal
	amountOut         decimal.Decimal
	feeAmount         decimal.Decimal
}

func (p *CorePool) HandleSwap(zeroForOne bool, amountSpecified decimal.Decimal, optionalSqrtPriceLimitX96 *decimal.Decimal, isStatic bool) (decimal.Decimal, decimal.Decimal, decimal.Decimal, error) {
	// Set price limit based on direction if not provided
	var sqrtPriceLimitX96 decimal.Decimal
	if optionalSqrtPriceLimitX96 == nil {
		if zeroForOne {
			sqrtPriceLimitX96 = MIN_SQRT_RATIO.Add(ONE)
		} else {
			sqrtPriceLimitX96 = MAX_SQRT_RATIO.Sub(ONE)
		}
	} else {
		sqrtPriceLimitX96 = *optionalSqrtPriceLimitX96
	}

	// Validate price limits
	if zeroForOne {
		if !sqrtPriceLimitX96.GreaterThan(MIN_SQRT_RATIO) {
			return ZERO, ZERO, ZERO, fmt.Errorf("price limit (%s) below minimum allowed ratio (%s)", sqrtPriceLimitX96, MIN_SQRT_RATIO)
		}
		if !sqrtPriceLimitX96.LessThan(p.SqrtPriceX96) {
			return ZERO, ZERO, ZERO, fmt.Errorf("price limit (%s) must be less than current price (%s) for token0 -> token1 swap", sqrtPriceLimitX96, p.SqrtPriceX96)
		}
	} else {
		if !sqrtPriceLimitX96.LessThan(MAX_SQRT_RATIO) {
			return ZERO, ZERO, ZERO, fmt.Errorf("price limit (%s) above maximum allowed ratio (%s)", sqrtPriceLimitX96, MAX_SQRT_RATIO)
		}
		if !sqrtPriceLimitX96.GreaterThan(p.SqrtPriceX96) {
			return ZERO, ZERO, ZERO, fmt.Errorf("price limit (%s) must be greater than current price (%s) for token1 -> token0 swap", sqrtPriceLimitX96, p.SqrtPriceX96)
		}
	}

	// Determine exact input vs exact output
	exactInput := amountSpecified.GreaterThanOrEqual(ZERO)

	// Initialize swap state
	state := swapState{
		amountSpecifiedRemaining: amountSpecified,
		amountCalculated:         ZERO,
		sqrtPriceX96:             p.SqrtPriceX96,
		tick:                     p.TickCurrent,
		liquidity:                p.Liquidity,
	}

	// Initialize fee growth value based on swap direction
	if zeroForOne {
		state.feeGrowthGlobalX128 = p.FeeGrowthGlobal0X128
	} else {
		state.feeGrowthGlobalX128 = p.FeeGrowthGlobal1X128
	}

	// Debug log swap initiation if in debug mode
	if logrus.GetLevel() >= logrus.DebugLevel {
		logrus.Debugf("Initiating swap: zeroForOne=%t, exactInput=%t, amountSpecified=%s, currentPrice=%s, limitPrice=%s",
			zeroForOne, exactInput, amountSpecified, p.SqrtPriceX96, sqrtPriceLimitX96)
	}

	// Main swap loop - continue until amount is used up or price limit is reached
	loopCount := 0
	for !(state.amountSpecifiedRemaining.Equal(ZERO) || state.sqrtPriceX96.Equal(sqrtPriceLimitX96)) {
		// Safety check for infinite loops
		loopCount++
		if loopCount > 1000 {
			return ZERO, ZERO, ZERO, fmt.Errorf("excessive loop iterations in swap calculation (>1000)")
		}

		step := StepComputations{
			sqrtPriceStartX96: state.sqrtPriceX96,
			tickNext:          0,
			initialized:       false,
			sqrtPriceNextX96:  ZERO,
			amountIn:          ZERO,
			amountOut:         ZERO,
			feeAmount:         ZERO,
		}

		// Find the next initialized tick
		tickNext, initialized, err := p.TickManager.GetNextInitializedTick(state.tick, p.TickSpacing, zeroForOne)
		if err != nil {
			return ZERO, ZERO, ZERO, fmt.Errorf("error finding next tick: %w", err)
		}

		step.tickNext = tickNext
		step.initialized = initialized

		// Ensure we stay within valid tick bounds
		if step.tickNext < MIN_TICK {
			step.tickNext = MIN_TICK
		} else if step.tickNext > MAX_TICK {
			step.tickNext = MAX_TICK
		}

		// Get the sqrt price at the next tick
		sqrtPriceNextX96bi, err := utils.GetSqrtRatioAtTick(step.tickNext)
		if err != nil {
			return ZERO, ZERO, ZERO, fmt.Errorf("error getting sqrt ratio at tick %d: %w", step.tickNext, err)
		}
		step.sqrtPriceNextX96 = decimal.NewFromBigInt(sqrtPriceNextX96bi, 0)

		// Determine target price for this step
		var sqrtRatioTargetX96 decimal.Decimal
		if zeroForOne {
			if step.sqrtPriceNextX96.LessThan(sqrtPriceLimitX96) {
				sqrtRatioTargetX96 = sqrtPriceLimitX96
			} else {
				sqrtRatioTargetX96 = step.sqrtPriceNextX96
			}
		} else {
			if step.sqrtPriceNextX96.GreaterThan(sqrtPriceLimitX96) {
				sqrtRatioTargetX96 = sqrtPriceLimitX96
			} else {
				sqrtRatioTargetX96 = step.sqrtPriceNextX96
			}
		}

		// Compute the actual swap step
		_sqrtPriceX96, _amountIn, _amountOut, _feeAmount, err := utils.ComputeSwapStep(
			state.sqrtPriceX96.BigInt(),
			sqrtRatioTargetX96.BigInt(),
			state.liquidity.BigInt(),
			state.amountSpecifiedRemaining.BigInt(),
			constants.FeeAmount(p.Fee),
		)
		if err != nil {
			return ZERO, ZERO, ZERO, fmt.Errorf("error computing swap step: %w", err)
		}

		// Update state based on step computation
		state.sqrtPriceX96 = decimal.NewFromBigInt(_sqrtPriceX96, 0)
		step.amountIn = decimal.NewFromBigInt(_amountIn, 0)
		step.amountOut = decimal.NewFromBigInt(_amountOut, 0)
		step.feeAmount = decimal.NewFromBigInt(_feeAmount, 0)

		// Update remaining amounts based on exact input or output
		if exactInput {
			state.amountSpecifiedRemaining = state.amountSpecifiedRemaining.Sub(step.amountIn.Add(step.feeAmount))
			state.amountCalculated = state.amountCalculated.Sub(step.amountOut)
		} else {
			state.amountSpecifiedRemaining = state.amountSpecifiedRemaining.Add(step.amountOut)
			state.amountCalculated = state.amountCalculated.Add(step.amountIn.Add(step.feeAmount))
		}

		// Update fee growth if there's liquidity
		if state.liquidity.IsPositive() {
			feeGrowthDelta := step.feeAmount.Mul(Q128).Div(state.liquidity)
			// Make sure to round down to avoid overcharging fees
			state.feeGrowthGlobalX128 = state.feeGrowthGlobalX128.Add(feeGrowthDelta.RoundDown(0))
		}

		// Handle crossing tick boundary
		if state.sqrtPriceX96.Equal(step.sqrtPriceNextX96) {
			if step.initialized {
				// Get the tick and handle crossing it
				nextTick, err := p.TickManager.GetTickAndInitIfAbsent(step.tickNext)
				if err != nil {
					return ZERO, ZERO, ZERO, fmt.Errorf("error getting tick %d: %w", step.tickNext, err)
				}

				var liquidityNet decimal.Decimal
				if isStatic {
					// For simulation, just read the value without updating state
					liquidityNet = nextTick.LiquidityNet
				} else {
					// For actual swap, cross the tick and update fee growth
					if zeroForOne {
						liquidityNet = nextTick.Cross(state.feeGrowthGlobalX128, p.FeeGrowthGlobal1X128)
					} else {
						liquidityNet = nextTick.Cross(p.FeeGrowthGlobal0X128, state.feeGrowthGlobalX128)
					}
				}

				// Adjust the sign of liquidity net based on swap direction
				if zeroForOne {
					liquidityNet = liquidityNet.Neg()
				}

				// Update the liquidity
				state.liquidity, err = AddDelta(state.liquidity, liquidityNet)
				if err != nil {
					return ZERO, ZERO, ZERO, fmt.Errorf("error updating liquidity at tick %d: %w", step.tickNext, err)
				}
			}

			// Update the current tick
			if zeroForOne {
				state.tick = step.tickNext - 1
			} else {
				state.tick = step.tickNext
			}
		} else if !state.sqrtPriceX96.Equal(step.sqrtPriceStartX96) {
			// If price changed but we didn't cross a tick, compute the new tick
			state.tick, err = GetTickAtSqrtRatio(state.sqrtPriceX96)
			if err != nil {
				return ZERO, ZERO, ZERO, fmt.Errorf("error computing tick at price %s: %w", state.sqrtPriceX96, err)
			}
		}

		// Debug logging for the step if needed
		if logrus.GetLevel() >= logrus.TraceLevel {
			logrus.Tracef("Swap step: tick=%d, price=%s, amountIn=%s, amountOut=%s, feeAmount=%s, liquidityRemaining=%s",
				state.tick, state.sqrtPriceX96, step.amountIn, step.amountOut, step.feeAmount, state.liquidity)
		}
	}

	// Update the pool state if this is not a static (simulation) swap
	if !isStatic {
		p.SqrtPriceX96 = state.sqrtPriceX96
		if state.tick != p.TickCurrent {
			p.TickCurrent = state.tick
		}
		if !state.liquidity.Equal(p.Liquidity) {
			p.Liquidity = state.liquidity
		}
		if zeroForOne {
			p.FeeGrowthGlobal0X128 = state.feeGrowthGlobalX128
		} else {
			p.FeeGrowthGlobal1X128 = state.feeGrowthGlobalX128
		}
	}

	// Calculate final amounts
	var amount0, amount1 decimal.Decimal
	if zeroForOne == exactInput {
		amount0 = amountSpecified.Sub(state.amountSpecifiedRemaining)
		amount1 = state.amountCalculated
	} else {
		amount0 = state.amountCalculated
		amount1 = amountSpecified.Sub(state.amountSpecifiedRemaining)
	}

	// Log the swap completion
	if logrus.GetLevel() >= logrus.DebugLevel && !isStatic {
		logrus.Debugf("Swap complete: amount0=%s, amount1=%s, newPrice=%s, newTick=%d",
			amount0, amount1, state.sqrtPriceX96, state.tick)
	}

	return amount0, amount1, state.sqrtPriceX96, nil
}

type SwapSolution struct {
	AmountSpecified   decimal.Decimal
	SqrtPriceLimitX96 *decimal.Decimal
}

func (p *CorePool) tryToDryRun(param *UniV3SwapEvent, amountSpec decimal.Decimal, sqrtPriceLimitX96 *decimal.Decimal) bool {
	// Determine direction of swap from amount0 (matches JS implementation)
	var zeroForOne = param.Amount0.IsPositive()

	// Try to execute the swap with our parameters
	amount0, amount1, priceX96, err := p.HandleSwap(zeroForOne, amountSpec, sqrtPriceLimitX96, true)
	if err != nil {
		// This is a dry run, so errors are expected for some parameter combinations
		if logrus.GetLevel() >= logrus.DebugLevel {
			logrus.Debugf("Dry run swap failed for tx: %s, error: %s", param.RawEvent.TxHash, err)
		}
		return false
	}

	// Check if our output exactly matches the observed event (all three values must match)
	result := amount0.Equal(param.Amount0) &&
		amount1.Equal(param.Amount1) &&
		priceX96.Equal(param.SqrtPriceX96)

	return result
}

func incTowardsInfinity(d decimal.Decimal) decimal.Decimal {
	if d.IsZero() {
		return d
	}

	// Simply add/subtract 1 (equivalent to JavaScript's FullMath.incrTowardInfinity)
	if d.IsPositive() {
		return d.Add(ONE)
	} else {
		return d.Sub(ONE)
	}
}
func (p *CorePool) ResolveInputFromSwapResultEvent(param *UniV3SwapEvent) (decimal.Decimal, *decimal.Decimal, error) {
	if param == nil {
		return ZERO, nil, fmt.Errorf("swap event is nil")
	}

	// Create a list to hold our potential solutions
	var solutionList []SwapSolution

	// Basic solutions without price limit (solution3, solution4 in JS)
	solution3 := SwapSolution{SqrtPriceLimitX96: nil, AmountSpecified: param.Amount0}
	solution4 := SwapSolution{SqrtPriceLimitX96: nil, AmountSpecified: param.Amount1}
	solutionList = append(solutionList, solution3, solution4)

	// Define solutions with price limits (solution1, solution2 in JS)
	solution1 := SwapSolution{SqrtPriceLimitX96: &param.SqrtPriceX96}
	solution2 := SwapSolution{SqrtPriceLimitX96: &param.SqrtPriceX96}

	// Handle zero liquidity or -1 liquidity cases
	if param.Liquidity.IsZero() {
		// When liquidity is zero, adjust the amount to match the event output
		solution1.AmountSpecified = incTowardsInfinity(param.Amount0)
		solution2.AmountSpecified = incTowardsInfinity(param.Amount1)
	} else {
		solution1.AmountSpecified = param.Amount0
		solution2.AmountSpecified = param.Amount1
	}

	// If the price changed during the swap, try additional solutions
	if !param.SqrtPriceX96.Equal(p.SqrtPriceX96) {
		// Special case for liquidity = -1 (which is how some contracts represent a special case)
		liquidityMinus1 := decimal.NewFromInt(-1)
		if param.Liquidity.Equal(liquidityMinus1) {
			// In JS code, these are solution5 and solution6
			solution5 := SwapSolution{AmountSpecified: param.Amount0, SqrtPriceLimitX96: &param.SqrtPriceX96}
			solution6 := SwapSolution{AmountSpecified: param.Amount1, SqrtPriceLimitX96: &param.SqrtPriceX96}
			solutionList = append(solutionList, solution5, solution6)
		}

		// Add the liquidity-adjusted solutions to the list (solution1, solution2)
		solutionList = append(solutionList, solution1, solution2)
	}

	// Try each solution
	for i, solution := range solutionList {
		// Check if this solution reproduces the observed swap event
		if p.tryToDryRun(param, solution.AmountSpecified, solution.SqrtPriceLimitX96) {
			if logrus.GetLevel() >= logrus.DebugLevel {
				logrus.Debugf("Found swap solution %d for tx: %s, pool: %s",
					i, param.RawEvent.TxHash, param.RawEvent.Address)
			}
			return solution.AmountSpecified, solution.SqrtPriceLimitX96, nil
		}
	}

	// If we've tried all solutions and none worked, log details and return error
	err := fmt.Errorf("failed to find swap solution for tx: %s, pool: %s, amount0: %s, amount1: %s, sqrtPrice: %s",
		param.RawEvent.TxHash, param.RawEvent.Address, param.Amount0, param.Amount1, param.SqrtPriceX96)
	logrus.Error(err)
	return ZERO, nil, err
}

func (p *CorePool) checkTicks(tickLower, tickUpper int) error {
	if !(tickLower < tickUpper) {
		return errors.New("tickLower should lower than tickUpper")
	}
	if !(tickLower >= MIN_TICK) {
		return errors.New("tickLower should NOT lower than MIN_TICK")
	}
	if !(tickUpper <= MAX_TICK) {
		return errors.New("tickUpper should NOT greater than MAX_TICK")
	}
	return nil
}

func (p *CorePool) modifyPosition(owner string, tickLower, tickUpper int, liquidityDelta decimal.Decimal) (*Position, decimal.Decimal, decimal.Decimal, error) {
	err := p.checkTicks(tickLower, tickUpper)
	if err != nil {
		return nil, ZERO, ZERO, err
	}
	amount0 := ZERO
	amount1 := ZERO
	positionView := p.PositionManager.GetPositionReadonly(owner, tickLower, tickUpper)
	if liquidityDelta.IsNegative() {
		negatedLiquidityDelta := liquidityDelta.Neg()
		if !positionView.Liquidity.GreaterThanOrEqual(negatedLiquidityDelta) {
			return nil, ZERO, ZERO, errors.New("Liquidity Underflow")
		}
	}
	position, err := p.updatePosition(owner, tickLower, tickUpper, liquidityDelta)
	if err != nil {
		return nil, ZERO, ZERO, err
	}
	if !liquidityDelta.IsZero() {
		if p.TickCurrent < tickLower {
			tmp1, err := GetSqrtRatioAtTick(tickLower)
			if err != nil {
				return nil, ZERO, ZERO, err
			}
			tmp2, err := GetSqrtRatioAtTick(tickUpper)
			if err != nil {
				return nil, ZERO, ZERO, err
			}
			amount0, err = GetAmount0Delta(tmp1, tmp2, liquidityDelta)
			if err != nil {
				return nil, ZERO, ZERO, err
			}
		} else if p.TickCurrent < tickUpper {
			tmp2, err := GetSqrtRatioAtTick(tickUpper)
			if err != nil {
				return nil, ZERO, ZERO, err
			}
			amount0, err = GetAmount0Delta(p.SqrtPriceX96, tmp2, liquidityDelta)
			if err != nil {
				return nil, ZERO, ZERO, err
			}
			tmp3, err := GetSqrtRatioAtTick(tickLower)
			if err != nil {
				return nil, ZERO, ZERO, err
			}
			amount1, err = GetAmount1Delta(tmp3, p.SqrtPriceX96, liquidityDelta)
			if err != nil {
				return nil, ZERO, ZERO, err
			}
			p.Liquidity, err = AddDelta(p.Liquidity, liquidityDelta)
			if err != nil {
				return nil, ZERO, ZERO, err
			}
		} else {
			tmp1, err := GetSqrtRatioAtTick(tickLower)
			if err != nil {
				return nil, ZERO, ZERO, err
			}
			tmp2, err := GetSqrtRatioAtTick(tickUpper)
			if err != nil {
				return nil, ZERO, ZERO, err
			}
			amount1, err = GetAmount1Delta(tmp1, tmp2, liquidityDelta)
			if err != nil {
				return nil, ZERO, ZERO, err
			}
		}
	}
	return position, amount0, amount1, nil
}

func (p *CorePool) updatePosition(owner string, lower int, upper int, delta decimal.Decimal) (*Position, error) {
	position := p.PositionManager.GetPositionAndInitIfAbsent(GetPositionKey(owner, lower, upper))
	flippedLower := false
	flippedUpper := false
	if !delta.IsZero() {
		tick, err := p.TickManager.GetTickAndInitIfAbsent(lower)
		if err != nil {
			return nil, err
		}
		flippedLower, err = tick.Update(delta, p.TickCurrent, p.FeeGrowthGlobal0X128, p.FeeGrowthGlobal1X128, false, p.MaxLiquidityPerTick)
		if err != nil {
			return nil, err
		}

		tick, err = p.TickManager.GetTickAndInitIfAbsent(upper)
		if err != nil {
			return nil, err
		}
		flippedUpper, err = tick.Update(delta, p.TickCurrent, p.FeeGrowthGlobal0X128, p.FeeGrowthGlobal1X128, true, p.MaxLiquidityPerTick)
		if err != nil {
			return nil, err
		}
	}
	fi0, fi1, err := p.TickManager.GetFeeGrowthInside(lower, upper, p.TickCurrent, p.FeeGrowthGlobal0X128, p.FeeGrowthGlobal1X128)
	if err != nil {
		return nil, err
	}
	err = position.Update(delta, fi0, fi1)
	if err != nil {
		return nil, err
	}
	if delta.IsNegative() {
		if flippedLower {
			p.TickManager.Clear(lower)
		}
		if flippedUpper {
			p.TickManager.Clear(upper)
		}
	}
	return position, nil
}

func (p *CorePool) Flush(db *gorm.DB) error {
	if p.HasCreated {
		return db.Model(p).Updates(map[string]interface{}{
			"current_block_num":       p.CurrentBlockNum,
			"token0_balance":          p.Token0Balance,
			"token1_balance":          p.Token1Balance,
			"sqrt_price_x96":          p.SqrtPriceX96,
			"liquidity":               p.Liquidity,
			"tick_current":            p.TickCurrent,
			"fee_growth_global0_x128": p.FeeGrowthGlobal0X128,
			"fee_growth_global1_x128": p.FeeGrowthGlobal1X128,
			"tick_manager":            p.TickManager,
			"position_manager":        p.PositionManager,
		}).Error
	} else {
		p.HasCreated = true
		return db.Create(p).Error
	}
}

type ActionType string

type Record struct {
	Id         string
	ActionType ActionType
	Params     interface{}
	Amount0    decimal.Decimal
	Amount1    decimal.Decimal
	Timestamp  time.Time
}
