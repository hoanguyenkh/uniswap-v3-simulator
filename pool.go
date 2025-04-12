package uniswap_v3_simulator

import (
	"errors"
	"fmt"
	"time"

	"github.com/daoleno/uniswapv3-sdk/constants"
	"github.com/daoleno/uniswapv3-sdk/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
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

	if zeroForOne {
		if !sqrtPriceLimitX96.GreaterThan(MIN_SQRT_RATIO) {
			return ZERO, ZERO, ZERO, errors.New("RATIO_MIN")
		}
		if !sqrtPriceLimitX96.LessThan(p.SqrtPriceX96) {
			return ZERO, ZERO, ZERO, errors.New("RATIO_CURRENT")
		}
	} else {
		if !sqrtPriceLimitX96.LessThan(MAX_SQRT_RATIO) {
			return ZERO, ZERO, ZERO, errors.New("RATIO_MAX")
		}
		if !sqrtPriceLimitX96.GreaterThan(p.SqrtPriceX96) {
			return ZERO, ZERO, ZERO, errors.New("RATIO_CURRENT")
		}
	}

	exactInput := amountSpecified.GreaterThanOrEqual(ZERO)
	state := swapState{
		amountSpecifiedRemaining: amountSpecified,
		amountCalculated:         ZERO,
		sqrtPriceX96:             p.SqrtPriceX96,
		tick:                     p.TickCurrent,
		liquidity:                p.Liquidity,
	}

	if zeroForOne {
		state.feeGrowthGlobalX128 = p.FeeGrowthGlobal0X128
	} else {
		state.feeGrowthGlobalX128 = p.FeeGrowthGlobal1X128
	}

	// Add loop protection variables
	iterationCount := 0
	const maxIterations = 1024 // Set a reasonable maximum

	// Add a very small threshold to prevent infinite looping due to decimal precision
	epsilonDecimal := decimal.NewFromFloat(0.00001)

	// 达到限价或者兑换完成
	for {
		// Add iteration counter to prevent infinite loops
		iterationCount++
		if iterationCount > maxIterations {
			logrus.Warnf("HandleSwap reached max iterations (%d) - forcing exit for tx %s", maxIterations, "unknown")
			break
		}

		// Check if we should exit the loop - with tolerance for floating point imprecision
		amountIsZero := state.amountSpecifiedRemaining.Abs().LessThanOrEqual(epsilonDecimal)
		priceReached := state.sqrtPriceX96.Sub(sqrtPriceLimitX96).Abs().Div(
			decimal.Max(state.sqrtPriceX96, sqrtPriceLimitX96)).LessThanOrEqual(epsilonDecimal)

		if amountIsZero || priceReached {
			break
		}

		step := StepComputations{
			sqrtPriceStartX96: ZERO, tickNext: 0, initialized: false, sqrtPriceNextX96: ZERO, amountIn: ZERO, amountOut: ZERO, feeAmount: ZERO}
		step.sqrtPriceStartX96 = state.sqrtPriceX96

		tickNext, initialized, err := p.TickManager.GetNextInitializedTick(state.tick, p.TickSpacing, zeroForOne)
		if err != nil {
			return ZERO, ZERO, ZERO, err
		}

		step.tickNext = tickNext
		step.initialized = initialized
		if step.tickNext < MIN_TICK {
			step.tickNext = MIN_TICK
		} else if step.tickNext > MAX_TICK {
			step.tickNext = MAX_TICK
		}
		sqrtPriceNextX96bi, err := utils.GetSqrtRatioAtTick(step.tickNext)
		if err != nil {
			return ZERO, ZERO, ZERO, err
		}
		step.sqrtPriceNextX96 = decimal.NewFromBigInt(sqrtPriceNextX96bi, 0)
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
		_sqrtPriceX96, _amountIn, _amountOut, _feeAmount, err := utils.ComputeSwapStep(state.sqrtPriceX96.BigInt(), sqrtRatioTargetX96.BigInt(), state.liquidity.BigInt(), state.amountSpecifiedRemaining.BigInt(), constants.FeeAmount(p.Fee))
		if err != nil {
			return ZERO, ZERO, ZERO, err
		}

		state.sqrtPriceX96 = decimal.NewFromBigInt(_sqrtPriceX96, 0)
		step.amountIn = decimal.NewFromBigInt(_amountIn, 0)
		step.amountOut = decimal.NewFromBigInt(_amountOut, 0)
		step.feeAmount = decimal.NewFromBigInt(_feeAmount, 0)

		if exactInput {
			state.amountSpecifiedRemaining = state.amountSpecifiedRemaining.Sub(step.amountIn.Add(step.feeAmount))
			state.amountCalculated = state.amountCalculated.Sub(step.amountOut)
		} else {
			state.amountSpecifiedRemaining = state.amountSpecifiedRemaining.Add(step.amountOut)
			state.amountCalculated = state.amountCalculated.Add(step.amountIn.Add(step.feeAmount))
		}
		if state.liquidity.IsPositive() {
			state.feeGrowthGlobalX128 = state.feeGrowthGlobalX128.Add(step.feeAmount.Mul(Q128).Div(state.liquidity).RoundDown(0))
		}
		if state.sqrtPriceX96.Equal(step.sqrtPriceNextX96) {
			if step.initialized {
				nextTick, err := p.TickManager.GetTickAndInitIfAbsent(step.tickNext)
				if err != nil {
					return ZERO, ZERO, ZERO, err
				}
				var liquidityNet decimal.Decimal
				if isStatic {
					liquidityNet = nextTick.LiquidityNet
				} else {
					if zeroForOne {
						liquidityNet = nextTick.Cross(state.feeGrowthGlobalX128, p.FeeGrowthGlobal1X128)
					} else {
						liquidityNet = nextTick.Cross(p.FeeGrowthGlobal0X128, state.feeGrowthGlobalX128)
					}
				}
				if zeroForOne {
					liquidityNet = liquidityNet.Neg()
				}
				state.liquidity, err = AddDelta(state.liquidity, liquidityNet)
				if err != nil {
					return ZERO, ZERO, ZERO, err
				}
			}
			if zeroForOne {
				state.tick = step.tickNext - 1
			} else {
				state.tick = step.tickNext
			}
		} else if !state.sqrtPriceX96.Equal(step.sqrtPriceStartX96) {
			state.tick, err = GetTickAtSqrtRatio(state.sqrtPriceX96)
			if err != nil {
				return ZERO, ZERO, ZERO, err
			}
		}
	}

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
	var amount0, amount1 decimal.Decimal
	if zeroForOne == exactInput {
		amount0 = amountSpecified.Sub(state.amountSpecifiedRemaining)
		amount1 = state.amountCalculated
	} else {
		amount0 = state.amountCalculated                              // -1
		amount1 = amountSpecified.Sub(state.amountSpecifiedRemaining) // -2
	}
	return amount0, amount1, state.sqrtPriceX96, nil
}

type SwapSolution struct {
	AmountSpecified   decimal.Decimal
	SqrtPriceLimitX96 *decimal.Decimal
}

func (p *CorePool) tryToDryRun(param *UniV3SwapEvent, amountSpec decimal.Decimal, sqrtPriceLimitX96 *decimal.Decimal) bool {
	var zeroForOne = param.Amount0.IsPositive()
	amount0, amount1, priceX96, err := p.HandleSwap(zeroForOne, amountSpec, sqrtPriceLimitX96, true)
	if err != nil {
		logrus.Error(err)
		return false
	}
	result := amount0.Equal(param.Amount0) && amount1.Equal(param.Amount1) && priceX96.Equal(param.SqrtPriceX96)
	if param.RawEvent.Address == common.HexToAddress("0xCba27C8e7115b4Eb50Aa14999BC0866674a96eCB") {
		fmt.Println(amount0, param.Amount0)
		fmt.Println(amount1, param.Amount1)
		fmt.Println(sqrtPriceLimitX96, param.SqrtPriceX96)
	}
	return result
}

func incTowardsInfinity(d decimal.Decimal) decimal.Decimal {
	if d.IsZero() {
		return d
		//logrus.Fatal(d)
	}
	if d.IsPositive() {
		return d.Add(ONE)
	} else {
		return d.Sub(ONE)
	}
}
func (p *CorePool) ResolveInputFromSwapResultEvent(param *UniV3SwapEvent) (decimal.Decimal, *decimal.Decimal, error) {
	// Create an empty slice to hold all our solution candidates
	var solutionList []SwapSolution

	// Record the starting price for debugging
	startPrice := p.SqrtPriceX96

	// === GROUP 1: Base solutions with exact amounts ===

	// Solution 1 & 2: Try exact amount0 and amount1 with target price
	solutionList = append(solutionList, SwapSolution{
		AmountSpecified:   param.Amount0,
		SqrtPriceLimitX96: &param.SqrtPriceX96,
	})
	solutionList = append(solutionList, SwapSolution{
		AmountSpecified:   param.Amount1,
		SqrtPriceLimitX96: &param.SqrtPriceX96,
	})

	// Solution 3 & 4: Try exact amount0 and amount1 without price limit
	solutionList = append(solutionList, SwapSolution{
		AmountSpecified:   param.Amount0,
		SqrtPriceLimitX96: nil,
	})
	solutionList = append(solutionList, SwapSolution{
		AmountSpecified:   param.Amount1,
		SqrtPriceLimitX96: nil,
	})

	// === GROUP 2: Adjusted amounts for precision issues ===

	// Solution 5 & 6: Try slightly incremented/decremented amounts with target price
	incrementedAmount0 := incTowardsInfinity(param.Amount0)
	incrementedAmount1 := incTowardsInfinity(param.Amount1)

	solutionList = append(solutionList, SwapSolution{
		AmountSpecified:   incrementedAmount0,
		SqrtPriceLimitX96: &param.SqrtPriceX96,
	})
	solutionList = append(solutionList, SwapSolution{
		AmountSpecified:   incrementedAmount1,
		SqrtPriceLimitX96: &param.SqrtPriceX96,
	})

	// Solution 7 & 8: Try slightly incremented/decremented amounts without price limit
	solutionList = append(solutionList, SwapSolution{
		AmountSpecified:   incrementedAmount0,
		SqrtPriceLimitX96: nil,
	})
	solutionList = append(solutionList, SwapSolution{
		AmountSpecified:   incrementedAmount1,
		SqrtPriceLimitX96: nil,
	})

	// === GROUP 3: Larger adjustments for significant precision issues ===

	// Solution 9 & 10: Try larger adjustments (± 0.1%)
	adjustmentFactor := decimal.NewFromFloat(0.001) // 0.1%
	amount0Adjusted := param.Amount0.Mul(decimal.NewFromInt(1).Add(adjustmentFactor))
	amount1Adjusted := param.Amount1.Mul(decimal.NewFromInt(1).Add(adjustmentFactor))

	solutionList = append(solutionList, SwapSolution{
		AmountSpecified:   amount0Adjusted,
		SqrtPriceLimitX96: &param.SqrtPriceX96,
	})
	solutionList = append(solutionList, SwapSolution{
		AmountSpecified:   amount1Adjusted,
		SqrtPriceLimitX96: &param.SqrtPriceX96,
	})

	// Solution 11 & 12: Try negative adjustments
	amount0AdjustedNeg := param.Amount0.Mul(decimal.NewFromInt(1).Sub(adjustmentFactor))
	amount1AdjustedNeg := param.Amount1.Mul(decimal.NewFromInt(1).Sub(adjustmentFactor))

	solutionList = append(solutionList, SwapSolution{
		AmountSpecified:   amount0AdjustedNeg,
		SqrtPriceLimitX96: &param.SqrtPriceX96,
	})
	solutionList = append(solutionList, SwapSolution{
		AmountSpecified:   amount1AdjustedNeg,
		SqrtPriceLimitX96: &param.SqrtPriceX96,
	})

	// === GROUP 4: Special handling for zero liquidity cases ===

	// For zero liquidity pools, try larger variations
	if param.Liquidity.IsZero() {
		// Try with larger increments for zero liquidity cases
		largerIncAmount0 := param.Amount0.Mul(decimal.NewFromFloat(1.01)) // +1%
		largerIncAmount1 := param.Amount1.Mul(decimal.NewFromFloat(1.01)) // +1%

		solutionList = append(solutionList, SwapSolution{
			AmountSpecified:   largerIncAmount0,
			SqrtPriceLimitX96: &param.SqrtPriceX96,
		})
		solutionList = append(solutionList, SwapSolution{
			AmountSpecified:   largerIncAmount1,
			SqrtPriceLimitX96: &param.SqrtPriceX96,
		})

		// Try with larger decrements too
		largerDecAmount0 := param.Amount0.Mul(decimal.NewFromFloat(0.99)) // -1%
		largerDecAmount1 := param.Amount1.Mul(decimal.NewFromFloat(0.99)) // -1%

		solutionList = append(solutionList, SwapSolution{
			AmountSpecified:   largerDecAmount0,
			SqrtPriceLimitX96: &param.SqrtPriceX96,
		})
		solutionList = append(solutionList, SwapSolution{
			AmountSpecified:   largerDecAmount1,
			SqrtPriceLimitX96: &param.SqrtPriceX96,
		})
	}

	// Try each solution
	var bestSolution *SwapSolution
	var bestError decimal.Decimal = decimal.NewFromInt(0)
	const tolerance = 0.001 // 0.1% tolerance

	for i, solution := range solutionList {
		var zeroForOne = param.Amount0.IsPositive()
		amount0, amount1, priceX96, err := p.HandleSwap(zeroForOne, solution.AmountSpecified, solution.SqrtPriceLimitX96, true)

		if err != nil {
			logrus.Debugf("Solution %d failed with error: %v", i, err)
			continue
		}

		// Check if amounts are within tolerance
		amount0Match := amount0.Sub(param.Amount0).Abs().Div(decimal.Max(param.Amount0.Abs(), decimal.NewFromInt(1))).LessThanOrEqual(decimal.NewFromFloat(tolerance))
		amount1Match := amount1.Sub(param.Amount1).Abs().Div(decimal.Max(param.Amount1.Abs(), decimal.NewFromInt(1))).LessThanOrEqual(decimal.NewFromFloat(tolerance))
		priceMatch := priceX96.Sub(param.SqrtPriceX96).Abs().Div(param.SqrtPriceX96).LessThanOrEqual(decimal.NewFromFloat(tolerance))

		// If all are within tolerance, consider it a match
		if amount0Match && amount1Match && priceMatch {
			// logrus.Infof("Found solution within tolerance: %+v", solution)
			return solution.AmountSpecified, solution.SqrtPriceLimitX96, nil
		}

		// Calculate how close it is
		priceError := priceX96.Sub(param.SqrtPriceX96).Abs()
		amount0Error := amount0.Sub(param.Amount0).Abs()
		amount1Error := amount1.Sub(param.Amount1).Abs()

		// Normalize errors based on values
		if !param.SqrtPriceX96.IsZero() {
			priceError = priceError.Div(param.SqrtPriceX96)
		}
		if !param.Amount0.IsZero() {
			amount0Error = amount0Error.Div(param.Amount0.Abs())
		} else if !amount0.IsZero() {
			amount0Error = decimal.NewFromFloat(1) // Large error if expected 0 but got non-zero
		}

		if !param.Amount1.IsZero() {
			amount1Error = amount1Error.Div(param.Amount1.Abs())
		} else if !amount1.IsZero() {
			amount1Error = decimal.NewFromFloat(1) // Large error if expected 0 but got non-zero
		}

		totalError := priceError.Add(amount0Error).Add(amount1Error)

		// Save the solution with the smallest error
		if bestSolution == nil || totalError.LessThan(bestError) {
			bestError = totalError
			copySolution := solution
			bestSolution = &copySolution

			// Log the close match for debugging
			logrus.Debugf("Better approximation found (error: %s): %+v", totalError, solution)
			logrus.Debugf("  Expected: amount0=%s, amount1=%s, price=%s",
				param.Amount0, param.Amount1, param.SqrtPriceX96)
			logrus.Debugf("  Got: amount0=%s, amount1=%s, price=%s",
				amount0, amount1, priceX96)
		}
	}

	// If no exact match but we have a close approximation, use it if error is small enough
	// (e.g., less than 0.1% total error)
	if bestSolution != nil && bestError.LessThan(decimal.NewFromFloat(0.001*3)) { // 0.1% * 3 (three values)
		logrus.Infof("Using approximate solution with error %s: %+v", bestError, bestSolution)
		return bestSolution.AmountSpecified, bestSolution.SqrtPriceLimitX96, nil
	}

	// Add debugging info to the error message
	err := fmt.Errorf("failed find swap solution for tx: %s, pool: %s, amounts: %s/%s, price: %s -> %s",
		param.RawEvent.TxHash, param.RawEvent.Address,
		param.Amount0, param.Amount1, startPrice, param.SqrtPriceX96)

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
