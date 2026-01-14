const logger = require('../utils/logger');
const { TRADING_PAIRS, fetchOrderBook, executeBuy, executeSell } = require('../exchanges');
const { recordArbitrageOpportunity, recordTrade } = require('../database');
const { 
  ArbitrageError, 
  ValidationError,
  withRetry, 
  enhanceError,
  validators,
  recoveryStrategies 
} = require('../utils/errors');

const ARBITRAGE_THRESHOLD = parseFloat(process.env.ARBITRAGE_THRESHOLD || '0.5');
const TRADE_AMOUNT = parseFloat(process.env.TRADE_AMOUNT || '100');
const MAX_CONCURRENT_TRADES = parseInt(process.env.MAX_CONCURRENT_TRADES || '3');
const SCAN_INTERVAL = parseInt(process.env.SCAN_INTERVAL || '10000');

const activeArbitrageOps = new Map();
const scanErrors = new Map(); // Track scan errors per symbol
const exchangeHealth = new Map(); // Track exchange health

/**
 * Validate arbitrage configuration
 */
function validateConfiguration() {
  if (ARBITRAGE_THRESHOLD <= 0 || ARBITRAGE_THRESHOLD > 50) {
    throw new ValidationError('Arbitrage threshold must be between 0 and 50', 'ARBITRAGE_THRESHOLD', ARBITRAGE_THRESHOLD);
  }

  if (TRADE_AMOUNT <= 0 || TRADE_AMOUNT > 100000) {
    throw new ValidationError('Trade amount must be between 0 and 100000', 'TRADE_AMOUNT', TRADE_AMOUNT);
  }

  if (MAX_CONCURRENT_TRADES <= 0 || MAX_CONCURRENT_TRADES > 10) {
    throw new ValidationError('Max concurrent trades must be between 1 and 10', 'MAX_CONCURRENT_TRADES', MAX_CONCURRENT_TRADES);
  }

  if (SCAN_INTERVAL < 1000 || SCAN_INTERVAL > 60000) {
    throw new ValidationError('Scan interval must be between 1000ms and 60000ms', 'SCAN_INTERVAL', SCAN_INTERVAL);
  }
}

/**
 * Start the arbitrage scanner
 * @param {Object} exchanges - Map of exchange instances
 */
function startArbitrageScanner(exchanges) {
  try {
    // Validate configuration
    validateConfiguration();

    // Validate exchanges
    if (!exchanges || typeof exchanges !== 'object' || Object.keys(exchanges).length === 0) {
      throw new ArbitrageError('No exchanges provided for arbitrage scanning', 'NO_EXCHANGES');
    }

    logger.info(`Starting arbitrage scanner with ${ARBITRAGE_THRESHOLD}% threshold`, {
      threshold: ARBITRAGE_THRESHOLD,
      tradeAmount: TRADE_AMOUNT,
      maxConcurrentTrades: MAX_CONCURRENT_TRADES,
      scanInterval: SCAN_INTERVAL,
      exchangeCount: Object.keys(exchanges).length
    });

    // Initialize exchange health tracking
    Object.keys(exchanges).forEach(exchangeId => {
      exchangeHealth.set(exchangeId, {
        healthy: true,
        lastError: null,
        errorCount: 0,
        lastSuccessfulScan: Date.now()
      });
    });

    // Start scanning with error recovery
    const scanWithRecovery = async () => {
      try {
        await scanForArbitrageOpportunities(exchanges);
      } catch (error) {
        const enhancedError = enhanceError(error, { operation: 'scanForArbitrageOpportunities' });
        logger.error(`Arbitrage scan failed: ${enhancedError.message}`, {
          error: enhancedError.message,
          code: enhancedError.code
        });
      }
    };

    // Initial scan
    scanWithRecovery();

    // Set up interval scanning
    setInterval(scanWithRecovery, SCAN_INTERVAL);

    // Set up health monitoring
    setInterval(() => {
      monitorExchangeHealth();
      cleanupStaleOperations();
    }, 30000); // Every 30 seconds

  } catch (error) {
    const enhancedError = enhanceError(error, { operation: 'startArbitrageScanner' });
    logger.error(`Failed to start arbitrage scanner: ${enhancedError.message}`, {
      error: enhancedError.message,
      code: enhancedError.code,
      stack: enhancedError.stack
    });
    throw enhancedError;
  }
}

/**
 * Monitor exchange health and update status
 */
function monitorExchangeHealth() {
  const now = Date.now();
  const healthThreshold = 300000; // 5 minutes

  for (const [exchangeId, health] of exchangeHealth.entries()) {
    const timeSinceLastSuccess = now - health.lastSuccessfulScan;
    
    if (timeSinceLastSuccess > healthThreshold && health.healthy) {
      health.healthy = false;
      logger.warn(`Exchange ${exchangeId} marked as unhealthy - no successful scans in ${timeSinceLastSuccess}ms`, {
        exchangeId,
        timeSinceLastSuccess,
        errorCount: health.errorCount
      });
    } else if (timeSinceLastSuccess <= healthThreshold && !health.healthy) {
      health.healthy = true;
      health.errorCount = 0;
      logger.info(`Exchange ${exchangeId} marked as healthy again`, {
        exchangeId,
        timeSinceLastSuccess
      });
    }
  }
}

/**
 * Clean up stale arbitrage operations
 */
function cleanupStaleOperations() {
  const now = Date.now();
  const staleThreshold = 300000; // 5 minutes

  for (const [key, timestamp] of activeArbitrageOps.entries()) {
    if (now - timestamp > staleThreshold) {
      activeArbitrageOps.delete(key);
      logger.debug(`Cleaned up stale arbitrage operation: ${key}`, {
        key,
        age: now - timestamp
      });
    }
  }
}

/**
 * Scan for arbitrage opportunities across all exchanges and trading pairs
 * @param {Object} exchanges - Map of exchange instances
 */
async function scanForArbitrageOpportunities(exchanges) {
  try {
    const exchangeIds = Object.keys(exchanges);
    if (exchangeIds.length < 2) {
      throw new ArbitrageError(`Need at least 2 exchanges for arbitrage, only found ${exchangeIds.length}`, 'INSUFFICIENT_EXCHANGES');
    }

    // Check if we're at max concurrent trades
    if (activeArbitrageOps.size >= MAX_CONCURRENT_TRADES) {
      logger.debug(`Skipping scan - at max concurrent trades (${activeArbitrageOps.size}/${MAX_CONCURRENT_TRADES})`);
      return;
    }

    // Filter healthy exchanges
    const healthyExchanges = {};
    for (const exchangeId of exchangeIds) {
      const health = exchangeHealth.get(exchangeId);
      if (health && health.healthy) {
        healthyExchanges[exchangeId] = exchanges[exchangeId];
      }
    }

    if (Object.keys(healthyExchanges).length < 2) {
      logger.warn('Not enough healthy exchanges for arbitrage scanning', {
        totalExchanges: exchangeIds.length,
        healthyExchanges: Object.keys(healthyExchanges).length
      });
      return;
    }

    // Scan each trading pair with error isolation
    const scanPromises = TRADING_PAIRS.map(symbol => 
      recoveryStrategies.gracefulDegradation(
        () => scanPairAcrossExchanges(healthyExchanges, symbol),
        () => {
          logger.debug(`Skipping ${symbol} due to scan errors`);
          return Promise.resolve();
        },
        { operation: 'scanPairAcrossExchanges', symbol }
      )
    );

    await Promise.allSettled(scanPromises);

  } catch (error) {
    const enhancedError = enhanceError(error, { operation: 'scanForArbitrageOpportunities' });
    logger.error(`Error in arbitrage scanning: ${enhancedError.message}`, {
      error: enhancedError.message,
      code: enhancedError.code
    });
    throw enhancedError;
  }
}

/**
 * Scan a specific trading pair across all exchanges
 * @param {Object} exchanges - Map of exchange instances
 * @param {String} symbol - Trading pair symbol
 */
async function scanPairAcrossExchanges(exchanges, symbol) {
  try {
    // Input validation
    validators.isValidSymbol(symbol);

    const priceData = [];
    const exchangeIds = Object.keys(exchanges);
    
    // Fetch price data from all exchanges with error isolation
    const fetchPromises = exchangeIds.map(async (exchangeId) => {
      try {
        const exchange = exchanges[exchangeId];
        
        // Check if exchange supports this symbol
        if (!exchange.markets || !exchange.markets[symbol]) {
          return null;
        }

        const orderBook = await fetchOrderBook(exchange, symbol);
        if (!orderBook || !orderBook.bids || !orderBook.asks || 
            orderBook.bids.length === 0 || orderBook.asks.length === 0) {
          return null;
        }

        const bestBid = orderBook.bids[0][0];
        const bestAsk = orderBook.asks[0][0];

        // Validate price data
        if (typeof bestBid !== 'number' || typeof bestAsk !== 'number' || 
            bestBid <= 0 || bestAsk <= 0 || bestBid >= bestAsk) {
          logger.warn(`Invalid price data from ${exchangeId} for ${symbol}`, {
            exchangeId,
            symbol,
            bestBid,
            bestAsk
          });
          return null;
        }

        // Update exchange health on success
        const health = exchangeHealth.get(exchangeId);
        if (health) {
          health.lastSuccessfulScan = Date.now();
          if (!health.healthy) {
            health.healthy = true;
            health.errorCount = 0;
            logger.info(`Exchange ${exchangeId} recovered and marked healthy`, { exchangeId });
          }
        }

        return {
          exchange: exchangeId,
          exchangeObj: exchange,
          symbol,
          bestBid,
          bestAsk,
          timestamp: Date.now()
        };

      } catch (error) {
        const enhancedError = enhanceError(error, { exchangeId, symbol, operation: 'fetchPriceData' });
        
        // Update exchange health on error
        const health = exchangeHealth.get(exchangeId);
        if (health) {
          health.errorCount++;
          health.lastError = enhancedError.message;
          
          if (health.errorCount >= 3) {
            health.healthy = false;
            logger.warn(`Exchange ${exchangeId} marked unhealthy after ${health.errorCount} errors`, {
              exchangeId,
              errorCount: health.errorCount,
              lastError: health.lastError
            });
          }
        }

        logger.debug(`Error fetching ${symbol} data from ${exchangeId}: ${enhancedError.message}`, {
          exchangeId,
          symbol,
          error: enhancedError.message,
          code: enhancedError.code
        });
        
        return null;
      }
    });

    const results = await Promise.allSettled(fetchPromises);
    
    // Filter successful results
    for (const result of results) {
      if (result.status === 'fulfilled' && result.value) {
        priceData.push(result.value);
      }
    }

    if (priceData.length < 2) {
      logger.debug(`Insufficient price data for ${symbol} - only ${priceData.length} exchanges`, {
        symbol,
        availableExchanges: priceData.length,
        requiredExchanges: 2
      });
      return;
    }

    // Find arbitrage opportunities
    await findAndExecuteArbitrageOpportunity(priceData, symbol);

  } catch (error) {
    const enhancedError = enhanceError(error, { symbol, operation: 'scanPairAcrossExchanges' });
    
    // Track scan errors per symbol
    const errorCount = scanErrors.get(symbol) || 0;
    scanErrors.set(symbol, errorCount + 1);
    
    if (errorCount >= 5) {
      logger.warn(`Symbol ${symbol} has ${errorCount + 1} consecutive scan errors, may need attention`, {
        symbol,
        errorCount: errorCount + 1,
        error: enhancedError.message
      });
    }
    
    throw enhancedError;
  }
}

/**
 * Find and execute arbitrage opportunity from price data
 * @param {Array} priceData - Array of price data from exchanges
 * @param {String} symbol - Trading pair symbol
 */
async function findAndExecuteArbitrageOpportunity(priceData, symbol) {
  try {
    // Sort to find best opportunities
    priceData.sort((a, b) => b.bestBid - a.bestBid);
    const highestBid = priceData[0];
    
    priceData.sort((a, b) => a.bestAsk - b.bestAsk);
    const lowestAsk = priceData[0];
    
    // Can't arbitrage on the same exchange
    if (highestBid.exchange === lowestAsk.exchange) {
      return;
    }

    const priceDiff = highestBid.bestBid - lowestAsk.bestAsk;
    const percentageDiff = (priceDiff / lowestAsk.bestAsk) * 100;
    
    // Check if opportunity meets threshold
    if (percentageDiff <= ARBITRAGE_THRESHOLD) {
      return;
    }

    // Validate opportunity data
    if (priceDiff <= 0 || percentageDiff <= 0) {
      logger.warn(`Invalid arbitrage calculation for ${symbol}`, {
        symbol,
        priceDiff,
        percentageDiff,
        buyPrice: lowestAsk.bestAsk,
        sellPrice: highestBid.bestBid
      });
      return;
    }

    const potentialProfit = priceDiff * (TRADE_AMOUNT / lowestAsk.bestAsk);
    
    const opportunity = {
      symbol,
      buyExchange: lowestAsk.exchange,
      sellExchange: highestBid.exchange,
      buyPrice: lowestAsk.bestAsk,
      sellPrice: highestBid.bestBid,
      priceDifference: priceDiff,
      percentageDifference: percentageDiff,
      potentialProfit,
      timestamp: new Date().toISOString(),
      scanTimestamp: Date.now()
    };

    logger.info(`ARBITRAGE OPPORTUNITY: ${symbol} - Buy on ${lowestAsk.exchange} at ${lowestAsk.bestAsk}, Sell on ${highestBid.exchange} at ${highestBid.bestBid} - ${percentageDiff.toFixed(2)}% difference`, {
      symbol,
      buyExchange: lowestAsk.exchange,
      sellExchange: highestBid.exchange,
      percentageDiff: percentageDiff.toFixed(2),
      potentialProfit: potentialProfit.toFixed(2)
    });
    
    // Record opportunity (non-blocking)
    recordArbitrageOpportunity(opportunity).catch(error => {
      logger.error(`Failed to record arbitrage opportunity: ${error.message}`, {
        symbol,
        error: error.message
      });
    });
    
    // Check if we should execute this opportunity
    const key = `${symbol}-${lowestAsk.exchange}-${highestBid.exchange}`;
    if (!activeArbitrageOps.has(key) && activeArbitrageOps.size < MAX_CONCURRENT_TRADES) {
      // Execute arbitrage in background
      executeArbitrageWithRecovery(
        lowestAsk.exchangeObj, 
        highestBid.exchangeObj, 
        symbol, 
        TRADE_AMOUNT, 
        opportunity,
        key
      );
    } else {
      logger.debug(`Skipping arbitrage execution - operation already active or at max capacity`, {
        key,
        activeOps: activeArbitrageOps.size,
        maxOps: MAX_CONCURRENT_TRADES
      });
    }

  } catch (error) {
    const enhancedError = enhanceError(error, { symbol, operation: 'findAndExecuteArbitrageOpportunity' });
    logger.error(`Error finding arbitrage opportunity for ${symbol}: ${enhancedError.message}`, {
      symbol,
      error: enhancedError.message,
      code: enhancedError.code
    });
    throw enhancedError;
  }
}

/**
 * Execute arbitrage with error recovery
 * @param {Object} buyExchange - Exchange to buy on
 * @param {Object} sellExchange - Exchange to sell on
 * @param {String} symbol - Trading pair symbol
 * @param {Number} amount - Amount to trade in quote currency
 * @param {Object} opportunity - Arbitrage opportunity details
 * @param {String} key - Unique key for this operation
 */
async function executeArbitrageWithRecovery(buyExchange, sellExchange, symbol, amount, opportunity, key) {
  // Mark operation as active
  activeArbitrageOps.set(key, Date.now());
  
  try {
    await executeArbitrage(buyExchange, sellExchange, symbol, amount, opportunity);
  } catch (error) {
    const enhancedError = enhanceError(error, { 
      symbol, 
      buyExchange: buyExchange?.id, 
      sellExchange: sellExchange?.id,
      operation: 'executeArbitrageWithRecovery' 
    });
    
    logger.error(`Arbitrage execution failed: ${enhancedError.message}`, {
      symbol,
      buyExchange: buyExchange?.id,
      sellExchange: sellExchange?.id,
      error: enhancedError.message,
      code: enhancedError.code
    });
  } finally {
    // Clean up operation tracking
    setTimeout(() => {
      activeArbitrageOps.delete(key);
    }, 60000); // Keep for 1 minute to prevent immediate re-execution
  }
}

/**
 * Execute an arbitrage opportunity
 * @param {Object} buyExchange - Exchange to buy on
 * @param {Object} sellExchange - Exchange to sell on
 * @param {String} symbol - Trading pair symbol
 * @param {Number} amount - Amount to trade in quote currency
 * @param {Object} opportunity - Arbitrage opportunity details
 */
async function executeArbitrage(buyExchange, sellExchange, symbol, amount, opportunity) {
  try {
    // Input validation
    validators.isValidSymbol(symbol);
    validators.isValidAmount(amount);
    
    if (!buyExchange || !buyExchange.id) {
      throw new ArbitrageError('Invalid buy exchange', 'INVALID_BUY_EXCHANGE');
    }
    
    if (!sellExchange || !sellExchange.id) {
      throw new ArbitrageError('Invalid sell exchange', 'INVALID_SELL_EXCHANGE');
    }

    if (!opportunity || typeof opportunity.buyPrice !== 'number' || opportunity.buyPrice <= 0) {
      throw new ValidationError('Invalid opportunity data', 'opportunity', opportunity);
    }

    const baseAmount = amount / opportunity.buyPrice;
    
    // Validate calculated amount
    if (!isFinite(baseAmount) || baseAmount <= 0) {
      throw new ValidationError('Invalid calculated base amount', 'baseAmount', baseAmount);
    }

    logger.info(`Executing arbitrage trade for ${symbol}`, {
      symbol,
      buyExchange: buyExchange.id,
      sellExchange: sellExchange.id,
      baseAmount,
      quoteAmount: amount,
      buyPrice: opportunity.buyPrice,
      sellPrice: opportunity.sellPrice
    });

    // Execute buy order with retry
    logger.info(`Executing buy order for ${baseAmount} ${symbol} on ${buyExchange.id} at ${opportunity.buyPrice}`);
    const buyOrder = await withRetry(
      () => executeBuy(buyExchange, symbol, baseAmount),
      {
        maxRetries: 1,
        baseDelay: 1000,
        context: { operation: 'executeBuy', exchangeId: buyExchange.id, symbol, amount: baseAmount },
        retryCondition: (error) => {
          // Only retry on network timeouts
          return error.message.includes('timeout') && 
                 !error.message.includes('insufficient') &&
                 !error.message.includes('balance');
        }
      }
    );
    
    if (!buyOrder) {
      throw new ArbitrageError(`Failed to execute buy order on ${buyExchange.id}`, 'BUY_ORDER_FAILED');
    }

    // Execute sell order with retry
    logger.info(`Executing sell order for ${baseAmount} ${symbol} on ${sellExchange.id} at ${opportunity.sellPrice}`);
    const sellOrder = await withRetry(
      () => executeSell(sellExchange, symbol, baseAmount),
      {
        maxRetries: 1,
        baseDelay: 1000,
        context: { operation: 'executeSell', exchangeId: sellExchange.id, symbol, amount: baseAmount },
        retryCondition: (error) => {
          return error.message.includes('timeout') && 
                 !error.message.includes('insufficient') &&
                 !error.message.includes('balance');
        }
      }
    );
    
    if (!sellOrder) {
      // If sell order fails after buy order succeeds, log critical error
      logger.error(`CRITICAL: Buy order succeeded but sell order failed for ${symbol}`, {
        symbol,
        buyExchange: buyExchange.id,
        sellExchange: sellExchange.id,
        buyOrderId: buyOrder.id || 'simulated',
        baseAmount
      });
      throw new ArbitrageError(`Failed to execute sell order on ${sellExchange.id} after successful buy`, 'SELL_ORDER_FAILED_AFTER_BUY');
    }

    // Calculate actual profit based on executed trade
    const actualProfit = (opportunity.sellPrice - opportunity.buyPrice) * baseAmount;

    // Create trade record
    const trade = {
      ...opportunity,
      buyOrderId: buyOrder.id || 'simulated',
      sellOrderId: sellOrder.id || 'simulated',
      amount: baseAmount,  // Required field for Trade schema
      actualProfit,        // Required field for Trade schema
      quoteAmount: amount,
      completed: true,
      completedAt: new Date().toISOString(),
      executionTime: Date.now() - opportunity.scanTimestamp
    };
    
    // Record trade (non-blocking)
    recordTrade(trade).catch(error => {
      logger.error(`Failed to record trade: ${error.message}`, {
        symbol,
        error: error.message
      });
    });
    
    logger.info(`Successfully executed arbitrage: ${trade.potentialProfit.toFixed(2)} profit`, {
      symbol,
      buyExchange: buyExchange.id,
      sellExchange: sellExchange.id,
      profit: trade.potentialProfit.toFixed(2),
      executionTime: trade.executionTime
    });

  } catch (error) {
    const enhancedError = enhanceError(error, { 
      symbol, 
      buyExchange: buyExchange?.id, 
      sellExchange: sellExchange?.id,
      amount,
      operation: 'executeArbitrage' 
    });
    
    logger.error(`Error executing arbitrage: ${enhancedError.message}`, {
      symbol,
      buyExchange: buyExchange?.id,
      sellExchange: sellExchange?.id,
      error: enhancedError.message,
      code: enhancedError.code,
      stack: enhancedError.stack
    });
    
    throw enhancedError;
  }
}

/**
 * Get arbitrage scanner status
 * @returns {Object} Scanner status information
 */
function getArbitrageStatus() {
  return {
    configuration: {
      threshold: ARBITRAGE_THRESHOLD,
      tradeAmount: TRADE_AMOUNT,
      maxConcurrentTrades: MAX_CONCURRENT_TRADES,
      scanInterval: SCAN_INTERVAL
    },
    runtime: {
      activeOperations: activeArbitrageOps.size,
      activeOperationKeys: Array.from(activeArbitrageOps.keys()),
      exchangeHealth: Object.fromEntries(exchangeHealth),
      scanErrors: Object.fromEntries(scanErrors)
    },
    timestamp: new Date().toISOString()
  };
}

module.exports = {
  startArbitrageScanner,
  getArbitrageStatus
}; 