#### Python script to screen stocks based on Mark Minervini's "Trend Template Criteria".

   1. price above 150 & 200 SMA
   2. 150 above 200 SMA
   3. 200 SMA is uptrending for at least 1 month, preferably 4-5 months
   4. 50 SMA above 150
   5. price above 50 SMA (within 8% for 'low cheats')
   6. price 30% above 52-week low
   7. within 25% of 52 week high (closer the better)
   8. relative strength (as reported by Investors' Business Daily) >= 70, preferably 80s or 90s

#### Here's a quick video summary of Mark's book: 
<https://www.youtube.com/watch?v=W5ljClz4H3g>

#### Or for those that prefer the written word: 
<https://www.amazon.com/Trade-Like-Stock-Market-Wizard>

#### Basic features so far:
* pulls all stock tickers from SEC.gov
* uses [yfinance](https://github.com/ranaroussi/yfinance/tree/main) to scrape historical price and quarterly financial data
* calulcates an approximation of the IDB score
* saves results to g-sheets

#### Stuff I'm working on or planning to work on:
* generate charts for the top N tickers
* algorithm(s) to recognize and plot patterns (e.g., VCP, flag/pennant, breakout, etc)
* automatically run a screen at market close every Friday and publish data and charts to web