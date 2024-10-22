### Python script to screen stocks based on Mark Minervini's "Trend Template Criteria".

   1. price above 150 & 200 SMA
   2. 150 above 200 SMA
   3. 200 SMA is uptrending for at least 1 month, preferably 4-5 months
   4. 50 SMA above 150
   5. price above 50 SMA (within 8% for 'low cheats')
   6. price 30% above 52-week low
   7. within 25% of 52 week high (closer the better)
   8. relative strength (as reported by Investors' Business Daily) >= 70, preferably 80s or 90s

### Here's a quick video summary of Mark's book: 
<https://www.youtube.com/watch?v=W5ljClz4H3g>

### Or for those that prefer the written word: 
<https://www.amazon.com/Trade-Like-Stock-Market-Wizard>

### Basic features so far:
* pulls all stock tickers from SEC.gov
* uses [yfinance](https://github.com/ranaroussi/yfinance/tree/main) to scrape historical price and quarterly financial data
* calculates an approximation of the IBD score
* saves results to g-sheets

### Stuff I'm working on or planning to work on:
* generate charts for the top N tickers
* algorithm(s) to recognize and plot patterns (e.g., VCP, flag/pennant, breakout, etc)
* automatically run a screen at market close every Friday and publish data and charts to web

### Notes for running the script:
* search (CTRL+F) for "DEBUG" within main() to enable one of the commented out lines that drastically shrinks the ticker list for DEBUG purposes
* script takes one command line option, the Google Drive folder ID being written to, which is the string of random looking characters following the base URL: https://drive.google.com/drive/folders/
* need to set up auth to permit writes to the G-Drive (CTRL+F for "JSON" here: https://developers.google.com/workspace/guides/create-credentials)
* script expects to find your JSON file in ../auth (relative to the location of the script itself)
