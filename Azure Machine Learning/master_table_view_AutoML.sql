-- SQL View to consolidate stock data from multiple individual stock tables into a single view
-- and being able to create a unique job for AutoML and run for each ticker symbol a forecasting model.

CREATE VIEW dbo.All_Stocks_Gold AS
SELECT 'AMD' as Ticker, Date, AMD_Close as ClosePrice, AMD_SMA10 as SMA10, AMD_SMA50 as SMA50, AMD_SMA100 as SMA100, AMD_ZScore20 as ZScore20, AMD_DailyRet as DailyRet, AMD_Volatility20 as Volatility20 FROM dbo.AMD_Gold
UNION ALL
SELECT 'AMZN', Date, AMZN_Close, AMZN_SMA10, AMZN_SMA50, AMZN_SMA100, AMZN_ZScore20, AMZN_DailyRet, AMZN_Volatility20 FROM dbo.AMZN_Gold
UNION ALL
SELECT 'AVGO', Date, AVGO_Close, AVGO_SMA10, AVGO_SMA50, AVGO_SMA100, AVGO_ZScore20, AVGO_DailyRet, AVGO_Volatility20 FROM dbo.AVGO_Gold
UNION ALL
SELECT 'GOOG', Date, GOOG_Close, GOOG_SMA10, GOOG_SMA50, GOOG_SMA100, GOOG_ZScore20, GOOG_DailyRet, GOOG_Volatility20 FROM dbo.GOOG_Gold
UNION ALL
SELECT 'META', Date, META_Close, META_SMA10, META_SMA50, META_SMA100, META_ZScore20, META_DailyRet, META_Volatility20 FROM dbo.META_Gold
UNION ALL
SELECT 'MSFT', Date, MSFT_Close, MSFT_SMA10, MSFT_SMA50, MSFT_SMA100, MSFT_ZScore20, MSFT_DailyRet, MSFT_Volatility20 FROM dbo.MSFT_Gold
UNION ALL
SELECT 'NVDA', Date, NVDA_Close, NVDA_SMA10, NVDA_SMA50, NVDA_SMA100, NVDA_ZScore20, NVDA_DailyRet, NVDA_Volatility20 FROM dbo.NVDA_Gold
UNION ALL
SELECT 'ORCL', Date, ORCL_Close, ORCL_SMA10, ORCL_SMA50, ORCL_SMA100, ORCL_ZScore20, ORCL_DailyRet, ORCL_Volatility20 FROM dbo.ORCL_Gold
UNION ALL
SELECT 'PANW', Date, PANW_Close, PANW_SMA10, PANW_SMA50, PANW_SMA100, PANW_ZScore20, PANW_DailyRet, PANW_Volatility20 FROM dbo.PANW_Gold;