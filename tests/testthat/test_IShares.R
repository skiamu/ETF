library(testthat)
library(ETF)

context("IShares")

x <- IShares(summary_link = IShares_link_US)
x <- IShares_IT(summary_link = IShares_link_IT)
y <- IShares_US(summary_link = IShares_link_US)

y[y$localExchangeTicker == "IVV",]$productPageUrl
https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/1467271812596.ajax?tab=top&fileType=json


https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund
