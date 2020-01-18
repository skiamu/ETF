context("ETF")

etf <- ETF(summary_link = "https://www.ishares.com/it/investitori-professionali/it/product-screener/product-screener-v3.jsn?dcrPath=/templatedata/config/product-screener-v3/data/it/it/product-screener/product-screener&siteEntryPassthrough=true")

test_that("ETF right class", {
    expect_true(
        "ETF" %in% class(etf)
    )
})
