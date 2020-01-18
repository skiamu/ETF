# link <- "https://www.ishares.com/it/investitore-privato/it/prodotti/etf-product-list/1524727818182.ajax?fileType=json"
# link_ita <- "https://www.ishares.com/it/investitori-professionali/it/product-screener/product-screener-v3.jsn?dcrPath=/templatedata/config/product-screener-v3/data/it/it/product-screener/product-screener&siteEntryPassthrough=true"
# link_usa <- "https://www.ishares.com/us/product-screener/product-screener-v3.jsn?dcrPath=/templatedata/config/product-screener-v3/data/en/us-ishares/product-screener-ketto&siteEntryPassthrough=true"
# link_usa <- "https://www.ishares.com/us/product-screener/product-screener-v3.jsn?dcrPath=/templatedata/config/product-screener-v3/data/en/us-ishares/product-screener-ketto"
#
# res <- httr::GET(url = link_ita)
# w <- httr::content(res, as = "parsed")
# str(w$data$tableData$data, max.level = 1)
# w$data$tableData$columns %>% str()
# w$data$tableData$data[[1]] %>% length()
