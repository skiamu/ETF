try({setwd("~/RProjects/ETF")})
library(ETF) # or devtools::load_all(".")
source("../Weekly_Commons/CollectData_IShares.R")

aod <- as.Date("2020-02-12")
tickers <- c("IAUP_UK", "HEAL_IT", "DGTL_IT", "RBOT_IT", "ECAR_UK")
res <- tickers %>% purrr::map(get_from_mongo, aod = aod)

heal <- res[[2]]$constituents %>%
    dplyr::mutate(xx = prezzo * nominale) %>%
    dplyr::select(asset_class, ponderazione, prezzo, nominale, valore_di_mercato, xx, valore_nozionale, valuta_di_mercato, ticker_dellemittente)

res %>%
    purrr::compact() %>%
    purrr::map(QueryResult_region) %>%
    purrr::map(scale_weight) %>%
    purrr::map(parse_country) %>%
    purrr::map(parse_sector) -> q
q[[1]]$constituents -> a


res %>%
    purrr::compact() %>%
    purrr::map(QueryResult_region) %>%
    purrr::map(scale_weight) %>%
    purrr::map(parse_country) %>%
    purrr::map(parse_sector) %>%
    purrr::map(compute_NRC_inventory) %>%
    purrr::walk(to_mongo)
w[[1]] %>% to_mongo()
w[[1]] %>% readr::write_csv(path = "output.csv")
