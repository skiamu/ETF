library(ETF)
library(futile.logger)
library(glue)
library(tidyverse)
source("../Weekly_Commons/CollectData_IShares.R")
source("./script/mongo_functions.R")

## -----------------------------------------------------------------------------
# IT ETFs
## -----------------------------------------------------------------------------
IT <- IShares_IT(summary_link = IShares_link_IT,
                get_constituents = TRUE,
                download_constituents_csv = FALSE)
IT_data <- to_mongo_data_format(IT)
IT_data %>% purrr::walk(write_to_mongo)
## -----------------------------------------------------------------------------
# US ETFs
## -----------------------------------------------------------------------------
US <- IShares_US(summary_link = IShares_link_US,
                 get_constituents = TRUE,
                 download_constituents_csv = FALSE)
US_data <- to_mongo_data_format(US)
US_data %>% purrr::walk(write_to_mongo)
## -----------------------------------------------------------------------------
# UK ETFs
## -----------------------------------------------------------------------------
UK <- IShares_UK(summary_link = IShares_link_UK,
                 get_constituents = TRUE,
                 download_constituents_csv = FALSE)
UK_data <- to_mongo_data_format(UK)
UK_data %>% purrr::walk(write_to_mongo)








w <- get_from_mongo(ticker = "MVEU_IT", aod = as.Date("2020-02-12"))

# isharesETF_Save(
#     ticker = data[[i]]$ticker[1],
#     aod = data[[i]]$aod[1],
#     data = data[[i]]$data
# )
#
# #isharesCacheConn_etf$remove('{"ticker":"EMV_IT"}')
#
# isharesETF_Get(ticker = "MVEU_IT") -> w
# w %>% str()
#
# js <- '{"aod": 20200101, "ticker": "EVV", "data": {"summary_data": [{"s1" : 1, "s2" : 2 }, {"s1" : 1, "s2" : 2 }], "constituents": [{"c1" : 1, "c2" : 2}, {"c1" : 3, "c2" : 4}]}}'
# jsonlite::fromJSON(js) %>% str()
#
# jsonlite::toJSON(data[[i]])
#
#
# test <- list(aod = 20200101,
#              ticker = "EVV",
#              data = list(
#                  summary_data = data.frame(s1 = c(1,1), s2 = c(2,2)),
#                  constituents = data.frame(c1 = c(1,3), c2 = c(2,4))
#              ))
# jsonlite::toJSON(test, auto_unbox = T)
