library(ETF)
source("../Weekly_Commons/CollectData_IShares.R")
source("./script/mongo_functions.R")

aod <- as.Date("2020-02-12")
tickers <- c("IAUP_UK", "HEAL_IT", "DGTL_IT", "RBOT_IT", "ECAR_UK")
res <- tickers %>% purrr::map(get_from_mongo, aod = aod)
heal <- res[[2]]$constituents
heal_summaty <- res[[2]]$summary_data
sum(res[[5]]$constituents$weight)
