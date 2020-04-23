tryCatch({
    # library(ETF)
    devtools::load_all(".") # source the whole package ETF
    library(futile.logger)
    # library(glue)
    # library(tidyverse)
    # source("../Weekly_Commons/CollectData_Base.R")
    # source("../Weekly_Commons/CollectData_IShares.R")
    source("../Weekly_Commons/CollectData_SourceMe.R")
    ## -------------------------------------------------------------------------
    # 1) DOWNLOAD AND STORE ISHARES DATA
    ## -------------------------------------------------------------------------
    ## 1.1) IT ETFs ------------------------------------------------------------
    tickers_to_keep_IT <- c("HEAL", "DGTL", "RBOT")
    IT <- IShares_IT(summary_link = IShares_link_IT,
                     get_constituents = TRUE,
                     download_constituents_csv = FALSE,
                     tickers_to_keep = tickers_to_keep_IT)
    IT_data <- to_mongo_data_format(IT)
    IT_data %>% purrr::walk(write_IShares_to_mongo)
    ## 1.2) US ETFs ------------------------------------------------------------
    # US <- IShares_US(summary_link = IShares_link_US,
    #                  get_constituents = TRUE,
    #                  download_constituents_csv = FALSE)
    # US_data <- to_mongo_data_format(US)
    # US_data %>% purrr::walk(write_IShares_to_mongo)
    ## 1.3) IT ETFs ------------------------------------------------------------
    tickers_to_keep_UK <- c("ECAR")
    UK <- IShares_UK(summary_link = IShares_link_UK,
                     get_constituents = TRUE,
                     download_constituents_csv = FALSE,
                     tickers_to_keep = tickers_to_keep_UK)
    UK_data <- to_mongo_data_format(UK)
    UK_data %>% purrr::walk(write_IShares_to_mongo)
    ## -------------------------------------------------------------------------
    # 2) CONSTITUENTS FOR NRC
    ## -------------------------------------------------------------------------
    # if we get here we know that there's aod_nav/aod_constituents concordance within ETF
    aod_list <- c(get_constituents_aod(IT), get_constituents_aod(UK))
    if (aod_list %>% purrr::every(~ .x == aod_list[[1]])) {
        aod <- aod_list[[1]]
        futile.logger::flog.info(glue::glue("All ETF has the same constituents aod = {aod}"))
        tickers <- c("IAUP_UK", "HEAL_IT", "DGTL_IT", "RBOT_IT", "ECAR_UK")
        res <- tickers %>% purrr::map(get_from_mongo, aod = aod)
        res %>%
            purrr::compact() %>%
            purrr::map(QueryResult_region) %>%
            purrr::map(scale_weight) %>%
            purrr::map(parse_country) %>%
            purrr::map(parse_sector) %>%
            purrr::map(compute_NRC_inventory) %>%
            purrr::walk(to_mongo)
    } else {
        stop("All ETF has the same constituents aod")
    }
},
error = function(e) {
    stop(e)
})



