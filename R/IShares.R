
main <- function(){
    raw_data <- download_summary_IShares(region = "Italy")
    c(col_names_df, IShare_summary_df) %<-% parse_IShares_summary_US(raw_data)
    # funds <- c("IVV", "SHY", "IJR")
    holdings_ETF_US_list <- download_holdings_ETF_US(IShare_summary_df,
                                                     fund_to_download = NULL)
    parsed_holdings_ETF_US_list <- parse_holdings_ETF_US(holdings_ETF_US_list)
} # main

download_summary_IShares <- function(region = "Italy"){
    # DESCRIPTION:
    # INPUT:
    # OUTPUT:
    futile.logger::flog.info(glue::glue("downloading IShares summary data, region = {region}"))
    switch(region,
           Italy = {
               link <- "https://www.ishares.com/it/investitori-professionali/it/product-screener/product-screener-v3.jsn?dcrPath=/templatedata/config/product-screener-v3/data/it/it/product-screener/product-screener&siteEntryPassthrough=true"
           },
           US = {
               link <- "https://www.ishares.com/us/product-screener/product-screener-v3.jsn?dcrPath=/templatedata/config/product-screener-v3/data/en/us-ishares/product-screener-ketto"
           })
    res <- jsonlite::fromJSON(txt = link)
} # download_summary_IShares

parse_IShares_summary_IT <- function(raw_data){
    # DESCRIPTION:
    # INPUT:
    # OUTPUT:
    ## -------------------------------------------------------------------------
    # 0) UNPACK LIST
    ## -------------------------------------------------------------------------
    c(col_names_df, funds_list) %<-% raw_data
    ## -------------------------------------------------------------------------
    # 1) GENERAL DATA PARSING
    ## -------------------------------------------------------------------------
    IShare_summary_df <- funds_list %>%
        map_dfr(~ .x %>% map_dfc(extract_IShares_data_as_char)) %>%
        `colnames<-`(col_names_df$name)
    ## -------------------------------------------------------------------------
    # 2) SPECIFIC DATA PARSING
    ## -------------------------------------------------------------------------
    IShare_summary_df <- IShare_summary_df %>%
        mutate_at(vars(marketCap,totalFundSizeInMillions),
                  ~str_replace(., "\\.", "")) %>% # this is a special case
        mutate_all(~str_replace(., ",",".")) %>%
        readr::type_convert() %>%
        mutate_at(vars(contains("AsOf")), ~as.Date(., format = "%d/%m/%Y"))
    return(list(
        col_names_df = col_names_df,
        IShare_summary_df = IShare_summary_df
    ))
} # parse_IShares_summary_IT

parse_IShares_summary_US <- function(raw_data){
    # DESCRIPTION:
    # INPUT:
    # OUTPUT:

    ## -------------------------------------------------------------------------
    # 0) UNPACK LIST
    ## -------------------------------------------------------------------------
    c(code, message, status, data) %<-% raw_data
    c(tableData, config) %<-% data
    c(col_names_df, funds_list) %<-% tableData
    ## -------------------------------------------------------------------------
    # 1) GENERAL DATA PARSING
    ## -------------------------------------------------------------------------
    IShare_summary_df <- funds_list %>%
        map_dfr( ~ .x %>% map_dfc(extract_IShares_data_as_char)) %>%
        `colnames<-`(col_names_df$name) %>%
        readr::type_convert()
    ## -------------------------------------------------------------------------
    # 2) SPECIFIC DATA PARSING
    ## -------------------------------------------------------------------------
    # here there's no need to convert commas to dot as decimal separator
    IShare_summary_df <-  IShare_summary_df %>%
        mutate_at(vars(contains("AsOf")), ~as.Date(., format = "%b %d, %Y"))
    return(list(
        col_names_df = col_names_df,
        IShare_summary_df = IShare_summary_df
    ))
} # parse_IShares_summary_US

extract_IShares_data_as_char <- function(y){
    # DESCRIPTION:
    # INPUT:
    # OUTPUT:
    if (is.list(y)) {
        if (length(y) > 1)
            out <- y %>% purrr::keep(is.character) %>% purrr::pluck(1)
        else
            out <- purrr::pluck(y, 1)
    } else{
        if (length(y) > 1)
            out <- y[[1]]
        else
            out <- y
    }
    if (is.null(out))
        out <- NA
    if (!is.na(out) & out == "-")
        out <- NA
    out
} # extract_IShares_data_as_char

download_holdings_ETF_US <- function(IShare_summary_df, fund_to_download = NULL){
    # DESCRIPTION: this function downloads informations on the holding of a single
    #    ETF. For instance, suppose we want to download the holding for IVV. The file
    #    excel is located at the following link:
    #    https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund
    #    Compare it to this:
    #    https://www.ishares.com/us/products/244049/ishares-core-msci-eafe-etf/1467271812596.ajax?fileType=csv&fileName=IEFA_holdings&dataType=fund
    #    The first part could be recovered from the column `productPageUrl`, than add
    #    a fixed string and finally the ticker which could be found in the column
    #    `localExchangeTicker`.
    # INPUT:
    #    IShare_summary_df =
    #
    # OUTPUT:
    if(!is.null(fund_to_download)){
        IShare_summary_df <- IShare_summary_df %>%
            filter(localExchangeTicker %in% fund_to_download)
    }
    ## -------------------------------------------------------------------------
    # 1) CREATE THE URL
    ## -------------------------------------------------------------------------
    IShare_summary_df <- IShare_summary_df %>%
        mutate(
            effective_url = str_c("https://www.ishares.com",
                                  productPageUrl,
                                 "/1467271812596.ajax?fileType=csv&fileName=",
                                 glue::glue("{localExchangeTicker}_holdings"),
                                 "&dataType=fund")
        )
    ## -------------------------------------------------------------------------
    # 2) DOWNLOAD THE CSV FILES
    ## -------------------------------------------------------------------------
    out <- vector("list", length(IShare_summary_df$effective_url))
    for(i in seq_along(out)){
        flog.info(glue("downloading holdings of ETF {IShare_summary_df$localExchangeTicker[[i]]}"))
        out[[i]] <- tryCatch({
            IShare_summary_df$effective_url[[i]] %>%
                readr::melt_csv()
        },
        error = function(e) {
            message(e$message)
            list(NULL)
        })
    }
    names(out) <- IShare_summary_df$localExchangeTicker
    return(out)
} # download_holdings_ETF_US

parse_holdings_ETF_US <- function(holdings_ETF_US_list){
    # DESCRIPTION:
    # INPUT:
    #    holdings_ETF_US_list = names list
    # OUTPUT:
    out <- vector("list", length(holdings_ETF_US_list))
    names(out) <- names(holdings_ETF_US_list)
    for(i in seq_along(out)){
        flog.info(glue("parsing {names(holdings_ETF_US_list)[[i]]}"))
        out[[i]] <- tryCatch({
            ## 1) Find aod -----------------------------------------------------
            aod <- extract_aod_holdings_ETF_US(holdings_ETF_US_list[[i]])
            ## 2) Unmelt dataframe ---------------------------------------------
            holdings_ETF_US_df <- unmelt_holdings_ETF_US(holdings_ETF_US_list[[i]])
            ## 3) Add aod ------------------------------------------------
            holdings_ETF_US_df %>%
                mutate(aod = aod)
        },
        error = function(e){
            print(e)
            list(NULL)
        })
    }
    return(out)
} # parse_holdings_ETF_US

extract_aod_holdings_ETF_US <- function(melted_df){
    # DESCRIPTION: returns the Fund Holdings as of date from the ETF holdings csv
    #    file previoulsy read by `download_holdings_ETF_US`
    # INPUT:
    #    melted_df = melted dataframe returned by melt_csv containing the ETF US holdings
    # OUTPUT:
    aod_cell_name <- "Fund Holdings as of"
    if (aod_cell_name %in% melted_df$value){
        aod_coordinates <- melted_df %>%
            filter(value == "Fund Holdings as of") %>%
            distinct(value, .keep_all = TRUE) %>%
            select(row, col)
        aod <- melted_df %>%
            filter(row == aod_coordinates$row,
                   col == aod_coordinates$col + 1) %>%
            pull(value) %>%
            as.Date(format = "%B %d, %Y") # this might change, wrap it inside a tryCatch
    } else {
        return(NULL)
    }
} # extract_aod_holdings_ETF_US

unmelt_holdings_ETF_US <- function(melted_df){
    # DESCRIPTION:
    # INPUT:
    #    holdings_ETF_US_list = names list
    # OUTPUT:

    ## -------------------------------------------------------------------------
    # 0) PRELIMINARY FILTERS
    ## -------------------------------------------------------------------------
    metadata_value <- c("Inception Date",
                        "Fund Holdings as of",
                        "Total Net Assets",
                        "Shares Outstanding",
                        "Stock",
                        "Bond",
                        "Cash",
                        "Other")
    melted_df <- melted_df %>%
        filter(!(value %in% metadata_value),
               value != "")
    ## -------------------------------------------------------------------------
    # 1) FIND FIRST TABLE ROW
    ## -------------------------------------------------------------------------
    # remove the first few rows containing metadata
    likely_col_names <- c("ISIN", "Price", "Ticker")
    likely_col_df <- melted_df %>%
        filter(value %in% likely_col_names)
    if (nrow(likely_col_df) > 0) {
        if (nrow(likely_col_df) > 1 ) { # this happens when there'are two blocks of holdings in the csv
            start_table_row <- likely_col_df %>%
                filter(row == min(likely_col_df$row)) %>%
                pull(row) %>%
                unique()
            duplicated_header_row <- likely_col_df %>%
                filter(row != min(likely_col_df$row))
            melted_df <- melted_df %>%
                filter(!(row %in% unique(duplicated_header_row$row)))
        } else {
            start_table_row <- unique(likely_col_df$row)
        }
        # stopifnot(length(unique(likely_col_df$row)) == 1) # otherwise there's something strange
    } else {
        stop("can't determine where the table starts")
    }
    ## -------------------------------------------------------------------------
    # 2) UNMELT THE DATAFRAME
    ## -------------------------------------------------------------------------
    melted_df <- melted_df %>%
        filter(row >= start_table_row)
    table_col_names_df <- melted_df %>%
        filter(row == start_table_row)
    melted_df %>%
        filter(row > start_table_row) %>%
        left_join(
            select(table_col_names_df, col, col_names = value),
            by = "col"
        ) %>%
        tidyr::pivot_wider(row, names_from = col_names, values_from = value) %>%
        select(-row) %>%
        parse_df()
} # unmelt_holdings_ETF_US


parse_df <- function(df){
    # DESCRIPTION:
    # INPUT:
    # OUTPUT:
    df %>%
        `colnames<-`(parse_col_name(colnames(.))) %>%
        mutate_all(~replace(.x, .x == "-", NA_character_)) %>%
        mutate_all(~str_replace_all(.x, "[' ' | ,)]", "")) %>%
        mutate_all(readr::parse_guess)

} # parse_df

parse_col_name <- function(col_names){
    # DESCRIPTION:
    # INPUT:
    # OUTPUT:
    col_names %>%
        str_replace_all("[^[:alnum:]]", " ") %>%
        str_trim() %>%
        stringr::str_replace_all(fixed(" "), "_") %>%
        stringr::str_replace_all(fixed("__"), "_")

} # parse_col_name


to_mongo <- function(){
    my_collection <- mongo(collection = "ETF", db = "test")
    my_collection$insert(a)
    my_collection$count()
}
