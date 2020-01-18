## -----------------------------------------------------------------------------
# FUNCTIONS FOR DOWNLOADING AND PARSING SPDR ETFs FILEs
## -----------------------------------------------------------------------------
test_unit <- function(n_test){
    n_test <- 1
    if(n_test == 1){
        aod <- "20190912"
        # download_all_etf_SPDR(aod)
        file_name <- glue::glue("ETF/data_SPDR/summary/SPDR_summary_{aod}.xlsx")
        c(info_df, exchanges_df, perfo_df) %<-% read_SPDR_summary(file_name)
    }

} # test_unit

download_summary_SPDR <- function(aod){
    # DESCRIPTION: download the excel file which summarizes all the SPDR etfs. It can
    #    be found at the following link: https://it.spdrs.com/en/professional/product/index.seam
    # INPUT:
    #    aod =
    # OUTPUT:
    #    aod =

    SPDR_path <- glue::glue("https://it.spdrs.com/library-content/public/spdr-product-data-eu-en-{aod}.xlsx")
    file_name <- glue::glue("ETF/data_SPDR/summary/SPDR_summary_{aod}.xlsx")
    if(file.exists(file_name)){
        flog.info(glue::glue("File {file_name} is already present. I'll use this one! : {aod}"))
        return(aod)
    }
    response <- httr::GET(SPDR_path, write_disk(file_name, overwrite=TRUE))

} # download_all_etf_SPDR

read_SPDR_summary <- function(file_name){
    # DESCRIPTION: we're going to divide the excel file in blocks. A block is defined
    #   as a rectangular piece of the file excel.
    # INPUT:
    #   file_name =
    # OUTPUT:

    ## ---------------------------------------------------------------------------
    # 0) READ FILE INTO A TIDY EXCEL DATAFRAME
    ## ---------------------------------------------------------------------------
    # file_name <- glue::glue("ETF/data_SPDR/summary/SPDR_summary_{aod}.xlsx") # to be given as input
    tidy_xl <- tidyxl::xlsx_cells(path = file_name)
    max_row <- tidy_xl %>%
        filter(!is_blank) %>%
        filter(stringr::str_detect(character,"Source:" )) %>%
        pull(row)
    ## ---------------------------------------------------------------------------
    # 1) BLOCK GENERAL INFORMATION
    ## ---------------------------------------------------------------------------
    start_row_general_info <- 3
    start_col_general_info <- 1
    last_col_general_info <- tidy_xl %>%
        filter(character == "Settlement Cycle") %>%
        pull(col)
    headers_general_info <- tidy_xl %>%
        filter(between(col, start_col_general_info, last_col_general_info)) %>%
        filter(row == start_row_general_info) %>%
        pull(character)
    stopifnot(length(headers_general_info) == last_col_general_info)
    headers_general_info %>%
        purrr::map_dfc(function(x){
            tibble(!!x := get_col_char_single_header(tidy_xl, x, max_row))
        }) -> general_info_df
    ## ---------------------------------------------------------------------------
    # 2) BLOCK EXCHANGES
    ## ---------------------------------------------------------------------------
    start_col_exchanges <- last_col_general_info + 1
    last_col_exchanges <- tidy_xl %>%
        filter(character == "Bid/Offer Spread") %>%
        pull(col) - 1
    exchanges_tidy_xl <- tidy_xl %>%
        filter(between(col, start_col_exchanges, last_col_exchanges),
               between(row, start_row_general_info, max_row))
    exchanges <- c("London Stock Exchange", "Deutsche B?rse", "SIX Swiss Exchange",
                   "Borsa Italiana", "Euronext Paris", "Euronext Amsterdam")
    start_col_single_exchanges <- exchanges_tidy_xl %>%
        filter(character %in% exchanges) %>%
        pull(col)
    end_col_single_exchanges <- c(start_col_single_exchanges[-1] - 1, last_col_exchanges)
    pmap_dfr(list(start_col_single_exchanges, end_col_single_exchanges, exchanges), function(x,y,z){
        all_cells <- exchanges_tidy_xl %>%
            filter(!is_blank) %>%
            filter(between(col,x,y), between(row, start_row_general_info+1, max_row-1)) %>%
            select(row, col, data_type, character, numeric)
        first_header_row <- all_cells %>%
            filter(row == start_row_general_info+1) %>%
            select(row, col, currency = character)
        second_header_row <- all_cells %>%
            filter(row == start_row_general_info+2) %>%
            select(row, col, ticker_type = character)
        data_cells <- all_cells %>%
            filter(row >= start_row_general_info+3) %>%
            select(row, col, value = character)
        data_cells %>%
            enhead(first_header_row, "NNW") %>%
            enhead(second_header_row, "N") %>%
            # select(-row, -col) %>%
            mutate(exchange = z) %>%
            select(col, row, value, ticker_type, currency, exchange)
    }) -> exchanges_df
    ## ---------------------------------------------------------------------------
    # 2) BLOCK NAV INFORMATION
    ## ---------------------------------------------------------------------------
    start_col_nav_info <- last_col_exchanges + 1
    last_col_nav_info <- tidy_xl %>%
        filter(character == "Perf as of Date") %>%
        pull(col)
    headers_nav_info <- tidy_xl %>%
        filter(between(col, start_col_nav_info, last_col_nav_info)) %>%
        filter(row == start_row_general_info) %>%
        pull(character)
    headers_nav_info %>%
        # str_replace_all("[^[:alnum:]]", " ") %>%
        # str_trim() %>%
        # stringr::str_replace_all(fixed(" "), "_") %>%
        purrr::map_dfc(function(x){
            tibble(!!x := get_col_char_single_header(tidy_xl, x, max_row))
        }) -> nav_info_df
    general_info_df <- bind_cols(general_info_df, nav_info_df)
    ## ---------------------------------------------------------------------------
    # 4) BLOCK PERFO
    ## ---------------------------------------------------------------------------
    start_col_perfo <- last_col_nav_info + 1
    last_col_perfo <- tidy_xl %>%
        filter(character == "Fund Characteristics as of Date") %>%
        pull(col) - 1
    all_cells <- tidy_xl %>%
        filter(!is_blank) %>%
        filter(between(col, start_col_perfo, last_col_perfo)) %>%
        filter(between(row, start_row_general_info, max_row)) %>%
        select(row, col, data_type, character, numeric)
    all_cells[is.na(all_cells$character), "character"] <- all_cells[is.na(all_cells$character), "numeric"]
    first_header_row <- all_cells %>%
        filter(row == start_row_general_info) %>%
        select(row, col, perfo_type = character)
    second_header_row <- all_cells %>%
        filter(row == start_row_general_info+2) %>%
        select(row, col, period = character)
    data_cells <- all_cells %>%
        filter(row >= start_row_general_info+3) %>%
        select(row, col, value = character)
    data_cells %>%
        enhead(first_header_row, "NNW") %>%
        enhead(second_header_row, "N") %>%
        select(col, row, value, period, perfo_type) -> perfo_df
    ## ---------------------------------------------------------------------------
    # 5) FUND CHARACTERISTICS
    ## ---------------------------------------------------------------------------
    start_col_fund_charact <- last_col_perfo + 2
    last_col_fund_charact <- tidy_xl %>%
        filter(character == "Price/Earnings Ratio FY1") %>%
        pull(col)
    headers_fund_charact <- tidy_xl %>%
        filter(between(col, start_col_fund_charact, last_col_fund_charact)) %>%
        filter(row == start_row_general_info+2) %>%
        pull(character)
    fund_charact_aod_df <- tibble(
        `Fund Characteristics as of Date` = get_col_char_single_header(tidy_xl, "Fund Characteristics as of Date", max_row)
    )
    headers_fund_charact %>%
        # str_replace_all("[^[:alnum:]]", " ") %>%
        # str_trim() %>%
        # stringr::str_replace_all(fixed(" "), "_") %>%
        purrr::map_dfc(function(x){
            tibble(!!x := get_col_char_single_header(tidy_xl, x, max_row))
        }) -> fund_charact_df
    fund_charact_df <- bind_cols(fund_charact_aod_df, fund_charact_df)
    ## -------------------------------------------------------------------------
    # 6) FINAL BINDING
    ## -------------------------------------------------------------------------
    info_df <- bind_cols(general_info_df, fund_charact_df)
    mapping_df <- tidy_xl %>%
        filter(character %in% info_df$`Fund Name`) %>%
        select(row, character)
    exchanges_df <- exchanges_df %>%
        left_join(mapping_df, by = "row")
    perfo_df <- perfo_df %>%
        left_join(mapping_df, by = "row")
    ## -------------------------------------------------------------------------
    # 7) COLUMN PARSING
    ## -------------------------------------------------------------------------

    return(list(
        info_df = info_df,
        exchanges_df = exchanges_df,
        perfo_df = perfo_df
    ))

} # read_SPDR_summary

get_col_char_single_header <- function(tidy_xl, col_names, max_row){
    # DESCRIPTION: function for reading the content of a char column by column name
    # or by
    # INPUT:
    # OUTPUT:

    n_cols <- tidy_xl %>%
        filter(character %in% col_names) %>%
        pull(col)
    start_row <- tidy_xl %>%
        filter(character %in% col_names) %>%
        pull(row) %>%
        unique()
    stopifnot(length(start_row) == 1)
    tidy_xl %>%
        filter(col %in% n_cols,
               !is.na(character),
               row > start_row,
               row < max_row) %>%
        pull(character)
} # get_column_single_header

parse_summary_SPDR <- function(info_df, exchanges_df, perfo_df){
    # DESCRIPTION:
    # INPUT:
    # OUTPUT:

    perfo_df$value <- str_replace(perfo_df$value, "-",NA_character_) %>%
        parse_number()/100 # there numbers are all percentages
    ## -------------------------------------------------------------------------
    # 1) PARSE INFO_DF
    ## -------------------------------------------------------------------------
    # colnames(info_df) <- parse_col_name(colnames(info_df))
    info_df <- info_df %>%
        `colnames<-`(parse_col_name(colnames(.))) %>%
        mutate_all(~replace(., . == "-", NA_character_)) %>%
        mutate_all(~str_replace_all(., "[^([:alnum:]|' '| ,)]", "")) %>%
        mutate_all(readr::parse_guess)
    parse_guess(info_df$Effective_Convexity)
    ## -------------------------------------------------------------------------
    # 2) PARSE EXCHANGE_DF
    ## -------------------------------------------------------------------------

    ## -------------------------------------------------------------------------
    # 1) PARSE PERFO_DF
    ## -------------------------------------------------------------------------

} # parse_summary_SPDR

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
