
#' IShares_US class constructor
#'
#' the \code{IShares_US} class is a subclass of the \code{IShares} class and represents
#' the ETF data from the US IShares website. It overrides some methods of the
#' \code{IShares} class for taking into consideration American formatting specificity
#'
#' @param get_constituents \code{TRUE} for downloading ETF constituents, \code{FALSE}
#'    otherwise
#' @param download_constituents_csv \code{TRUE} for saving the csv constituents files
#'    from the ETF web site, \code{FALSE} otherwise
#' @param melted_constituents_list constituents data in melted format
#' @param constituents_list constituents data in parsed format
#' @param tickers_to_keep character vector containing the tickers to keep. One might
#' want to keep just a subset of tickers due to storage constraints
#' @param ... subclass additional attributes
#' @param class subclass names
#' @inheritParams IShares
#'
#' @return a new \code{IShares_US} object
IShares_US <- function(summary_link = character(),
                       get_constituents = TRUE,
                       download_constituents_csv = FALSE,
                       melted_constituents_list = list(),
                       constituents_list = list(),
                       tickers_to_keep = character(),
                       ...,
                       class = character()) {
    ## -------------------------------------------------------------------------
    # 1) CALL SUPERCONSTRUCTOR
    ## -------------------------------------------------------------------------
    obj <- new_IShares(
        summary_link = summary_link,
        melted_constituents_list = melted_constituents_list,
        constituents_list = constituents_list,
        region = "US",
        tickers_to_keep = tickers_to_keep,
        ...,
        class = c(class, "IShares_US")
    )
    ## -------------------------------------------------------------------------
    # 2) PARSE CHARACTER SUMMMARY DATA
    ## -------------------------------------------------------------------------
    obj$summary_data <- parse_summary_data_US(get_summary_data(obj))
    ## -------------------------------------------------------------------------
    # 3) GET ETF CONSTITUENTS
    ## -------------------------------------------------------------------------
    if (get_constituents) {
        obj$melted_constituents_list <- download_etf_constituents(
            summary_data = get_summary_data(obj),
            url_fixed_number = get_url_fixed_number(obj),
            download_csv = download_constituents_csv,
            region = get_region(obj)
        )
        template_classification <- classify_constituent_data(
            melted_constituents_list = obj$melted_constituents_list,
            region = "US",
            n_template = 3
        )
        obj$constituents_list <- parse_etf_constituents(
            melted_constituents_list = obj$melted_constituents_list,
            template_classification = template_classification
        )
    }
    return(obj)
} # IShares_US

parse_summary_data_US <- function(summary_data) {
    futile.logger::flog.info("parsing US summary data from char")
    assertthat::assert_that(is.data.frame(summary_data))
    summary_data %>%
        purrr::modify_if(is.character, readr::parse_guess) %>%
        purrr::modify_at(dplyr::vars(dplyr::contains("AsOf")), parse_date_col_US) %>%
        purrr::modify_at("inceptionDate", parse_date_col_US)
} # parse_summary_data_IT

parse_date_col_US <- function(vec) {
    if (all(is.character(vec))) {
        vec %>% readr::parse_date(format = "%b %d, %Y")
    } else {
        vec
    }
} # parse_date_col_US

#' check if data is in template 1 format
#'
#' the constituents are in template 1 if in the range A2:A8 there are the following
#' strings: Fund Holdings as of, Inception Date, Shares Outstanding, Stock, Bond
#' Cash and Other. In the column B there are the values respectively.
#'
#' @param melted_data constituent dataframe in melted format
#'
#' @return \code{TRUE} if data is in template 1 format, \code{FALSE} otherwise
is_template_1_US <- function(melted_data) {
    if (is.null(melted_data)) return(FALSE)
    assertthat::assert_that(is.data.frame(melted_data))
    incipit_col <- c(
        "Fund Holdings as of",
        "Inception Date",
        "Shares Outstanding",
        "Stock",
        "Bond",
        "Cash",
        "Other")
    if (all(incipit_col %in% melted_data$value)) { # make sure these cols are not repeted below
        occurrences <- melted_data %>%
            dplyr::filter(value %in% incipit_col, col == 1) %>%
            dplyr::group_by(value) %>%
            dplyr::summarise(n = dplyr::n()) %>%
            dplyr::mutate(is_occurrance_1 = n == 1)
        if (any(occurrences$is_occurrance_1)) TRUE else FALSE
    } else {
        FALSE
    }
} # is_template_1

#' Parse template 1 US data
#'
#' this function parses the constituents data which is in the template 1 format.
#' After extracting the column names, the character values (indeed the function
#' readr::melt_csv convert all the data to char) are trimmed for removing blanks.
#' The SEDOL colum is forced to be char
#'
#' @param melted_data constituent data in melted format
#'
#' @return a parsed tibble with constituent data
parse_template_1_US <- function(melted_data) {
    assertthat::assert_that(is.data.frame(melted_data))
    aod <- melted_data %>%
        dplyr::filter(row == 2, col == 2) %>%
        dplyr::pull(value) %>%
        readr::parse_date(format = "%b %d, %Y")
    # there could be duplicated column names. By using the function dplyr::distinct
    # we take just one of these columns. For example this happens for EMB where
    # the column `Asset Class` is repeted twice
    col_names <- melted_data %>%
        dplyr::filter(row == 10) %>% # col names start at row 10
        dplyr::select(col, col_name = value) %>%
        dplyr::distinct(col_name, .keep_all = TRUE)
    data <- melted_data %>%
        dplyr::filter(row > 10) %>%
        dplyr::filter(!stringr::str_detect(value, pattern = "^\\s+$")) %>% # remove whitespaces
        dplyr::inner_join(
            col_names,
            by = "col") %>%
        tidyr::pivot_wider(
            id_cols = row,
            names_from = col_name,
            values_from = value) %>%
        dplyr::select(-row) %>%
        dplyr::filter_all(dplyr::any_vars(!is.na(.))) %>%
        dplyr::mutate(aod = aod, idx = 1:nrow(.)) %>%
        dplyr::select(aod, idx, dplyr::everything()) %>%
        purrr::modify_if(is.character, readr::parse_guess)
    # the SEDOL must bu a string
    if ("SEDOL" %in% colnames(data)) data$SEDOL <- as.character(data$SEDOL)
    return(data)
} # parse_template_1

#' Check if data is in template 2 format
#'
#' data is in template 2 format if there're two template 1 stacked one above the
#' other. This is the case for the Russell 2500 ETF. This ETF buys the Russel 2000
#' and than buys 500 stocks. The first chunk of data has the  Russel 2000 ETF while
#' the second chinck expands this ETF showing all the 2500 names
#'
#' @inheritParams parse_template_1_US
#'
#' @return \code{TRUE} if data is in template 2 format, \code{FALSE} otherwise
is_template_2_US <- function(melted_data) {
    if (is.null(melted_data)) return(FALSE)
    assertthat::assert_that(is.data.frame(melted_data))
    incipit_col <- c(
        "Fund Holdings as of",
        "Inception Date",
        "Shares Outstanding",
        "Stock",
        "Bond",
        "Cash",
        "Other")
    if (all(incipit_col %in% melted_data$value)) { # make sure these cols are not repeted below
        occurrences <- melted_data %>%
            dplyr::filter(value %in% incipit_col, col == 1) %>%
            dplyr::group_by(value) %>%
            dplyr::summarise(n = dplyr::n()) %>%
            dplyr::mutate(is_occurrance_1 = n == 1)
        if (any(occurrences$is_occurrance_1)) FALSE else TRUE
    } else {
        FALSE
    }
} # is_template_2

#' Parse template 2 US data
#'
#' this function find the two blocks of data and passes them to \link{parse_template_1_US}
#'
#' @inheritParams is_template_1_US
#'
#' @return a parsed tibble with constituent data
parse_template_2_US <- function(melted_data) {
    assertthat::assert_that(is.data.frame(melted_data))
    ## -------------------------------------------------------------------------
    # 1) EXTRACT AOD
    ## -------------------------------------------------------------------------
    aod <- melted_data %>%
        dplyr::filter(row == 2, col == 2) %>%
        dplyr::pull(value) %>%
        readr::parse_date(format = "%b %d, %Y")
    ## -------------------------------------------------------------------------
    # 2) EXTRACT ETF NAMES
    ## -------------------------------------------------------------------------
    first_etf_name <- melted_data %>%
        dplyr::filter(row == 1, col == 1) %>%
        dplyr::pull(value)
    second_etf_name_nrow <- melted_data %>%
        dplyr::filter(value == "Fund Holdings as of") %>%
        dplyr::filter(row == max(row)) %>%
        dplyr::pull(row) - 1
    second_etf_name <- melted_data %>%
        dplyr::filter(row == second_etf_name_nrow, col == 1) %>%
        dplyr::pull(value)
    ## -------------------------------------------------------------------------
    # 3) EXTRACT FIRST ETF CONSTITUENTS
    ## -------------------------------------------------------------------------
    chunk1 <- melted_data %>%
        dplyr::filter(row < second_etf_name_nrow - 1) %>% # blank separating line
        parse_template_1_US %>%
        dplyr::mutate(etf_name = first_etf_name)
    chunk2 <- melted_data %>%
        dplyr::filter(row >= second_etf_name_nrow) %>%
        dplyr::mutate(row = row - second_etf_name_nrow + 1) %>%
        parse_template_1_US() %>%
        dplyr::mutate(etf_name = second_etf_name)
    dplyr::bind_rows(chunk1, chunk2)
} # parse_template_2

is_template_3_US <- function(melted_data) {
    if (is.null(melted_data)) return(FALSE)
    assertthat::assert_that(is.data.frame(melted_data))
    col_names <- melted_data %>%
        dplyr::filter(row == 3, col %in% c(1, 2)) %>%
        dplyr::pull(value)
    if (all(col_names == c("Name", "Weight (%)"))) {
        TRUE
    } else {
        FALSE
    }
} # is_template_3

parse_template_3_US <- function(melted_data) {
    assertthat::assert_that(is.data.frame(melted_data))
    col_names <- melted_data %>%
        dplyr::filter(value %in% c("Name", "Weight (%)")) %>%
        dplyr::select(col, col_name = value)
    melted_data %>%
        dplyr::filter(row > 3) %>%
        dplyr::filter(!stringr::str_detect(value, pattern = "^\\s+$")) %>% # remove whitespaces
        dplyr::inner_join(
            col_names,
            by = "col"
        ) %>%
        tidyr::pivot_wider(
            id_cols = row,
            names_from = col_name,
            values_from = value
        ) %>%
        na.omit() %>%
        dplyr::select(-row)
} # parse_template_3

get_url_fixed_number.IShares_US <- function(obj) {
    as.character("1467271812596")
} # get_url_fixed_number.IShares_IT
