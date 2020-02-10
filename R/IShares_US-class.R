
#' IShares_US class constructor
#'
#' IShares_US class constructor
#'
#' @inheritParams IShares
IShares_US <- function(summary_link = character(),
                       get_constituents = TRUE,
                       constituents_list = list(),
                       ...,
                       class = character()) {
    ## -------------------------------------------------------------------------
    # 1) CALL SUPERCONSTRUCTOR
    ## -------------------------------------------------------------------------
    obj <- new_IShares(
        summary_link = summary_link,
        constituents_list = constituents_list,
        ...,
        class = c(class, "IShares_US")
    )
    ## -------------------------------------------------------------------------
    # 2) PARSE CHARACTER SUMMMARY DATA
    ## -------------------------------------------------------------------------
    obj$summary_data <- parse_summary_data_US(obj$summary_data)
    ## -------------------------------------------------------------------------
    # 3) GET ETF CONSTITUENTS
    ## -------------------------------------------------------------------------
    if (obj$get_constituents) {
        constituents_list <- download_etf_constituents_US(obj$summary_data)
        template_classification <- classify_constituent_data(constituents_list)
        obj$constituents_list <- parse_etf_constituents(constituents_list,
                                                        template_classification)
    }
    return(obj)
} # IShares_US

parse_summary_data_US <- function(summary_data) {
    futile.logger::flog.info("parsing US summary data from char")
    assertthat::assert_that(is.data.frame(chr_summary_data))
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

download_etf_constituents_US <- function(summary_data, download_csv = TRUE, url_fixed_number) {
    futile.logger::flog.info("downloading US ETF constituents")
    assertthat::assert_that(is.data.frame(summary_data))
    ## -------------------------------------------------------------------------
    # 1) CREATE THE URL
    ## -------------------------------------------------------------------------
    summary_data <- summary_data %>%
        dplyr::mutate(
            effective_productPageUrl = glue::glue("https://www.ishares.com{productPageUrl}/{url_fixed_number}.ajax?fileType=csv&fileName={localExchangeTicker}_holdings&dataType=fund")
        )
    ## -------------------------------------------------------------------------
    # 2) DOWNLOAD THE CSV
    ## -------------------------------------------------------------------------
    if (download_csv) {
        dir.create("./csv_files")
        summary_data[["effective_productPageUrl"]] %>%
            purrr::iwalk( ~ {
                ticker <- summary_data[["localExchangeTicker"]][[.y]]
                if (!is.null(ticker) & !is.na(ticker)) {
                    futile.logger::flog.info(glue::glue("downloading constituents for {ticker} in csv format"))
                    try({
                        download.file(
                            url = .x,
                            destfile = glue::glue("./csv_files/{ticker}.csv")
                        )
                    })
                } else {
                    NULL
                }

            })
    }
    ## -------------------------------------------------------------------------
    # 3) DOWNLOAD MELTED DATA
    ## -------------------------------------------------------------------------
    constituents_list <- purrr::map2(
        summary_data[["effective_productPageUrl"]],
        summary_data[["localExchangeTicker"]],
        ~ {
            futile.logger::flog.info(glue::glue("downloading constituents for {.y} in melted format"))
            tryCatch({
                readr::melt_csv(file = .x)
            },
            error = function(e) {
                futile.logger::flog.error(e)
                NULL
            })
        }) %>%
        purrr::set_names(summary_data[["localExchangeTicker"]])
} # download_etf_constituents_US

classify_constituent_data <- function(constituents_list) {
    assertthat::assert_that(is.list(constituents_list))
    out <- constituents_list %>%
        purrr::imap( ~ {
            futile.logger::flog.info(glue::glue("classifying {.y}"))
            tryCatch({
                if (is_template_1(.x)) {
                    "parse_template_1"
                } else if (is_template_2(.x)) {
                    "parse_template_2"
                } else if (is_template_3(.x)) {
                    "parse_template_3"
                } else {
                    NULL
                }
            },
            error = function(e) {
                futile.logger::flog.error(glue::glue("{.y} : {e}"))
                return(NULL)
            })
        })
} # classify_constituent_data

parse_etf_constituents <- function(constituents_list,
                                   template_classification) {
    purrr::map2(
        constituents_list,
        template_classification,
        ~ {
            if (!is.null(.y)) {
                try({ get(.y)(.x) })
            } else {
                NULL
            }
        }
    )
} # parse_etf_constituents

#' check if data is in template 1 format
#'
#' the constituents are in template 1 if in the range A2:A8 there are the following
#' strings: Fund Holdings as of Inception Date, Shares Outstanding, Stock, Bond
#' Cash and Other. The column B there are the values respectively.
#'
#' @param melted_data
#'
#' @return \code{TRUE} if data is in template 1 format \code{FALSE} otherwise
is_template_1 <- function(melted_data) {
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
            dplyr::filter(n > 1)
        if (nrow(occurrences) == 0) TRUE else FALSE
    } else {
        FALSE
    }
} # is_template_1

parse_template_1 <- function(melted_data) {
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
        dplyr::inner_join(
            col_names,
            by = "col") %>%
        tidyr::pivot_wider(
            id_cols = row,
            names_from = col_name,
            values_from = value) %>%
        dplyr::mutate(aod = aod) %>%
        purrr::modify_if(is.character, readr::parse_guess)
} # parse_template_1

#' check if data is in template 2 format
#'
#' data is in template 2 format if there're two template 1 stacked one above the
#' other. This is the case for the Russell 2500 ETF. This ETF buys the Russel 2000
#' and than buys 500 stocks. The first chunk of data has the  Russel 2000 ETF while
#' the second chinck expands this ETF showing all the 2500 names
#'
#'
is_template_2 <- function(melted_data) {
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
            dplyr::filter(n == 2)
        if (nrow(occurrences) == 0) FALSE else TRUE
    } else {
        FALSE
    }
} # is_template_2

parse_template_2 <- function(melted_data) {
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
        parse_template_1 %>%
        dplyr::mutate(etf_name = first_etf_name)
    chunk2 <- melted_data %>%
        dplyr::filter(row >= second_etf_name_nrow) %>%
        dplyr::mutate(row = row - second_etf_name_nrow + 1) %>%
        parse_template_1() %>%
        dplyr::mutate(etf_name = second_etf_name)
    dplyr::bind_rows(chunk1, chunk2)
} # parse_template_2

is_template_3 <- function(melted_data) {
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

parse_template_3 <- function(melted_data) {
    assertthat::assert_that(is.data.frame(melted_data))
    col_names <- melted_data %>%
        dplyr::filter(value %in% c("Name", "Weight (%)")) %>%
        dplyr::select(col, col_name = value)
    melted_data %>%
        dplyr::filter(row > 3) %>%
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
