#' IShares class constructor
#'
#' IShares class constructor
#'
#' @param summary_link link where the IShares summary data is downloaded from.
#'    This is the data summarizing all the ETF of the IShares provider
#'
#' @export
IShares <- function(summary_link) {
    stopifnot(is.character(summary_link))
    new_IShares(
        summary_link = summary_link
    )
} # IShares

new_IShares <- function(summary_link,
                        ...,
                        class = character()) {
    res_parsed <- download_summary_data(summary_link = summary_link)
    summary_data <- parse_summary_data(res_parsed = res_parsed)
    new_ETF(
        summary_link = summary_link,
        summary_data = summary_data,
        ...,
        class = c(class, "IShares")
    )
} # new_IShares

#' @export
download_summary_data <- function(summary_link, as = "parsed") {
    futile.logger::flog.info(
        glue::glue("Downloading IShares summary data from {summary_link} and returning it as {dplyr::if_else(as == \"parsed\", \"list\", \"text\")}")
    )
    res <- httr::GET(url = summary_link)
    res_parsed <- httr::content(res, as = as)
    return(res_parsed)
} # download_summary_data

#' Parse IShares list
#'
#' @param res_parsed a list returned by the \link{httr::content} function. The list
#'     ha the following attributes: \itemize{
#'     \item \code{code}
#'     \item \code{message}
#'     \item \code{status}
#'     \item \code{code}
#'     }
#'     The most important attribute is data which contains the actual IShares data.
#'     In particular, it's a list with the following attributes: \itemize{
#'        \item \code{tableData} a list with two attributes: \code{columns} and
#'           \code{data}. The first one store the column names and the second the actual data
#'        \item \code{config}
#'     }
parse_summary_data <- function(res_parsed) {
    assertthat::assert_that(is.list(res_parsed))
    ## -------------------------------------------------------------------------
    # 1) COLNAMES PARSING
    ## -------------------------------------------------------------------------
    columns_df <- tibble::tibble(a = res_parsed$data$tableData$columns) %>%
        tidyr::unnest_wider(a) %>%
        dplyr::mutate(col_id = 1:nrow(.)) %>%
        dplyr::select(col_id, col_name = name)
    ## -------------------------------------------------------------------------
    # 2) DATA PARSING
    ## -------------------------------------------------------------------------
    out <- res_parsed$data$tableData$data %>%
        purrr::map(~ {
            column_data_df <- tibble::tibble(etf_data_list = .x) %>%
                dplyr::mutate(col_id = 1:nrow(.))
            column_data_df_list <- column_data_df %>%
                dplyr::filter(purrr::map_lgl(etf_data_list, is.list)) %>%
                purrr::quietly(tidyr::unnest_wider)(etf_data_list) %>%
                purrr::pluck("result") %>%
                dplyr::select(col_id, value = d)
            column_data_df_chr <- column_data_df %>%
                dplyr::filter(purrr::map_lgl(etf_data_list, is.character)) %>%
                dplyr::mutate(etf_data_list = purrr::map_chr(etf_data_list, ~
                                                                 parse_chr_data(unlist(.x)))) %>%
                dplyr::select(col_id, value = etf_data_list)
            column_data_df_num <- column_data_df %>%
                dplyr::filter(purrr::map_lgl(etf_data_list, is.numeric)) %>%
                dplyr::mutate(value = unlist(etf_data_list)) %>%
                dplyr::select(col_id, value)
            list(column_data_df_chr, column_data_df_list, column_data_df_num) %>%
                purrr::map_dfc(~ {
                    .x %>%
                        dplyr::left_join(columns_df,
                                         by = "col_id") %>%
                        dplyr::select(-col_id) %>%
                        tidyr::pivot_wider(names_from = col_name, values_from = value)
                })
        })
    out <- out %>%
        purrr::reduce(dplyr::bind_rows)
} # parse_summary_data

parse_chr_data <- function(s) {
    dplyr::case_when(
        s == "-" ~ NA_character_,
        TRUE ~ as.character(s)
    )
} # parse_chr_data

#' Download IShares constituents
#'
#' @param summary_data summary data dataframe as returned by \link{parse_summary_data}
#' @param download_csv if \code{TRUE} the constituents csv files will be saved in
#'    a local folder called `csv_files`
#' @param url_fixed_number
#'
#' @return a named-list. Names are the \code{localExchangeTicker} found in \code{summary_data}
#'    and the elements are the csv file found in each ETF web page in melted format.
#'    By melted format we mean as output of the function \link{readr::melt_csv}
download_etf_constituents <- function(summary_data, download_csv = TRUE, url_fixed_number) {
    # futile.logger::flog.info("downloading US ETF constituents")
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
    # keep only funds with a ticker, the other are index fund
    localExchangeTicker <-
        summary_data[["localExchangeTicker"]][!is.na(summary_data[["localExchangeTicker"]])]
    effective_productPageUrl <-
        summary_data[["effective_productPageUrl"]][!is.na(summary_data[["localExchangeTicker"]])]
    constituents_list <- purrr::map2(
        effective_productPageUrl,
        localExchangeTicker,
        ~ {
            if (!is.null(.y) & !is.na(.y)) {
                futile.logger::flog.info(glue::glue("downloading constituents for {.y} in melted format"))
                tryCatch({
                    readr::melt_csv(file = .x)
                },
                error = function(e) {
                    futile.logger::flog.error(e)
                    NULL
                })
            } else {
                NULL
            }
        }) %>%
        purrr::set_names(localExchangeTicker)
} # download_etf_constituents_US

classify_constituent_data <- function(constituents_list, region = "US", n_template = 3) {
    assertthat::assert_that(is.list(constituents_list))
    out <- constituents_list %>%
        purrr::imap( ~ {
            futile.logger::flog.info(glue::glue("classifying {.y}"))
            tryCatch({
                out <- NULL
                for (i in 1:n_template) {
                    if (get(glue::glue("is_template_{i}_{region}"))(.x)) {
                        out <- glue::glue("parse_template_{i}_{region}")
                        break
                    }
                }
                out
            },
            error = function(e) {
                futile.logger::flog.error(glue::glue("{.y} : {e}"))
                return(NULL)
            })
        })
} # classify_constituent_data

parse_etf_constituents <- function(constituents_list,
                                   template_classification) {
    out <- vector("list", length(constituents_list))
    for (i in seq_along(constituents_list)) {
        if (!is.null(constituents_list[[i]])) {
            futile.logger::flog.info(glue::glue("parsing {names(constituents_list)[[i]]} using {template_classification[[i]]}"))
            out[[i]] <- tryCatch({
                get(template_classification[[i]])(constituents_list[[i]])
            },
            error = function(e) {
                futile.logger::flog.error(e)
                return(NULL)
            })
        } else {
            NULL
        }
    }
    names(out) <- names(constituents_list)
    return(out)
} # parse_etf_constituents


#' @export
get_summary_data <- function(obj) {
    UseMethod("get_summary_data")
} # get_summary_data

get_summary_data.IShares <- function(obj) {
    assertthat::has_name(obj, "summary_data")
    obj[["summary_data"]]
}
