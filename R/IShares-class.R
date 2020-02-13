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
    futile.logger::flog.info("parsing summary data")
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
    # since the column `localExchangeTicker` will be used extensively in the code
    # we want to make sure it doesn't contain white spaces
    if ("localExchangeTicker" %in% colnames(out)) {
        out$localExchangeTicker <- out$localExchangeTicker %>%
            stringr::str_trim()
    } else {
        futile.logger::flog.warn("the column `localExchangeTicker` doesn't exist in summary data!")
    }
    return(out)
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
                    readr::melt_csv(file = .x,
                                    trim_ws = TRUE,
                                    na = c("", "NA", "-", " "))

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

classify_constituent_data <- function(melted_constituents_list, region = "US", n_template = 3) {
    assertthat::assert_that(is.list(melted_constituents_list))
    out <- melted_constituents_list %>%
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

parse_etf_constituents <- function(melted_constituents_list,
                                   template_classification) {
    ## -------------------------------------------------------------------------
    # 1) PARSE ETF CONSTITUENTS
    ## -------------------------------------------------------------------------
    out <- vector("list", length(melted_constituents_list))
    for (i in seq_along(melted_constituents_list)) {
        if (!is.null(melted_constituents_list[[i]])) {
            futile.logger::flog.info(glue::glue("parsing {names(melted_constituents_list)[[i]]} using {template_classification[[i]]}"))
            out[[i]] <- tryCatch({
                get(template_classification[[i]])(melted_constituents_list[[i]])
            },
            error = function(e) {
                futile.logger::flog.error(e)
                return(NULL)
            })
        } else {
            NULL
        }
    }
    ## -------------------------------------------------------------------------
    # 2) SET LIST NAMES
    ## -------------------------------------------------------------------------
    names(out) <- names(melted_constituents_list)
    ## -------------------------------------------------------------------------
    # 3) SIGNAL ETF WITH NO CONSTITUENTS
    ## -------------------------------------------------------------------------
    is_null_lgl <- melted_constituents_list %>% purrr::map_lgl(is.null)
    if (length(is_null_lgl) > 0) {
        out[is_null_lgl] %>%
            purrr::iwalk( ~ {
                futile.logger::flog.warn(glue::glue("etf {.y} doesn't have constituents data"))
            })
    }
    # out <- out[!is_null_lgl]
    ## -------------------------------------------------------------------------
    # 4) PARSE COLUMN NAMES
    ## -------------------------------------------------------------------------
    out <- out %>% purrr::modify_if(purrr::negate(is.null), rename_col)
    return(out)
} # parse_etf_constituents

rename_col <- function(df) {
    df %>%
        dplyr::rename_all( ~ {
            .x %>%
                stringr::str_replace_all(pattern = "[:punct:]", "") %>%
                stringr::str_trim() %>%
                stringr::str_replace_all(pattern = " ", "_") %>%
                stringr::str_to_lower()
        })
} # rename_col

#' @export
add_const_to_summary_data <- function(obj) {
    UseMethod("add_const_to_summary_data")
} # add_const_to_summary_data

add_const_to_summary_data <- function(obj) {
    if (!purrr::is_empty(get_constituents(obj, ticker = "all")) &
        !purrr::is_empty(get_summary_data(obj))) {
        tibble::tibble(
            ticker = names(get_constituents(obj, ticker = "all")),
            constituents = get_constituents(obj, ticker = "all")
        ) %>%
            dplyr::right_join(
                get_summary_data(obj),
                by = c("ticker"="localExchangeTicker")
            )
    }
} # add_const_to_summary_data

#' Convert data in mongo format
#'
#' The data in mongo is saved using the aod and ticker as keys and the data is
#' a list where the first field if the row from summary_data and the second the
#' constituents datafarme
#'
#' @param obj a \code{IShares} object or one of its subclasses
#'
#' @return
#'
#' @export
to_mongo_data_format <- function(obj) {
    UseMethod("to_mongo_data_format")
} # to_mongo_data_format

#' @export
to_mongo_data_format <- function(obj) {
    tickers <- names(get_constituents(obj, ticker = "all"))
    constituents_list <- get_constituents(obj)
    summary_data <- get_summary_data(obj) %>%
        dplyr::filter(localExchangeTicker %in% tickers)
    out <- vector("list", length(tickers))
    for (i in seq_along(tickers)) {
        futile.logger::flog.info(glue::glue("converting in mongo format {tickers[[i]]}"))
        summary_data_slice <- summary_data %>%
            dplyr::filter(localExchangeTicker == tickers[[i]])
        assertthat::assert_that(nrow(summary_data_slice) == 1)
        ## 1) get aod ----------------------------------------------------------
        if ("navAmountAsOf" %in% colnames(summary_data_slice)) {
            aod <- summary_data_slice[["navAmountAsOf"]]
            if (is.na(aod)) {
                futile.logger::flog.error(glue::glue("aod NA for {tickers[[i]]}, not exported to mongo"))
                next
            }
        } else {
            futile.logger::flog.error(glue::glue("`summary_data` has no column `navAmountAsOf`!"))
            next
        }
        ## 2) get constituents -------------------------------------------------
        constituents_df <- constituents_list[[i]]
        if (is.null(constituents_df)) {
            futile.logger::flog.warn(glue::glue("{tickers[[i]]} has no constituents, exporting only summary data"))
        }
        ## 3) exporting to mongo format  ---------------------------------------
        out[[i]] <- list(
            aod = aod ,
            ticker = glue::glue("{tickers[[i]]}_{get_region(obj)}"),
            data = list(summary_data = summary_data_slice,
                        constituents = constituents_df)
        )
    }
    out <- out %>%
        purrr::set_names(tickers) %>%
        purrr::discard(is.null)
    dplyr::setdiff(tickers, names(out)) %>%
        purrr::walk( ~ {
            futile.logger::flog.info(glue::glue("{.x} not expored to mongo format"))
        })
    return(out)
} # to_mongo_data_format

#' @export
get_summary_data <- function(obj) {
    UseMethod("get_summary_data")
} # get_summary_data

get_summary_data.IShares <- function(obj) {
    assertthat::has_name(obj, "summary_data")
    obj[["summary_data"]]
}

#' @export
get_constituents <- function(obj, ticker) {
    UseMethod("get_constituents")
} # get_constituents

#' @export
get_constituents <- function(obj, ticker = "all") {
    assertthat::has_name(obj, "constituents_list")
    if (ticker == "all") {
        obj$constituents_list
    } else {
        assertthat::has_name(obj$constituents_list, ticker)
        obj$constituents_list[ticker]
    }
} # get_constituents

#' @export
get_melted_constituents_list <- function(obj, ticker) {
    UseMethod("get_melted_constituents_list")
} # get_melted_constituents_list

get_melted_constituents_list <- function(obj, ticker = "all") {
    assertthat::has_name(obj, "melted_constituents_list")
    if (ticker == "all") {
        obj$melted_constituents_list
    } else {
        assertthat::has_name(obj$melted_constituents_list, ticker)
        obj$melted_constituents_list[ticker]
    }
} # get_constituents

#' @export
get_region <- function(obj) {
    UseMethod("get_region")
} # get_region

get_region.IShares <- function(obj) {
    assertthat::has_name(obj, "region")
    obj[["region"]]
} # get_region


#' @export
to_csv <- function(obj, output_folder) {
    UseMethod("to_csv")
} # to_csv

#' @export
to_csv <- function(obj, output_folder) {
    ## -------------------------------------------------------------------------
    # 1) EXPORT SUMMARY DATA
    ## -------------------------------------------------------------------------
    summary_data <- get_summary_data(obj)
    if (!purrr::is_empty(summary_data)) {
        futile.logger::flog.info(glue::glue("exporting summary data for object of class {class(obj)[[1]]}, output_folder = {output_folder}"))
        readr::write_csv(summary_data, path = glue::glue("{output_folder}/{get_region(obj)}_summary_data.csv"))
    }
    ## -------------------------------------------------------------------------
    # 2) EXPORT CONSTITUENTS
    ## -------------------------------------------------------------------------
    get_constituents(obj, ticker = "all") %>%
        purrr::iwalk( ~ {
            if (!is.null(.x)) {
                futile.logger::flog.info(glue::glue("exporting {.y} constituents to csv, output_folder = {output_folder}"))
                readr::write_csv(.x, path = glue::glue("{output_folder}/{.y}_{get_region(obj)}_constituents.csv"))
            }
        })
} # to_csv
