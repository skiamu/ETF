#' IShares class constructor
#'
#' The \code{IShares} class is a "virtual" class in the sense that it's never
#' actually instantiated but used to give the interface to its subclasses.
#' For instace at the \code{IShares} class level we implement the getter
#' methods
#'
#' @param summary_link link where the IShares summary data (in JSON format) is downloaded from.
#'    This is the data summarizing all the ETF of the IShares provider
#' @param tickers_to_keep character vector containing the tickers to keep. One might
#' want to keep just a subset of tickers due to storage constraints
#'
#' @return a new \code{IShares} object
IShares <- function(summary_link, tickers_to_keep) {
    stopifnot(is.character(summary_link))
    new_IShares(
        summary_link = summary_link,
        tickers_to_keep = tickers_to_keep
    )
} # IShares

new_IShares <- function(summary_link,
                        tickers_to_keep,
                        ...,
                        class = character()) {
    res_parsed <- download_summary_data(summary_link = summary_link)
    summary_data <- parse_summary_data(res_parsed = res_parsed,
                                       tickers_to_keep = tickers_to_keep)
    new_ETF(
        summary_link = summary_link,
        summary_data = summary_data,
        ...,
        class = c(class, "IShares")
    )
} # new_IShares

#' Download summary data
#'
#' download the JSON summary data from the IShares website. For instance, this data
#' is rendered at the following address \link{https://www.ishares.com/uk/individual/en/products/etf-investments}
#'
#' @inheritParams httr::content
#' @inheritParams IShares
#'
#' @return the JSON data downloaded from \code{summary_link} is the format specified by
#'    \code{as}
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
#' parses the raw JSON data (usually returned as nested lists) into a clean dataframe
#' of character data. We decided to return character data to avoid data type issues
#' during the parsing. When the JSON data is downloaded using \code{as = "parsed"},
#' the parsing is done using the functions in the \link{https://tidyr.tidyverse.org/index.html} package
#'
#' @param res_parsed a list returned by the \link{httr::content} function. The list
#'     has the following attributes: \itemize{
#'     \item \code{code}
#'     \item \code{message}
#'     \item \code{status}
#'     \item \code{code}
#'     }
#'     The most important attribute is data which contains the actual IShares data.
#'     In particular, it's a list with the following attributes: \itemize{
#'        \item \code{tableData} a list with two attributes: \code{columns} and
#'           \code{data}. The first one store the column names and the second the actual data
#'        \item \code{config}.
#'     }
#' @param tickers_to_keep character vector containing the tickers to keep. One might
#' want to keep just a subset of tickers due to storage constraints
#'
#' @return a dataframe of characters. Each row containd summary data for one etf
parse_summary_data <- function(res_parsed, tickers_to_keep) {
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
    ## -------------------------------------------------------------------------
    # 3) SUMMARY DATA SUBSET
    ## -------------------------------------------------------------------------
    # since the column `localExchangeTicker` will be used extensively in the code
    # we want to make sure it doesn't contain white spaces
    if ("localExchangeTicker" %in% colnames(out)) {
        out$localExchangeTicker <- out$localExchangeTicker %>%
            stringr::str_trim()
    } else {
        futile.logger::flog.warn("the column `localExchangeTicker` doesn't exist in summary data!")
        stop("the column `localExchangeTicker` doesn't exist in summary data!")
    }
    if (!purrr::is_empty(tickers_to_keep)) {
        out <- out %>% dplyr::filter(localExchangeTicker %in% tickers_to_keep)
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
#' downloads from the IShares website the data regarding etf constituents. Constituent data
#' is publish in each etf web page as a csv file and we read it directly from there.
#' In \code{summary_data} there's the link pointing to the page where the csv is
#' published
#'
#' @param summary_data summary data dataframe as returned by \link{parse_summary_data}
#' @param download_csv if \code{TRUE} the constituents csv files will be saved in
#'    a local folder called `csv_files`
#' @param url_fixed_number a country-specific code found in the url
#' @param region IShares geographical region (e.g. US)
#'
#' @return a named-list. Names are the \code{localExchangeTicker} found in \code{summary_data}
#'    and the elements are the csv file found in each ETF web page in melted format.
#'    By melted format we mean as output of the function \link{readr::melt_csv}
download_etf_constituents <- function(summary_data, download_csv = TRUE, url_fixed_number, region) {
    # futile.logger::flog.info("downloading US ETF constituents")
    assertthat::assert_that(is.data.frame(summary_data))
    assertthat::assert_that(is.logical(download_csv))
    ## -------------------------------------------------------------------------
    # 1) CREATE THE URL
    ## -------------------------------------------------------------------------
    # for an example go to a single etf IShares web page and copy the address of the
    # csv file in the constituents section
    summary_data <- summary_data %>%
        dplyr::mutate(
            effective_productPageUrl = glue::glue("https://www.ishares.com{productPageUrl}/{url_fixed_number}.ajax?fileType=csv&fileName={localExchangeTicker}_holdings&dataType=fund")
        )
    ## -------------------------------------------------------------------------
    # 2) DOWNLOAD THE CSV
    ## -------------------------------------------------------------------------
    if (download_csv) {
        dir.create("./csv_files", showWarnings = FALSE)
        summary_data[["effective_productPageUrl"]] %>%
            purrr::iwalk( ~ {
                ticker <- summary_data[["localExchangeTicker"]][[.y]]
                if (!is.null(ticker) & !is.na(ticker)) {
                    futile.logger::flog.info(glue::glue("downloading constituents for {ticker} in csv format"))
                    try({
                        download.file(
                            url = .x,
                            destfile = glue::glue("./csv_files/{ticker}_{region}.csv")
                        )
                    })
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

#' Classify constituent data
#'
#' The constituent data is downloaded in a melted format so as not to incurr in
#' reading errors. After that we have to understand the structure of the csv files.
#' For this reason we assume there exsist fix templates. This function assigns
#' templates to constituent data: for instance, if a dataframe is in template_1
#' format than \code{classify_constituent_data} will assign to it the string
#' \code{parse_template_1}, which is the function used for parsing the dataframe
#'
#' @param melted_constituents_list list of dataframes with the constituent csv file
#'    in melted format downloaded using the \code{readr::melt_csv} function
#' @param n_template number of template data model availabe
#' @inheritParams download_etf_constituents
#'
#' @return a named-list with the names of the parsing functions for each constituent
#'    dataframe. If no template matches the data \code{NULL} is returned instead
classify_constituent_data <- function(melted_constituents_list, region = "US", n_template = 3) {
    assertthat::assert_that(is.list(melted_constituents_list))
    assertthat::assert_that(is.character(region))
    assertthat::assert_that(is.numeric(n_template))
    futile.logger::flog.info(glue::glue("classifing {region} ETF constituents using {n_template} template"))
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

#' Parse constituent data
#'
#' Call the parsing function for each melted constituents dataframe. The tickers included
#' in the output are the names of the input list \code{melted_constituents_list}.
#' That is, all those ETF for which the LocalExchangeTicker is different from
#' \code{NA} in \code{summary_data}.
#'
#' @param template_classification named-list returned by the function \link{classify_constituent_data}
#'    with the names of the function used to parse each melted dataframe
#' @inheritParams classify_constituent_data
#'
#' @return a list of dataframes. The dataframes are in a wide format and as parsed as
#'    possible. List names are local tickers
parse_etf_constituents <- function(melted_constituents_list,
                                   template_classification) {
    assertthat::assert_that(all(
        is.list(melted_constituents_list),
        is.list(template_classification)
    ))
    ## -------------------------------------------------------------------------
    # 1) CALL ETF PARSERS
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
            })
        } else {
            futile.logger::flog.info(glue::glue("not parsing {names(melted_constituents_list)[[i]]} because it's NULL"))
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

#' Parse colum names
#'
#' Most of the column names contain special characters or spaces. This function
#' gives them an uniform formatting by removing spaces and special characters
#'
#' @param df a datframe
#'
#' @return a dataframe with parsed column names
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

#' Convert data in mongo format
#'
#' The data is saved in mongo using the aod and ticker as keys and the data is
#' a list where the first field is the row from \code{summary_data} and the second the
#' \code{constituents} datafarme
#'
#' @param obj a \code{IShares} object or one of its subclasses
#'
#' @return a named list. Elements are list with the following fields
#'    \itemize{
#'       \item \code{aod} nav as of date
#'       \item \code{ticker} local exchange ticker plus region
#'       \item \code{data} a list with summary and constiruents data
#'    }
#'
#' @export
to_mongo_data_format <- function(obj) {
    UseMethod("to_mongo_data_format")
} # to_mongo_data_format

#' @export
to_mongo_data_format.IShares <- function(obj) {
    tickers <- names(get_constituents(obj, ticker = "all"))
    constituents_list <- get_constituents(obj)
    summary_data <- get_summary_data(obj) %>% dplyr::filter(localExchangeTicker %in% tickers)
    out <- vector("list", length(tickers))
    for (i in seq_along(tickers)) {
        futile.logger::flog.info(glue::glue("converting to mongo format {tickers[[i]]}"))
        summary_data_slice <- summary_data %>% dplyr::filter(localExchangeTicker == tickers[[i]])
        assertthat::assert_that(nrow(summary_data_slice) == 1)
        ## ---------------------------------------------------------------------
        ## 1) GET AOD
        ## ---------------------------------------------------------------------
        if ("navAmountAsOf" %in% colnames(summary_data_slice)) {
            aod_nav <- summary_data_slice[["navAmountAsOf"]]
        } else {
            # futile.logger::flog.error(glue::glue("`summary_data` has no column `navAmountAsOf`!"))
            stop(glue::glue("`summary_data` has no column `navAmountAsOf`!"))
        }
        if ("aod" %in% colnames(constituents_list[[i]])) {
            aod_constituents <- unique(constituents_list[[i]][["aod"]])
        } else {
            # futile.logger::flog.error(glue::glue("constituents dataframe has no column `aod`!"))
            stop(glue::glue("constituents dataframe has no column `aod`!"))
        }
        if (aod_nav == aod_constituents) {
            aod <- aod_nav
        } else {
            # futile.logger::flog.error(glue::glue("aod_nav = {aod_nav} != aod_constituents = {aod_constituents}"))
            stop(glue::glue("aod_nav = {aod_nav} != aod_constituents = {aod_constituents}"))
        }
        if (is.null(constituents_list[[i]])) {
            futile.logger::flog.warn(glue::glue("{tickers[[i]]} has no constituents, exporting only summary data"))
        }
        ## ---------------------------------------------------------------------
        ## 2) EXPORTING TO MONGO FORMAT
        ## ---------------------------------------------------------------------
        out[[i]] <- list(
            aod = aod ,
            ticker = glue::glue("{tickers[[i]]}_{get_region(obj)}"),
            data = list(summary_data = summary_data_slice,
                        constituents = constituents_list[[i]])
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

#' @export
get_summary_data.IShares <- function(obj) {
    assertthat::has_name(obj, "summary_data")
    obj[["summary_data"]]
}

#' @export
get_constituents <- function(obj, ticker) {
    UseMethod("get_constituents")
} # get_constituents

#' @export
get_constituents.IShares <- function(obj, ticker = "all") {
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

get_melted_constituents_list.IShares <- function(obj, ticker = "all") {
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
get_constituents_aod <- function(obj) {
    UseMethod("get_constituents_aod")
} # get_constituents_aod

get_constituents_aod.IShares <- function(obj) {
    assertthat::has_name(obj, "constituents_list")
    obj$constituents_list %>%
        purrr::imap( ~ unique(.x[["aod"]]))
} # get_constituents_aod.IShares

#' @export
get_nav_aod <- function(obj) {
    UseMethod("get_nav_aod")
} # get_nav_aod

get_nav_aod.IShares <- function(obj) {
    assertthat::has_name(obj, "summary_data")
    assertthat::assert_that("navAmountAsOf" %in% colnames(obj$summary_data))
    out <- unique(obj$summary_data$navAmountAsOf)
    assertthat::assert_that(length(out) == 1)
    return(out)
} # get_nav_aod.IShares

#' @export
to_csv <- function(obj, output_folder) {
    UseMethod("to_csv")
} # to_csv

#' Export to csv format
#'
#' \code{to_csv} export to csv format the dataframe \code{summary_data} and each
#' dataframe in \code{constituents_list}. The name of constituents files is the
#' ticker plus the region (e.g. IVV_US.csv)
#'
#' @param obj \code{IShares} object or one of its subclasses
#' @param output_folder folder where the csv files are to be saved
#'
#' @export
to_csv.IShares <- function(obj, output_folder) {
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
