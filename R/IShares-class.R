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
                tidyr::unnest_wider(etf_data_list) %>%
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
