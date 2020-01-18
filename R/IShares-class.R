new_IShares <- function(summary_link,
                        ...,
                        class = character()) {
    new_ETF(
        summary_link = summary_link,
        ...,
        class = c(class, "IShares")
    )
} # new_IShares

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

#' @export
download_summary_data <- function(obj, as = "text") {
    UseMethod("download_summary_data")
} # download_summary_data

#' Download IShares summary
#'
#' Downloads the IShares summary data from the IShares web site
#'
#' @param \code{IShares} object
#' @inheritParams httr::content
#'
#' @return a list with the following fields
#'    \itemize{
#'       \item \code{columns} column names
#'       \item \code{data} the data
#'    }
#' @export
download_summary_data.IShares <- function(obj, as = "text") {
    futile.logger::flog.info(
        glue::glue("Downloading IShares summary data from {obj$summary_link}")
    )
    res <- httr::GET(url = obj$summary_link)
    res_parsed <- httr::content(res, as = as)
    return(res_parsed)
} # download_summary_data.IShares

df <- tibble::tibble(a = res_parsed$data$tableData$columns) %>%
    tidyr::unnest_wider(a)

parse_summary_data_list <- function(res_parsed) {
    assertthat::assert_that(is.list(res_parsed))
    ## -------------------------------------------------------------------------
    # 1) COLNAMES PARSING
    ## -------------------------------------------------------------------------
    columns_df <- tibble::tibble(a = res_parsed$data$tableData$columns) %>%
        tidyr::unnest_wider(a) %>%
        dplyr::mutate(col_id = 1:nrow(.)) %>%
        dplyr::select(col_id, col_name = name)

    q <- res_parsed$data$tableData$data %>%
        purrr::map_dfr( ~ {
            column_data_df <- tibble::tibble(etf_data_list = .x) %>%
                dplyr::mutate(col_id = 1:nrow(.))
            column_data_df_num <- column_data_df %>%
                dplyr::filter(purrr::map_lgl(etf_data_list, is.list)) %>%
                tidyr::unnest_wider(etf_data_list) %>%
                dplyr::select(col_id, value = d)
            column_data_df_chr <- column_data_df %>%
                dplyr::filter(!purrr::map_lgl(etf_data_list, is.list)) %>%
                dplyr::mutate(etf_data_list = purrr::map_chr(etf_data_list, ~parse_chr_data(unlist(.x)))) %>%
                dplyr::select(col_id, value = etf_data_list)
            list(column_data_df_chr, column_data_df_num) %>%
                purrr::map_dfc( ~ {
                    .x %>%
                        dplyr::left_join(
                            columns_df,
                            by = "col_id"
                        ) %>%
                        dplyr::select(-col_id) %>%
                        tidyr::pivot_wider(names_from = col_name, values_from = value)
                })
        })


    column_data_df <- tibble::tibble(etf_data_list = res_parsed$data$tableData$data) %>%
        dplyr::slice(1) %>%
        tidyr::unnest(etf_data_list) %>%
        dplyr::mutate(col_id = 1:nrow(.),
                      row_id = 1)
    column_data_df_num <- column_data_df %>%
        dplyr::filter(purrr::map_lgl(etf_data_list, is.list)) %>%
        tidyr::unnest_wider(etf_data_list) %>%
        dplyr::select(col_id, value = r)
    column_data_df_chr <- column_data_df %>%
        dplyr::filter(!purrr::map_lgl(etf_data_list, is.list)) %>%
        dplyr::mutate(etf_data_list = purrr::map_chr(etf_data_list, ~parse_chr_data(unlist(.x)))) %>%
        dplyr::select(col_id, value = etf_data_list)
    column_data_df_num %>%
        dplyr::left_join(
            columns_df,
            by = "col_id"
        ) %>%
        # dplyr::mutate(row_id = 1) %>%
        dplyr::select(-col_id) %>%
        tidyr::pivot_wider(names_from = col_name, values_from = value)


} # parse_summary_data_list

parse_chr_data <- function(s) {
    dplyr::case_when(
        s == "-" ~ NA_character_,
        TRUE ~ as.character(s)
    )
} # parse_chr_data

# parse_summary_data <- function(json_obj) {
#     stopifnot(is.character(json_obj))
#     columns_data_json_obj <- json_obj %>%
#         tidyjson::json_types() %>%
#         tidyjson::gather_object(column.name = "name1") %>%
#         dplyr::filter(name1 == "data") %>%
#         tidyjson::gather_object(column.name = "name2") %>%
#         dplyr::filter(name2 == "tableData") %>%
#         tidyjson::gather_object(column.name = "name3")
#     columns <- columns_data_json_obj %>%
#         dplyr::filter(name3 == "columns") %>%
#         tidyjson::gather_array() %>%
#         tidyjson::spread_all() %>%
#         dplyr::as_tibble() %>%
#         dplyr::select(hidden, mobile, col_name = name) %>%
#         dplyr::mutate(column_index = 1:nrow(.))
#     data <- columns_data_json_obj %>%
#         dplyr::filter(name3 == "data") %>%
#         tidyjson::gather_array(column.name = "etf_index")
#     df <- 1:nrow(data) %>%
#         purrr::map_dfr( ~ {
#             tmp <- data %>%
#                 dplyr::filter(etf_index == .x) %>%
#                 tidyjson::gather_array(column.name = "column_index") %>%
#                 tidyjson::json_types()
#             data_string <- tmp %>%
#                 dplyr::filter(type == "string") %>%
#                 tidyjson::append_values_string() %>%
#                 dplyr::as_tibble() %>%
#                 dplyr::select(column_index, value = string)
#             data_number <- tmp %>%
#                 dplyr::filter(type == "number") %>%
#                 tidyjson::append_values_number() %>%
#                 dplyr::as_tibble() %>%
#                 dplyr::select(column_index, value = number)
#             data_object <- tmp %>%
#                 dplyr::filter(type == "object") %>%
#                 tidyjson::spread_all() %>%
#                 dplyr::as_tibble() %>%
#                 dplyr::select(column_index, value = r)
#             list(data_string, data_number, data_object) %>%
#                 purrr::map_dfc(compose_row, columns_df = columns, etf_index = .x)
#         })
#     df <- dplyr::tibble()
#     for (i in 1:nrow(data)) {
#         print(i)
#         tmp <- data %>%
#             dplyr::filter(etf_index == i) %>%
#             tidyjson::gather_array(column.name = "column_index") %>%
#             tidyjson::json_types()
#         data_string <- tmp %>%
#             dplyr::filter(type == "string") %>%
#             tidyjson::append_values_string() %>%
#             dplyr::as_tibble() %>%
#             dplyr::select(column_index, value = string)
#         data_number <- tmp %>%
#             dplyr::filter(type == "number") %>%
#             tidyjson::append_values_number() %>%
#             dplyr::as_tibble() %>%
#             dplyr::select(column_index, value = number)
#         data_object <- tmp %>%
#             dplyr::filter(type == "object") %>%
#             tidyjson::spread_all() %>%
#             dplyr::as_tibble() %>%
#             dplyr::select(column_index, value = r)
#         df <- dplyr::bind_rows(
#           df,
#           list(data_string, data_number, data_object) %>%
#               purrr::map_dfc(compose_row, columns_df = columns, etf_index = i)
#         )
#     }
#
#         dplyr::filter(array.index == 1) %>%
#         tidyjson::gather_array() %>%
#         tidyjson::json_types()
#     data %>%
#         dplyr::filter(type == "object") %>%
#         tidyjson::spread_all()
# } #parse_summary_data2

compose_row <- function(long_df, columns_df, etf_index) {
    long_df %>%
        dplyr::left_join(
            columns_df,
            by = "column_index"
        ) %>%
        dplyr::mutate(etf_index = etf_index) %>%
        tidyr::pivot_wider(etf_index, names_from = col_name, values_from = value)
} # compose_row

#' Parse column names
#'
#' This function parse the IShares column names returned in the \code{res$tableData$columns}
#' list
#'
#' @param colnames_list a list whose elements are lists with the following fields
#'    \itemize{
#'       \item \code{hidden}: logical
#'       \item \code{mobile}: logical
#'       \item \code{name}: column name (string)
#'    }
#'
#' @return a character vector
#'
#' @export
IShares_parse_summary_colnames <- function(colnames_list) {
    colnames_list %>%
        purrr::map_chr(~purrr::pluck(.x, "name"))
} # IShares_parse_summary_colnames
