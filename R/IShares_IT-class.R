
#' IShares_IT class constructor
#'
#' IShares_IT class constructor
#'
#' @inheritParams IShares
IShares_IT <- function(summary_link,
                       get_constituents = FALSE,
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
        class = c(class, "IShares_IT")
    )
    ## -------------------------------------------------------------------------
    # 2) PARSE CHARACTER SUMMMARY DATA
    ## -------------------------------------------------------------------------
    obj$summary_data <- parse_summary_data_IT(obj$summary_data)
    ## -------------------------------------------------------------------------
    # 3) GET ETF CONSTITUENTS
    ## -------------------------------------------------------------------------
    if (get_constituents) {
        constituents_list <- download_etf_constituents(
                summary_data = get_summary_data(obj),
                url_fixed_number = get_url_fixed_number(obj),
                download_csv = FALSE
            )
        template_classification <- classify_constituent_data(
            constituents_list = constituents_list,
            region = "IT",
            n_template = 1
        )
        obj$constituents_list <- parse_etf_constituents(constituents_list,
                                                        template_classification)
    }
    return(obj)
} # IShares_IT

parse_summary_data_IT <- function(summary_data) {
    futile.logger::flog.info("parsing IT summary data from char")
    assertthat::assert_that(is.data.frame(summary_data))
    summary_data %>%
        purrr::modify_if(is.character, readr::parse_guess, locale = readr::locale(decimal_mark = ",",
                                                                 grouping_mark = ".",
                                                                 date_format = "%d/%m/%Y")) %>%
        purrr::modify_at(dplyr::vars(dplyr::contains("AsOf")), parse_date_col_IT) %>%
        purrr::modify_at("inceptionDate", parse_date_col_IT)
} # parse_summary_data_IT

parse_date_col_IT <- function(vec) {
    if (all(is.character(vec))) {
        vec %>% readr::parse_date(format = "%d %b %Y", locale = readr::locale("it"))
    } else {
        vec
    }
}

get_url_fixed_number <- function(obj) {
    UseMethod("get_url_fixed_number")
} # get_url_fixed_number

get_url_fixed_number.IShares_IT <- function(obj) {
    as.character("1506575546154")
} # get_url_fixed_number.IShares_IT

#' @export
is_template_1_IT <- function(melted_data) {
    if (is.null(melted_data)) return(FALSE)
    assertthat::assert_that(is.data.frame(melted_data))
    cell_11 <- melted_data %>% dplyr::filter(row == 1, col == 1) %>% dplyr::pull(value)
    if (cell_11 == "Al") {
        TRUE
    } else {
        FALSE
    }
} # is_template_1_IT

#' @export
parse_template_1_IT <- function(melted_data) {
    assertthat::assert_that(is.data.frame(melted_data))
    ## -------------------------------------------------------------------------
    # 1) EXTRACT AOD
    ## -------------------------------------------------------------------------
    aod <- melted_data %>%
        dplyr::filter(row == 1, col == 2) %>%
        dplyr::pull(value) %>%
        as.Date(format = "%d/%m/%Y")
    ## -------------------------------------------------------------------------
    # 2) COL NAMES
    ## -------------------------------------------------------------------------
    col_names <- melted_data %>%
        dplyr::filter(row == 3) %>%
        dplyr::select(col, col_name = value) %>%
        dplyr::distinct(col_name, .keep_all = TRUE)
    melted_data %>%
        dplyr::filter(row > 3) %>%
        dplyr::left_join(col_names, by = "col") %>%
        tidyr::pivot_wider(
            id_cols = row,
            names_from = col_name,
            values_from = value
        ) %>%
        dplyr::mutate(aod = aod) %>%
        purrr::modify_if(
            is.character,
            ~ .x %>%
                stringr::str_replace("%", "") %>%
                readr::parse_guess(locale = readr::locale(decimal_mark = ",",
                                                            grouping_mark = ".",
                                                            date_format = "%d/%m/%Y"))
        )
} # parse_template_1_IT
