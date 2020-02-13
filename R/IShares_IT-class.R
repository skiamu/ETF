
#' IShares_IT class constructor
#'
#' IShares_IT class constructor
#'
#' @inheritParams IShares
IShares_IT <- function(summary_link,
                       get_constituents = FALSE,
                       download_constituents_csv = FALSE,
                       melted_constituents_list = list(),
                       constituents_list = list(),
                       ...,
                       class = character()) {
    ## -------------------------------------------------------------------------
    # 1) CALL SUPERCONSTRUCTOR
    ## -------------------------------------------------------------------------
    obj <- new_IShares(
        summary_link = summary_link,
        melted_constituents_list = melted_constituents_list,
        constituents_list = constituents_list,
        region = "IT",
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
        obj$melted_constituents_list <- download_etf_constituents(
                summary_data = get_summary_data(obj),
                url_fixed_number = get_url_fixed_number(obj),
                download_csv = download_constituents_csv
            )
        template_classification <- classify_constituent_data(
            melted_constituents_list = obj$melted_constituents_list,
            region = "IT",
            n_template = 1
        )
        obj$constituents_list <- parse_etf_constituents(
            melted_constituents_list = obj$melted_constituents_list,
            template_classification = template_classification
        )
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
} # parse_date_col_IT

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
    ## -------------------------------------------------------------------------
    # 3) WIDEN THE DATA
    ## -------------------------------------------------------------------------
    # there coulb be whote spaces in the value column of the `melted_data` dataframe.
    # Apparently single empty white spaces are not remove by setting `trim_ws = TRUE`
    # in readr::melt_csv. The parameter `skip_empty_rows` is set to FALSE for being able
    # to use the same numbering of the csv files.
    melted_data %>%
        dplyr::filter(row > 3) %>%
        dplyr::filter(!stringr::str_detect(value, pattern = "^\\s+$")) %>% # remove whitespaces
        dplyr::left_join(col_names, by = "col") %>%
        tidyr::pivot_wider(
            id_cols = row,
            names_from = col_name,
            values_from = value
        ) %>%
        dplyr::select(-row) %>%
        dplyr::filter_all(dplyr::any_vars(!is.na(.))) %>%
        dplyr::mutate(aod = aod, idx = 1:nrow(.)) %>%
        dplyr::select(aod, idx, dplyr::everything()) %>%
        purrr::modify_if(
            is.character,
            ~ .x %>%
                stringr::str_replace("%", "") %>% # remove it from the `Ponderazione` column
                readr::parse_guess(locale = readr::locale(decimal_mark = ",",
                                                          grouping_mark = ".",
                                                          date_format = "%d/%m/%Y"))
        )
} # parse_template_1_IT
