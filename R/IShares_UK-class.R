#' IShares_IT class constructor
#'
#' IShares_IT class constructor
#'
#' @inheritParams IShares
IShares_UK <- function(summary_link,
                       get_constituents = FALSE,
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
        region = "UK",
        tickers_to_keep = tickers_to_keep,
        ...,
        class = c(class, "IShares_UK")
    )
    ## -------------------------------------------------------------------------
    # 2) PARSE CHARACTER SUMMMARY DATA
    ## -------------------------------------------------------------------------
    obj$summary_data <- parse_summary_data_UK(get_summary_data(obj))
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
            region = "UK",
            n_template = 1
        )
        obj$constituents_list <- parse_etf_constituents(
            melted_constituents_list = obj$melted_constituents_list,
            template_classification = template_classification
        )
    }
    return(obj)
} # IShares_IT

parse_summary_data_UK <- function(summary_data) {
    futile.logger::flog.info("parsing UK summary data from char")
    assertthat::assert_that(is.data.frame(summary_data))
    summary_data %>%
        purrr::modify_if(is.character, readr::parse_guess, locale = readr::locale("en")) %>%
        purrr::modify_at(dplyr::vars(dplyr::contains("AsOf")), parse_date_col_UK) %>%
        purrr::modify_at("inceptionDate", parse_date_col_UK)
} # parse_summary_data_UK

parse_date_col_UK <- function(vec) {
    if (all(is.character(vec))) {
        tryCatch({
            vec %>% readr::parse_date(format = "%b %d, %Y")
        },
        warning = function(w) {
            vec %>% readr::parse_date(format = "%d/%b/%Y")
        })

    } else {
        vec
    }
} # parse_date_col_US

get_url_fixed_number.IShares_UK <- function(obj) {
    as.character("1506575576011")
} # get_url_fixed_number.IShares_IT


#' @export
is_template_1_UK <- function(melted_data) {
    if (is.null(melted_data)) return(FALSE)
    assertthat::assert_that(is.data.frame(melted_data))
    cell_11 <- melted_data %>% dplyr::filter(row == 1, col == 1) %>% dplyr::pull(value)
    if (cell_11 == "Fund Holdings as of") {
        TRUE
    } else {
        FALSE
    }
} # is_template_1_UK

#' @export
parse_template_1_UK <- function(melted_data) {
    assertthat::assert_that(is.data.frame(melted_data))
    ## -------------------------------------------------------------------------
    # 1) EXTRACT AOD
    ## -------------------------------------------------------------------------
    aod <- melted_data %>%
        dplyr::filter(row == 1, col == 2) %>%
        dplyr::pull(value) %>%
        parse_date_col_UK()
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
            ~ .x %>% readr::parse_guess(locale = readr::locale("en"))
        )
} # parse_template_1_UK
