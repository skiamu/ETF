
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
        constituents_list <- download_etf_constituents_US(summary_data = obj$summary_data,
                                                          url_fixed_number = get_url_fixed_number(obj))
        template_classification <- classify_constituent_data(constituents_list)
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
