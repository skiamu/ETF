
#' IShares_IT class constructor
#'
#' IShares_IT class constructor
#'
#' @inheritParams IShares
IShares_IT <- function(summary_link,
                       ...,
                       class = character()) {
    ## -------------------------------------------------------------------------
    # 1) CALL SUPERCONSTRUCTOR
    ## -------------------------------------------------------------------------
    obj <- new_IShares(
        summary_link = summary_link,
        ...,
        class = c(class, "IShares_IT")
    )
    ## -------------------------------------------------------------------------
    # 2) PARSE CHARACTER SUMMMARY DATA
    ## -------------------------------------------------------------------------
    obj$summary_data <- parse_summary_data_IT(obj$summary_data)
    return(obj)
} # IShares_IT

parse_summary_data_IT <- function(summary_data) {
    futile.logger::flog.info("parsing IT summary data from char")
    assertthat::assert_that(is.data.frame(chr_summary_data))
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
