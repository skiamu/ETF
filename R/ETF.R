new_ETF <- function(summary_link,
                    ...,
                    class = character()) {
    structure(list(summary_link = summary_link,
                   ...),
              class = c(class, "ETF"))
} # new_ETF

#' ETF class constructor
#'
#' ETF is the base class
#'
#' @param summary_link link where the ETF summary data is downloaded from.
#'
#' @export
ETF <- function(summary_link) {
    stopifnot(is.character(summary_link))
    new_ETF(
        summary_link = summary_link
    )
} # ETF
