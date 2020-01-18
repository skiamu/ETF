
#' IShares_US class constructor
#'
#' IShares_US class constructor
#'
#' @inheritParams IShares
IShares_US <- function(summary_link,
                       ...,
                       class = character()) {
    new_IShares(
        summary_link = summary_link,
        ...,
        class = c(class, "IShares_US")
    )
} # IShares_US


