
#' IShares_IT class constructor
#'
#' IShares_IT class constructor
#'
#' @inheritParams IShares
IShares_IT <- function(summary_link,
                       ...,
                       class = character()) {
    new_IShares(
        summary_link = summary_link,
        ...,
        class = c(class, "IShares_IT")
    )
} # IShares_IT
