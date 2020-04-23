#' Write to Mongodb
#'
#' @param data a element of the list returned by \link{to_mongo_data_format}. The attributes
#' are: \itemize{
#'    \item \code{aod} nav as of date
#'    \item \code{ticker} local exchange ticker plus region
#'    \item \code{data} a list with summary and constiruents data
#' }
#'
#' @export
write_IShares_to_mongo <- function(data) {
    futile.logger::flog.info(glue::glue("exporting {data$ticker} to mongo as of {data$aod}"))
    isharesETF_Save(
        ticker = data$ticker,
        aod = data$aod,
        data = data$data
    )
} # write_IShares_to_mongo

#' Read from mongo
#'
#' @param ticker etf ticker (local exchange ticker plus country code)
#' @param aod etf as-of-date
#'
#' @return a list with the following attributes:
#' \itemize{
#'    \item \code{aod}
#'    \item \code{ticker}
#'    \item \code{summary_data}
#'    \item \code{constituents}
#' }
get_from_mongo <- function(ticker, aod) {
    futile.logger::flog.info(glue::glue("reading {ticker} from mongo as of {aod}"))
    data <- isharesETF_Get(ticker = ticker, aod = aod)
    if (!is.data.frame(data)) {
        futile.logger::flog.info(glue::glue("object return is not a dataframe {ticker}, {aod}"))
        NULL
    } else {
        list(
            aod = data$aod,
            ticker = data$ticker,
            summary_data = tibble::as_tibble(data$data$summary_data[[1]]),
            constituents = tibble::as_tibble(data$data$constituents[[1]])
        )
    }
} # get_from_mongo


parse_output_from_mongo <- function(res) {
    res <- res %>% purrr::compact()


    res %>%
        purrr::reduce(~ {
            list(
                aod = c(from_int_to_date(.x$aod), from_int_to_date(.x$aod)),
                ticker = c(.x$ticker, .x$ticker),
                summary_data = list(),

            )
        })

    aod <- res %>%
        purrr::map_chr(~ as.character(.x$date))
    aod <- purrr::map_chr(res, ~ as.Date(as.character(.x), "%Y%m%d"))
} # parse_output_from_mongo

