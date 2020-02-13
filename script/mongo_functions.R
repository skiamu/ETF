write_to_mongo <- function(data) {
    futile.logger::flog.info(glue::glue("exporting {data$ticker} to mongo as of {data$aod}"))
    isharesETF_Save(
        ticker = data$ticker,
        aod = data$aod,
        data = data$data
    )
} # write_to_mongo

get_from_mongo <- function(ticker, aod) {
    futile.logger::flog.info(glue::glue("reading {ticker} from mongo as of {aod}"))
    data <- isharesETF_Get(ticker = ticker, aod = aod)
    list(
        aod = data$aod,
        ticker = data$ticker,
        summary_data = tibble::as_tibble(data$data$summary_data[[1]]),
        constituents = tibble::as_tibble(data$data$constituents[[1]])
    )
} # get_from_mongo
