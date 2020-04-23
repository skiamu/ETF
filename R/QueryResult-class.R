## -----------------------------------------------------------------------------
# ============================== QueryResult class =============================
## -----------------------------------------------------------------------------
QueryResult <- function(result = list(), ..., class = character()) {
    structure(
        append(
            x = result,
            values = list(...)
        ),
        class = c(class, "QueryResult")
    )
} # new_QueryResult

QueryResult_region <- function(result = list(), ..., class = character()) {
    assertthat::assert_that(is.list(result))
    region <- stringr::str_split(string = result$ticker, pattern = "_", simplify = TRUE)[1,2]
    QueryResult(
        result = result,
        region = region,
        ...,
        class = c(class, stringr::str_c("QueryResult", region, sep = "_"))
    )
} # QueryResult_region

## -----------------------------------------------------------------------------
# SCALE WEIGHT METHOD
## -----------------------------------------------------------------------------
#' Scale weights
#'
#' The instrument portfolio weights are recomputed as the ones given are rounded
#' up to the second digits
#'
#' @param  obj \code{QueryResult} object
scale_weight <- function(obj) {
    UseMethod("scale_weight")
} # scale_weight


scale_weight.QueryResult_IT <- function(obj) {
    if (c("valore_nozionale") %in% colnames(obj$constituents)) {
        obj$constituents <- obj$constituents %>%
            dplyr::mutate(weight_scaled = valore_nozionale / sum(valore_nozionale, na.rm = TRUE) * 100)
    } else {
        futile.logger::flog.info(glue::glue("column `valore_nozionale` is not available for {obj$ticker} at {obj$aod} --> `weight_scaled` is not computed"))
    }
    return(obj)
} # scale_weight.QueryResult_IT

scale_weight.QueryResult_UK <- function(obj) {
    if (c("notional_value") %in% colnames(obj$constituents)) {
        obj$constituents <- obj$constituents %>%
            dplyr::mutate(weight_scaled = notional_value / sum(notional_value, na.rm = TRUE) * 100)
    } else {
        futile.logger::flog.info(glue::glue("column `valore_nozionale` is not available for {obj$ticker} at {obj$aod} --> `weight_scaled` is not computed"))
    }
    return(obj)
} # scale_weight.QueryResult_UK

scale_weight.QueryResult_US <- scale_weight.QueryResult_UK
# scale_weight.QueryResult_US

## -----------------------------------------------------------------------------
# PARSE COUNTRY METHOD
## -----------------------------------------------------------------------------
parse_country <- function(obj) {
    UseMethod("parse_country")
} # parse_country

parse_country.QueryResult_IT <- function(obj) {
    if ("area_geografica" %in% colnames(obj$constituents)) {
        obj$constituents <- obj$constituents %>%
            manual_constituents_parsing() %>%
            dplyr::mutate(
                closest_country_it_code = stringdist::amatch(
                    area_geografica,
                    country_mapping$country_name_ita,
                    maxDist = Inf,
                    method = "jw"
                )
            ) %>%
            dplyr::left_join(country_mapping, by = c("closest_country_it_code"="idx")) %>%
            dplyr::left_join(gpa_country_mapping, by = c("alpha_3_code"="iso3"))
    } else {
        futile.logger::flog.info(glue::glue("column `area_geografica` is not available for {obj$ticker} at {obj$aod} --> country-name parsing is not computed"))
    }
    return(obj)
} # parse_country.QueryResult_IT

manual_constituents_parsing <- function(constituents_df) {
    if ("area_geografica" %in% colnames(constituents_df)) {
        constituents_df %>%
            dplyr::mutate(area_geografica = dplyr::case_when(
                area_geografica == "Corea" ~ "Corea del Sud",
                TRUE ~ area_geografica
            ))
    }
} # manual_constituents_parsing

parse_country.QueryResult_UK <- function(obj) {
    if ("location" %in% colnames(obj$constituents)) {
        obj$constituents <- obj$constituents %>%
            dplyr::mutate(
                closest_country_eng_code = stringdist::amatch(
                    location,
                    country_mapping$country_name_eng,
                    maxDist = Inf,
                    method = "jw"
                )
            ) %>%
            dplyr::left_join(country_mapping, by = c("closest_country_eng_code"="idx")) %>%
            dplyr::left_join(gpa_country_mapping, by = c("alpha_3_code"="iso3"))
    } else {
        futile.logger::flog.info(glue::glue("column `area_geografica` is not available for {obj$ticker} at {obj$aod} --> country-name parsing is not computed"))
    }
    return(obj)
} # parse_country.QueryResult_UK

parse_country.QueryResult_US <- function(obj) {
    stop("Non è stato implementato il metodo `parse_country` per la classe `QueryResult_US`")
} # parse_country.QueryResult_US

## -----------------------------------------------------------------------------
# PARSE SECTOR METHOD
## -----------------------------------------------------------------------------
parse_sector <- function(obj) {
    UseMethod("parse_sector")
} # parse_sector

parse_sector.QueryResult_IT <- function(obj) {
    if ("settore" %in% colnames(obj$constituents)) {
        obj$constituents <- obj$constituents %>%
            dplyr::mutate(idL3 = dplyr::case_when(
                settore == "Salute" ~ 31,
                settore == "IT" ~ 26,
                settore == "Consumi Discrezionali" ~ 21,
                settore == "Comunicazione" ~ 28,
                settore == "Industriali" ~ 16, # there could be other matches
                settore == "Finanziari" ~ 12,
                TRUE ~ NA_real_
            )) %>%
            dplyr::left_join(gpa_sector_mapping, by = "idL3")
    } else {
        futile.logger::flog.info(glue::glue("column `settore` is not available for {obj$ticker} at {obj$aod} --> sector parsing is not computed"))
    }
    return(obj)
} # parse_sector.QueryResult_IT

parse_sector.QueryResult_UK <- function(obj) {
    if ("sector" %in% colnames(obj$constituents)) {
        obj$constituents <- obj$constituents %>%
            dplyr::mutate(idL3 = dplyr::case_when(
                sector == "Information Technology" ~ 26,
                sector == "Consumer Discretionary" ~ 21,
                sector == "Industrials" ~ 16, # there could be other matches
                TRUE ~ NA_real_
            )) %>%
            dplyr::left_join(gpa_sector_mapping, by = "idL3")
    } else {
        futile.logger::flog.info(glue::glue("column `settore` is not available for {obj$ticker} at {obj$aod} --> sector parsing is not computed"))
    }
    return(obj)
} # parse_sector.QueryResult_UK

## -----------------------------------------------------------------------------
# GET NRC INVENTORY
## -----------------------------------------------------------------------------

compute_NRC_inventory <- function(obj) {
    UseMethod("compute_NRC_inventory")
} # compute_NRC_inventory

compute_NRC_inventory.QueryResult_IT <- function(obj) {
    ## -------------------------------------------------------------------------
    # 1) CHECK FOR MUST-HAVE COLUMNS
    ## -------------------------------------------------------------------------
    if (!all(c("asset_class") %in% colnames(obj$constituents))) { #must-have column names
        futile.logger::flog.info(
            glue::glue(
                "column `asset_class` is not available for {obj$ticker} at {obj$aod} --> NRC inventory data not added"
            )
        )
        return(NULL)
    }
    ## -------------------------------------------------------------------------
    # 2) CHECK FOR NICE-TO-HAVE COLUMNS
    ## -------------------------------------------------------------------------
    variable_names <- c("valuta_di_mercato", "nome", "isin", "weight_scaled", "ticker_dellemittente",
                        "nameL2", "country", "block", "nominale")
    if (!all(variable_names %in% colnames(obj$constituents))) {
        missing_variable_names <- dplyr::setdiff(variable_names, colnames(obj$constituents))
        obj$constituents[ , missing_variable_names] <- NA
    }
    ## -------------------------------------------------------------------------
    # 3) CREATE NRC INVENTORY
    ## -------------------------------------------------------------------------
    common_cols <- tibble::tibble(
        # codeL1Gpa = get_ticker(obj),
        # codeL1 = get_ticker(obj),
        # code = get_ticker(obj),
        # instNum = NA_real_,
        # fundOrBmk = "Fund"
    )
    obj$NRC_inventory <- obj$constituents %>% # at this point I'm sure these variables exist
        dplyr::mutate_at(dplyr::vars(weight_scaled), ~ .x / 100) %>%
        dplyr::mutate(nav = sum(valore_di_mercato, na.rm = TRUE)) %>%
        purrr::pmap_dfr(function(asset_class,
                                 valuta_di_mercato,
                                 nome,
                                 isin,
                                 weight_scaled,
                                 ticker_dellemittente,
                                 nameL2,
                                 country,
                                 block,
                                 nominale,
                                 valore_di_mercato,
                                 nav,
                                 valore_nozionale,
                                 ...) {
            df <- switch(
                asset_class,
                Azionario = {
                    tibble::tibble(
                        instType = "Equity",
                        instTypeL1 = "Equity",
                        instTypeL2 = "Pflio",
                        instTypeL3 = "Shares",
                        description = nome,
                        isin = isin,
                        weight = weight_scaled,
                        factorType = c("Equity", "Currency"),
                        factor = c(ticker_dellemittente, valuta_di_mercato),
                        factorSector = c(nameL2, NA_character_),
                        factorCountry = country,
                        factorCountryBlock = block,
                        factorIrDurCcy = valuta_di_mercato,
                        factorWeight = 1,
                        factorWeightCtr = weight,
                        is_processed = TRUE
                    )
                },
                FX = {
                    tibble::tibble(
                        instType = "FxFwd",
                        instTypeL1 = "Cash",
                        instTypeL2 = "Deriv",
                        instTypeL3 = "FxFwd",
                        description = nome,
                        fx = get_fx_rate(local_currency = ticker_dellemittente, base_currency = "USD", aod = get_aod(obj, "Date")),
                        weight = nominale  * fx / nav * sign(valore_nozionale), # exposure,,
                        factorType = c("Currency"),
                        factor = ticker_dellemittente,
                        factorCountry = country,
                        factorCountryBlock = block,
                        factorIrDurCcy = ticker_dellemittente,
                        factorWeight = 1,
                        factorWeightCtr = weight * factorWeight,
                        is_processed = TRUE
                    )
                },
                Contanti = {
                    tibble::tibble(
                        instType = "Cash",
                        instTypeL1 = "Cash",
                        instTypeL2 = "Pflio",
                        description = valuta_di_mercato,
                        weight = weight_scaled,
                        factorType = "Currency",
                        factor = valuta_di_mercato,
                        factorCountry = country,
                        factorCountryBlock = block,
                        factorIrDurCcy = valuta_di_mercato,
                        factorWeight = 1,
                        factorWeightCtr = weight_scaled,
                        is_processed = TRUE
                    )
                },
                Futures = {
                    tibble::tibble(
                        instType = "Future",
                        instTypeL1 = "Equity",
                        instTypeL2 = "Deriv",
                        instTypeL3 = "Future",
                        weight = weight_scaled, # it's already the exposure, check this
                        factorType = "Equity",
                        factor = ticker_dellemittente,
                        factorCountry = country,
                        factorCountryBlock = block,
                        factorIrDurCcy = valuta_di_mercato,
                        factorWeight = 1,
                        factorWeightCtr = weight * factorWeight,
                        is_processed = TRUE
                    )
                },
                `Cash Collateral and Margins` = {
                    tibble::tibble(
                        instType = nome,
                        instTypeL1 = "Cash",
                        instTypeL2 = "Pflio",
                        instTypeL3 = "Margin/Otc",
                        description = valuta_di_mercato,
                        weight = weight_scaled,
                        factorType = "Currency",
                        factor = valuta_di_mercato,
                        factorCountry = country,
                        factorCountryBlock = block,
                        factorIrDurCcy = valuta_di_mercato,
                        factorWeight = 1,
                        factorWeightCtr = weight_scaled,
                        is_processed = TRUE
                    )
                },
                {
                    tibble::tibble(
                        instType = asset_class,
                        is_processed = FALSE
                    )
                }
            )
        })
    # %>% dplyr::bind_cols(tidyr::uncount(common_cols, nrow(.)), .)
    return(obj)
} # compute_NRC_inventory.QueryResult_IT


compute_NRC_inventory.QueryResult_UK <- function(obj) {
    ## -------------------------------------------------------------------------
    # 1) CHECK FOR MUST-HAVE COLUMNS
    ## -------------------------------------------------------------------------
    if (!all(c("asset_class") %in% colnames(obj$constituents))) { #must-have column names
        futile.logger::flog.info(
            glue::glue(
                "column `asset_class` is not available for {obj$ticker} at {obj$aod} --> NRC inventory data not added"
            )
        )
        return(NULL)
    }
    ## -------------------------------------------------------------------------
    # 2) CHECK FOR NICE-TO-HAVE COLUMNS
    ## -------------------------------------------------------------------------
    variable_names <- c("market_currency", "name", "isin", "weight_scaled", "issuer_ticker",
                        "nameL2", "country", "block")
    if (!all(variable_names %in% colnames(obj$constituents))) {
        missing_variable_names <- dplyr::setdiff(variable_names, colnames(obj$constituents))
        obj$constituents[ , missing_variable_names] <- NA
    }
    ## -------------------------------------------------------------------------
    # 3) CREATE NRC INVENTORY
    ## -------------------------------------------------------------------------
    common_cols <- tibble::tibble(
        # codeL1Gpa = get_ticker(obj),
        # codeL1 = get_ticker(obj),
        # code = get_ticker(obj),
        # instNum = NA_real_,
        # fundOrBmk = "Fund"
    )
    obj$NRC_inventory <- obj$constituents %>% # at this point I'm sure these variables exist
        dplyr::mutate_at(dplyr::vars(weight_scaled), ~ .x / 100) %>%
        dplyr::mutate(nav = sum(market_value, na.rm = TRUE)) %>%
        purrr::pmap_dfr(function(asset_class,
                                 market_currency,
                                 name,
                                 isin,
                                 weight_scaled,
                                 issuer_ticker,
                                 nameL2,
                                 country,
                                 block,
                                 market_value,
                                 nominal,
                                 notional_value,
                                 nav,
                                 ...) {
            df <- switch(
                asset_class,
                Equity = {
                    tibble::tibble(
                        instType = "Equity",
                        instTypeL1 = "Equity",
                        instTypeL2 = "Pflio",
                        instTypeL3 = "Shares",
                        description = name,
                        isin = isin,
                        weight = weight_scaled,
                        factorType = c("Equity", "Currency"),
                        factor = c(issuer_ticker, market_currency),
                        factorSector = c(nameL2, NA_character_),
                        factorCountry = country,
                        factorCountryBlock = block,
                        factorIrDurCcy = market_currency,
                        factorWeight = 1,
                        factorWeightCtr = weight,
                        is_processed = TRUE
                    )
                },
                FX = {
                    tibble::tibble(
                        instType = "FxFwd",
                        instTypeL1 = "Cash",
                        instTypeL2 = "Deriv",
                        instTypeL3 = "FxFwd",
                        description = name,
                        fx = get_fx_rate(local_currency = issuer_ticker, base_currency = "USD", aod = get_aod(obj, "Date")),
                        weight = nominal * fx / nav * sign(notional_value), # exposure,,
                        factorType = c("Currency"),
                        factor = issuer_ticker,
                        factorCountry = country,
                        factorCountryBlock = block,
                        factorIrDurCcy = issuer_ticker,
                        factorWeight = 1,
                        factorWeightCtr = weight * factorWeight,
                        is_processed = TRUE
                    )
                },
                Cash = {
                    tibble::tibble(
                        instType = "Cash",
                        instTypeL1 = "Cash",
                        instTypeL2 = "Pflio",
                        description = market_currency,
                        weight = weight_scaled,
                        factorType = "Currency",
                        factor = market_currency,
                        factorCountry = country,
                        factorCountryBlock = block,
                        factorIrDurCcy = market_currency,
                        factorWeight = 1,
                        factorWeightCtr = weight_scaled,
                        is_processed = TRUE
                    )
                },
                Futures = {
                    tibble::tibble(
                        instType = "Future",
                        instTypeL1 = "Equity",
                        instTypeL2 = "Deriv",
                        instTypeL3 = "Future",
                        weight = weight_scaled, # it's already the exposure, check this
                        factorType = "Equity",
                        factor = issuer_ticker,
                        factorCountry = country,
                        factorCountryBlock = block,
                        factorIrDurCcy = market_currency,
                        factorWeight = 1,
                        factorWeightCtr = weight * factorWeight,
                        is_processed = TRUE
                    )
                },
                `Cash Collateral and Margins` = {
                    tibble::tibble(
                        instType = name,
                        instTypeL1 = "Cash",
                        instTypeL2 = "Pflio",
                        instTypeL3 = "Margin/Otc",
                        description = market_currency,
                        weight = weight_scaled,
                        factorType = "Currency",
                        factor = market_currency,
                        factorCountry = country,
                        factorCountryBlock = block,
                        factorIrDurCcy = market_currency,
                        factorWeight = 1,
                        factorWeightCtr = weight_scaled,
                        is_processed = TRUE
                    )
                },
                `Money Market` = {
                    tibble::tibble(
                        instType = "Cash",
                        instTypeL1 = "Cash",
                        instTypeL2 = "Pflio",
                        description = market_currency,
                        weight = weight_scaled,
                        factorType = "Currency",
                        factor = market_currency,
                        factorCountry = country,
                        factorCountryBlock = block,
                        factorIrDurCcy = market_currency,
                        factorWeight = 1,
                        factorWeightCtr = weight_scaled,
                        is_processed = TRUE
                    )
                },
                {
                    futile.logger::flog.info(glue::glue("no match for asset class = {asset_class}, ticker = {obj$ticker}"))
                    tibble::tibble(
                        instType = asset_class,
                        is_processed = FALSE
                    )
                }
            )
        })
    # %>% dplyr::bind_cols(tidyr::uncount(common_cols, nrow(.)), .)
    return(obj)
} # compute_NRC_inventory.QueryResult_UK


## -----------------------------------------------------------------------------
# =========================== GETTERS ==========================================
## -----------------------------------------------------------------------------
get_ticker <- function(obj) {
    UseMethod("get_ticker")
} # get_ticker

get_ticker.QueryResult <- function(obj) {
    assertthat::has_name(obj, "ticker")
    obj[["ticker"]]
} # get_ticker.QueryResult

get_aod <- function(obj, type) {
    UseMethod("get_aod")
} # get_aod

get_aod.QueryResult <- function(obj, type) {
    assertthat::assert_that(type %in% c("numeric", "Date"))
    assertthat::has_name(obj, "aod")
    if (type == "numeric") obj[["aod"]]
    else if (type == "Date") as.Date(as.character(obj[["aod"]]), format = "%Y%m%d")
} # get_aod.QueryResult

get_NRC_inventory <- function(obj) {
    UseMethod("get_NRC_inventory")
} # get_NRC_inventory

get_NRC_inventory.QueryResult <- function(obj) {
    assertthat::has_name(obj, "NRC_inventory")
    obj[["NRC_inventory"]]
} # get_NRC_inventory

get_summary_data.QueryResult <- function(obj) {
    assertthat::has_name(obj, "summary_data")
    obj[["summary_data"]]
}

## -----------------------------------------------------------------------------
# =========================== MONGO METHODS ====================================
## -----------------------------------------------------------------------------
to_mongo <- function(obj) {
    UseMethod("to_mongo")
} # to_mongo

to_mongo.QueryResult <- function(obj) {
    futile.logger::flog.info(glue::glue("writing NRC constitunts to mongo ticker = {get_ticker(obj)}"))
    assertthat::assert_that(exists("isharesConstituents4NRC_Save"))
    isharesConstituents4NRC_Save(
        aod = get_aod(obj, "numeric"),
        ticker = get_ticker(obj),
        isin = get_summary_data(obj)[["isin"]],
        data = get_NRC_inventory(obj)
    )
} # to_mongo.QueryResult
