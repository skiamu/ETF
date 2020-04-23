#' @importFrom magrittr %>%
#' @export
magrittr::`%>%`

#' @importFrom zeallot %<-%
#' @export
zeallot::`%<-%`

#' @importFrom readr read_delim
#' @export
readr::read_delim


IShares_link_IT <- "https://www.ishares.com/it/investitori-professionali/it/product-screener/product-screener-v3.jsn?dcrPath=/templatedata/config/product-screener-v3/data/it/it/product-screener/product-screener&siteEntryPassthrough=true"
IShares_link_US <- "https://www.ishares.com/us/product-screener/product-screener-v3.jsn?dcrPath=/templatedata/config/product-screener-v3/data/en/us-ishares/product-screener-ketto&siteEntryPassthrough=true"
IShares_link_UK <- "https://www.ishares.com/uk/individual/en/product-screener/product-screener-v3.jsn?dcrPath=/templatedata/config/product-screener-v3/data/en/uk/product-screener/product-screener&siteEntryPassthrough=true"


from_int_to_date <- function(n) {
    as.Date(as.character(n), format = "%Y%m%d")
} # from_int_to_date

create_internal_data <- function() {
    ## -------------------------------------------------------------------------
    # CREATE MAPPING ITALIAN COUNTRY NAMES --> ISO3CODES
    ## -------------------------------------------------------------------------
    country_list_raw <- httr::GET(url = "https://restcountries.eu/rest/v2/all")
    country_list <- httr::content(country_list_raw, as = "parsed")
    country_mapping <- country_list %>%
        imap_dfr( ~ {
            tibble(
                idx = .y,
                country_name_eng = .x[["name"]],
                country_name_ita = dplyr::if_else(is.null(.x[["translations"]][["it"]]),
                                     NA_character_,
                                     .x[["translations"]][["it"]]),
                alpha_3_code = dplyr::if_else(is.null(.x[["alpha3Code"]]),
                                              NA_character_,
                                              .x[["alpha3Code"]])
            )
        })
    ## -------------------------------------------------------------------------
    # CREATE MAPPING ISO3CODES --> GPA COUNTRY NAME
    ## -------------------------------------------------------------------------
    gpa_country_mapping <- tryCatch({
        getGpaDD_ReadDelim("risk/barra/map/country/Get?")
    },
    error = function(e) {
        return(NULL)
    })
    gpa_country_mapping <- gpa_country_mapping %>%
        dplyr::distinct(iso3, .keep_all = TRUE)
    ## -------------------------------------------------------------------------
    # CREATE MAPPING ISO3CODES --> GPA COUNTRY NAME
    ## -------------------------------------------------------------------------
    gpa_sector_mapping <- tryCatch({
        getGpaDD_ReadDelim("ap/instr/sector/ml/Get?")
    },
    error = function(e) {
        return(NULL)
    })
    gpa_sector_mapping <- gpa_sector_mapping %>%
        dplyr::select(idL3:nameL1) %>%
        dplyr::distinct()
    ## -------------------------------------------------------------------------
    # THE THE RESULTS TO 'R/sysdata.rda'
    ## -------------------------------------------------------------------------
    usethis::use_data(country_mapping, gpa_country_mapping, gpa_sector_mapping, internal = TRUE, overwrite = TRUE)
} # create_internal_data

get_fx_rate <- function(local_currency, base_currency, aod) {
    # local_currency <- "JPY"
    # base_currency <- "USD"
    # aod <- as.Date("2020-02-12")
    futile.logger::flog.info(glue::glue("Downloading fx time-series local_currency = {local_currency}, base_currency = {base_currency}, aod = {aod}"))
    assertthat::assert_that(exists("getGpaDD_ReadDelim"))
    local_ts <- glue::glue("ap/instr/ts/Get?&ID=&CODE={local_currency}&AOD={aod}") %>% getGpaDD_ReadDelim
    base_ts <- glue::glue("ap/instr/ts/Get?&ID=&CODE={base_currency}&AOD={aod}") %>% getGpaDD_ReadDelim
    local_fx <- local_ts %>%
        dplyr::filter(dt == aod) %>%
        dplyr::pull(priceEur)
    base_fx <- base_ts %>%
        dplyr::filter(dt == aod) %>%
        dplyr::pull(fx)
    return(local_fx * base_fx)
} # get_fx_rate
