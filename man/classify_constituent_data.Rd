% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/IShares-class.R
\name{classify_constituent_data}
\alias{classify_constituent_data}
\title{Classify constituent data}
\usage{
classify_constituent_data(melted_constituents_list, region = "US",
  n_template = 3)
}
\arguments{
\item{melted_constituents_list}{list of dataframes with the constituent csv file
in melted format downloaded using the \code{readr::melt_csv} function}

\item{region}{IShares geographical region (e.g. US)}

\item{n_template}{number of template data model availabe}
}
\value{
a named-list with the names of the parsing functions for each constituent
   dataframe. If no template matches the data \code{NULL} is returned instead
}
\description{
The constituent data is downloaded in a melted format so as not to incurr in
reading errors. After that we have to understand the structure of the csv files.
For this reason we assume there exsist fix templates. This function assigns
templates to constituent data: for instance, if a dataframe is in template_1
format than \code{classify_constituent_data} will assign to it the string
\code{parse_template_1}, which is the function used for parsing the dataframe
}
