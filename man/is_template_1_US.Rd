% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/IShares_US-class.R
\name{is_template_1_US}
\alias{is_template_1_US}
\title{check if data is in template 1 format}
\usage{
is_template_1_US(melted_data)
}
\arguments{
\item{melted_data}{constituent dataframe in melted format}
}
\value{
\code{TRUE} if data is in template 1 format, \code{FALSE} otherwise
}
\description{
the constituents are in template 1 if in the range A2:A8 there are the following
strings: Fund Holdings as of, Inception Date, Shares Outstanding, Stock, Bond
Cash and Other. In the column B there are the values respectively.
}
