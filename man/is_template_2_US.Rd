% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/IShares_US-class.R
\name{is_template_2_US}
\alias{is_template_2_US}
\title{Check if data is in template 2 format}
\usage{
is_template_2_US(melted_data)
}
\arguments{
\item{melted_data}{constituent data in melted format}
}
\value{
\code{TRUE} if data is in template 2 format, \code{FALSE} otherwise
}
\description{
data is in template 2 format if there're two template 1 stacked one above the
other. This is the case for the Russell 2500 ETF. This ETF buys the Russel 2000
and than buys 500 stocks. The first chunk of data has the  Russel 2000 ETF while
the second chinck expands this ETF showing all the 2500 names
}
