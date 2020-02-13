library(testthat)
library(ETF)

context("IShares")

u <- IShares(summary_link = IShares_link_IT)
x <- IShares_IT(summary_link = IShares_link_IT,
                get_constituents = TRUE,
                download_constituents_csv = FALSE)
y <- IShares_US(summary_link = IShares_link_US,
                get_constituents = TRUE,
                download_constituents_csv = FALSE)
z <- IShares_UK(summary_link = IShares_link_UK,
                get_constituents = TRUE,
                download_constituents_csv = FALSE)

to_csv(x, output_folder = "./output")
to_csv(y, output_folder = "./output")
to_csv(z, output_folder = "./output")

get_summary_data(y) -> a
