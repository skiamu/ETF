context("IShares")

w <- IShares(summary_link = IShares_link_IT)
res <- download_summary_data(w)

# consistency check, is they change the API this is gonna broke meaning that
# the download method has to be changerd
test_that("IShares summary data right structure", {
    expect_equal(names(res), c("tableData", "config"))
    expect_equal(names(res$tableData), c("columns", "data"))
})


