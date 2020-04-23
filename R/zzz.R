# .onLoad <- function(libname, pkgname) {
#     file_to_source <- c("~/RProjects/Weekly_Commons/CollectData_Base.R", # for downloading data from GPADD
#                         "~/RProjects/Weekly_Commons/CollectData_IShares.R")
#     packageStartupMessage(
#         "Hi, thank you for using the ETF package!\n",
#         glue::glue("I'll try now to load {file_to_source}")
#     )
#     try({
#         for (file in file_to_source) source(file)
#     })
# } # .onLoad
