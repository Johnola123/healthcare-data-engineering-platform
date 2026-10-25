# Optional independent R-based QC of source extracts.
# Install: install.packages(c("dplyr", "readr", "testthat"))
suppressPackageStartupMessages({library(dplyr); library(readr); library(testthat)})
patients <- read_csv("data/raw/patients.csv", show_col_types = FALSE)
encounters <- read_csv("data/raw/encounters.csv", show_col_types = FALSE)
stopifnot(nrow(patients) > 0, nrow(encounters) > 0)
test_that("keys and clinical bounds are valid", {
  expect_equal(n_distinct(patients$patient_id), nrow(patients))
  expect_equal(n_distinct(encounters$encounter_id), nrow(encounters))
  expect_true(all(encounters$patient_id %in% patients$patient_id))
  expect_true(all(encounters$total_cost >= 0, na.rm = FALSE))
  expect_true(all(between(patients$age, 0, 120)))
})
print(encounters |> group_by(department) |> summarise(encounters=n(), total_cost=sum(total_cost), .groups="drop"))
