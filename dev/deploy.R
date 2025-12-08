deploy <- function(
  server_name = "connect.strategyunitwm.nhs.uk",
  type = c("prod", "dev")
) {
  type <- match.arg(type)

  app_id <- 108
  app_name <- "nhp_compare_mitigation_prediction_app"
  app_title <- "Compare NHP Activity Mitigation Predictions"

  if (type == "dev") {
    app_id <- 193
    app_name <- paste0(app_name, "_dev")
    app_title <- paste(app_title, "(dev)")
  }

  rsconnect::deployApp(
    appName = app_name,
    appTitle = app_title,
    server = server_name,
    appId = app_id,
    appFiles = c(
      "R/",
      "inst/",
      "NAMESPACE",
      "DESCRIPTION",
      "app.R"
    ),
    envVars = c(
      "AZ_STORAGE_EP",
      "AZ_STORAGE_CONTAINER_INPUTS",
      "AZ_STORAGE_CONTAINER_SUPPORT"
    ),
    lint = FALSE,
    forceUpdate = TRUE
  )
}

# Deploy development version between releases
deploy(type = "dev")

# Deploy on release
deploy(type = "prod")
