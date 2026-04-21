make_raw_dt <- function(dat) {
  dat_prepared <- dat |>
    dplyr::filter(!is.na(.data$value_lo)) |> # only want mitigators selected by schemes
    dplyr::mutate(
      dplyr::across(
        c(
          tidyselect::starts_with("scheme"),
          tidyselect::starts_with("run"),
          tidyselect::starts_with("mitigator"),
          .data$value_point_or_range,
          .data$value_time_profile
        ),
        factor # enables discrete selection in interactive table
      ),
    )

  dat_prepared |>
    DT::datatable(
      fillContainer = TRUE,
      filter = "top",
      rownames = FALSE,
      selection = "none",
      extensions = "Buttons",
      options = list(
        dom = "Bftp",
        buttons = list(
          list(
            extend = "csv",
            filename = paste0(Sys.Date(), "_mitigator-comparison-data"),
            text = "Download (CSV)"
          )
        )
      ),
      callback = DT::JS(
        "$('button.buttons-csv').css('background','#337ab7');
        $('button.buttons-csv').css('border','#2e6da4');
        return table;"
      )
    ) |>
    DT::formatRound(
      # present to 3 dp, download will have unrestricted dp
      columns = c(
        "value_lo",
        "value_hi",
        "value_mid",
        "value_range",
        "nee_p10",
        "nee_p90",
        "nee_p50",
        "nee_mean"
      ),
      digits = 3
    )
}

make_mitigator_dt <- function(mitigator_lookup) {
  mitigator_lookup_prepared <- mitigator_lookup |>
    dplyr::select(
      .data$`Mitigator code`,
      `Mitigator name` = .data$`Strategy name`,
      `Mitigator variable` = .data$`Strategy variable`,
      .data$`Mitigator type`,
      .data$`Activity type`,
      `Mitigator grouping` = .data$Grouping
    ) |>
    dplyr::mutate(dplyr::across(tidyselect::everything(), factor))

  mitigator_lookup_prepared |>
    DT::datatable(
      fillContainer = TRUE,
      filter = "top",
      rownames = FALSE,
      selection = "none",
      extensions = "Buttons",
      options = list(
        dom = "Bftp",
        buttons = list(
          list(
            extend = "csv",
            filename = paste0(Sys.Date(), "_mitigator-lookup"),
            text = "Download (CSV)"
          )
        )
      ),
      callback = DT::JS(
        "$('button.buttons-csv').css('background','#337ab7');
        $('button.buttons-csv').css('border','#2e6da4');
        return table;"
      )
    )
}


#' Make the scheme lookup DT object
#'
#' Renders a DT object listing schemes represented in the app with filters and a
#' CSV download button.
#'
#' @param trust_code_lookup Tibble of scheme data - as provided by the
#'   `get_trust_lookup()` function in `fct_tabulate.R`
#'
#' @return DT object listing schemes
#' @export
make_scheme_dt <- function(trust_code_lookup) {
  schemes_prepared <- trust_code_lookup |>
    dplyr::mutate(
      `Scheme name` = .data$`Name of Hospital site`,
      `Trust name` = .data$`Name of Trust`,
      `Scheme code` = .data$`Trust ODS Code`,
      .keep = "none"
    ) |>
    dplyr::mutate(dplyr::across(tidyselect::everything(), factor)) |>
    dplyr::arrange(.data$`Scheme name`)

  schemes_prepared |>
    DT::datatable(
      fillContainer = TRUE,
      filter = "top",
      rownames = FALSE,
      selection = "none",
      extensions = "Buttons",
      options = list(
        pageLength = 100, # show all schemes
        dom = "Bft", # excluding the paging buttons
        buttons = list(
          list(
            extend = "csv",
            filename = paste0(Sys.Date(), "_mitigator-comparison-data"),
            text = "Download (CSV)"
          )
        )
      ),
      callback = DT::JS(
        "$('button.buttons-csv').css('background','#337ab7');
        $('button.buttons-csv').css('border','#2e6da4');
        return table;"
      )
    )
}

#' Prepare data for the mitigator uptake DT object
#'
#' Renders a tibble showing the proportion of schemes using each mitigator. Two
#' rates are shown: 1. covers all available schemes, 2. covers the subset of
#' schemes selected by the user.
#'
#' @param dat Tibble - the full prepared dataset for this app
#' @param selected_schemes Character vector - a list of mitigator_codes selected
#'   by the user
#'
#' @return Tibble listing the mitigators and summaries of schemes using them
#' @export
make_mitigator_uptake_dat <- function(dat, selected_schemes) {
  dat_return <-
    dat |>
    # remove ampersand from mitigator names - causes issues with DT filters
    dplyr::mutate(
      mitigator_name = gsub(
        pattern = "A&E",
        x = .data$mitigator_name,
        replacement = "ED"
      )
    ) |>
    # count schemes per mitigator
    dplyr::summarise(
      n_schemes_using_all = dplyr::n_distinct(
        .data$scheme_code,
        na.rm = TRUE
      ),
      n_schemes_using_selected = dplyr::n_distinct(
        .data$scheme_code[.data$scheme_code %in% selected_schemes],
        na.rm = TRUE
      ),
      .by = c(
        .data$mitigator_activity_type,
        .data$mitigator_group,
        .data$mitigator_name
      )
    ) |>
    # convert to rate
    dplyr::mutate(
      # get denominators
      n_schemes_all = dplyr::n_distinct(
        dat$scheme_code,
        na.rm = TRUE
      ),
      n_schemes_selected = dplyr::n_distinct(
        dat$scheme_code[dat$scheme_code %in% selected_schemes],
        na.rm = TRUE
      ),

      # convert to rates
      n_schemes_using_all_rate = .data$n_schemes_using_all /
        .data$n_schemes_all,
      n_schemes_using_selected_rate = .data$n_schemes_using_selected /
        .data$n_schemes_selected
    ) |>
    # prepare for display
    dplyr::select(
      -c(
        .data$n_schemes_using_all,
        .data$n_schemes_all,
        .data$n_schemes_using_selected,
        .data$n_schemes_selected
      )
    ) |>
    dplyr::mutate(
      # convert mitigators to factors for drop-down selectors in DT
      mitigator_activity_type = .data$mitigator_activity_type |> factor(),
      mitigator_group = .data$mitigator_group |> factor(),
      mitigator_name = .data$mitigator_name |> factor()
    )

  dat_return
}

#' Make the scheme uptake DT object
#'
#' Renders a DT object showing the proportion of mitigators in use by each
#' scheme. Two rates are shown: 1. covers all available mitigators, 2. covers
#' the subset of mitigators selected by the user.
#'
#' @param dat Tibble - the full prepared dataset for this app
#' @param selected_schemes Character vector - a list of mitigator_codes selected
#'   by the user
#'
#' @return DT object listing schemes and the proportions of mitigators in use by
#'   them
#' @export
make_mitigator_uptake_dt <- function(dat, selected_schemes) {
  # prepare the data
  make_mitigator_uptake_dat(dat = dat, selected_schemes = selected_schemes) |>
    # display as DT
    DT::datatable(
      rownames = FALSE,
      options = list(pageLength = 100, dom = "ft"),
      fillContainer = TRUE,
      escape = TRUE,
      filter = "top",
      colnames = c(
        "Activity type",
        "Mitigator group",
        "Mitigator",
        "Coverage (all schemes)",
        "Coverage (selected schemes)"
      )
    ) |>
    DT::formatPercentage(
      columns = c("n_schemes_using_all_rate", "n_schemes_using_selected_rate")
    )
}


#' Prepare the data for the scheme mitigator uptake DT object
#'
#' Renders a tibble showing the proportion of mitigators used by each scheme.
#' Two rates are shown: 1. covers all available mitigators, 2. covers the subset
#' of mitigators selected by the user.
#'
#' @param dat Tibble - the full prepared dataset for this app
#' @param selected_schemes Character vector - a list of scheme_codes selected by
#'   the user
#' @param focal_scheme Character vector - the focal scheme_code
#' @param selected_mitigators Character vector - a list of mitigators selected
#'   by the user
#'
#' @return Tibble listing schemes and the proportions of mitigators in use by
#'   them
#' @export
make_scheme_uptake_dat <- function(
  dat,
  selected_mitigators,
  selected_schemes,
  focal_scheme
) {
  dat_return <-
    dat |>
    # count schemes per mitigator
    dplyr::summarise(
      n_mitigators_using_all = dplyr::n_distinct(
        .data$mitigator_code,
        na.rm = TRUE
      ),
      n_mitigators_using_selected = dplyr::n_distinct(
        .data$mitigator_code[.data$mitigator_code %in% selected_mitigators],
        na.rm = TRUE
      ),
      .by = c(.data$scheme_code, .data$scheme_name)
    ) |>
    # convert to rate
    dplyr::mutate(
      # get denominators
      n_mitigators_all = dplyr::n_distinct(
        dat$mitigator_code,
        na.rm = TRUE
      ),
      n_mitigators_selected = dplyr::n_distinct(
        dat$mitigator_code[dat$mitigator_code %in% selected_mitigators],
        na.rm = TRUE
      ),

      # convert to rates
      n_mitigators_using_all_rate = .data$n_mitigators_using_all /
        .data$n_mitigators_all,
      n_mitigators_using_selected_rate = .data$n_mitigators_using_selected /
        .data$n_mitigators_selected
    ) |>
    # prepare for display
    dplyr::select(
      -c(
        .data$n_mitigators_using_all,
        .data$n_mitigators_all,
        .data$n_mitigators_using_selected,
        .data$n_mitigators_selected
      )
    ) |>
    dplyr::mutate(
      # convert scheme details to factors for drop-down selectors in DT
      scheme_code = .data$scheme_code |> factor(),
      scheme_name = .data$scheme_name |> factor()
    ) |>
    dplyr::filter(!is.na(.data$scheme_code))

  dat_return
}


#' Make the scheme mitigator uptake DT object
#'
#' Renders a DT object showing the proportion of mitigators used by each scheme.
#' Two rates are shown: 1. covers all available mitigators, 2. covers the subset
#' of mitigators selected by the user.
#'
#' @param dat Tibble - the full prepared dataset for this app
#' @param selected_schemes Character vector - a list of scheme_codes selected by
#'   the user
#' @param focal_scheme Character vector - the focal scheme_code
#' @param selected_mitigators Character vector - a list of mitigators selected
#'   by the user
#'
#' @return DT object listing schemes and the proportions of mitigators in use by
#'   them
#' @export
make_scheme_uptake_dt <- function(
  dat,
  selected_mitigators,
  selected_schemes,
  focal_scheme
) {
  # prepare the data
  make_scheme_uptake_dat(
    dat = dat,
    selected_mitigators = selected_mitigators,
    selected_schemes = .data$selected_scheme,
    focal_scheme = focal_scheme
  ) |>
    # display as DT
    DT::datatable(
      rownames = FALSE,
      options = list(pageLength = 100, dom = "Bft"),
      fillContainer = TRUE,
      escape = TRUE,
      style = "default", # needed to ensure formatStyle works as expected - due to clashes with bslib & bootstrap theme
      colnames = c(
        "Scheme code",
        "Scheme name",
        "Coverage (all mitigators)",
        "Coverage (selected mitigators)"
      ),
      filter = "top"
    ) |>
    DT::formatPercentage(
      columns = c(
        "n_mitigators_using_all_rate",
        "n_mitigators_using_selected_rate"
      )
    ) |>
    # style selected schemes in bold
    DT::formatStyle(
      columns = "scheme_code",
      target = "row",
      # highlight all selected schemes in bold
      fontWeight = DT::styleEqual(
        levels = c(selected_schemes),
        "bold",
        "normal"
      ),
      # highlight focal scheme in red too
      color = DT::styleEqual(levels = focal_scheme, "red", "black")
    )
}
