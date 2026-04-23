#' Prepare heatmap data
#'
#' Prepares the data ready for use in the heatmap plots.
#'
#' @param dat Tibble of mitigator data as produced by `populate_table()` in
#'   `fct_tabulate.R`
#' @param mitigator_codes Character vector of mitigator codes to display in the
#'   heatmap
#' @param scheme_codes Character vector of scheme codes to display in the
#'   heatmap
#' @param heatmap_type String describing the type of heatmap to produce - as
#'   defined by `input$heatmap_type`
#' @param scheme_order String describing the display order for schemes
#' @param mitigator_order String description of the display order for mitigators
#' @param values_displayed String description of the values displayed - as
#'   defined by `input$values_displayed`
#' @param toggle_heatmap_nee Boolean (default = FALSE) should the display
#'   include a column for NEE values for mitigators?
#' @param toggle_heatmap_aggregate_summaries Boolean (default = FALSE) should
#'   the display include columns showing minimum, maximum and mean values for
#'   each mitigator?
#' @param toggle_heatmap_scheme_adornments  Boolean (default = FALSE) should the
#'   scheme details include additional information such as scheme code, run
#'   stage and horizon year?
#' @param focal_scheme_code Code for the scheme chosen as the focal scheme by
#'   user
#'
#' @returns Tibble of data ready for use with heatmap plots
prepare_heatmap_dat <- function(
  dat,
  mitigator_codes,
  scheme_codes,
  focal_scheme_code,
  heatmap_type = "value_binary",
  scheme_order,
  mitigator_order,
  values_displayed,
  toggle_heatmap_nee = FALSE,
  toggle_heatmap_aggregate_summaries = FALSE,
  toggle_heatmap_scheme_adornments = FALSE
) {
  ## wrangle data ----

  # set up a list of 'context' scheme codes, i.e. are for display purposes
  scheme_additional <- c("NEE", "MIN", "MAX", "MEAN")

  # limit to schemes and mitigators of choice
  dat <-
    dat |>
    dplyr::filter(
      .data$mitigator_code %in% mitigator_codes,
      .data$scheme_code %in% scheme_codes
    )

  # should scheme names include scheme code, run stage and baseline/horizon years?
  if (toggle_heatmap_scheme_adornments) {
    dat <-
      dat |>
      dplyr::mutate(
        # strip out the pencil and pushpin emojis
        scheme_name = .data$scheme_name |>
          stringr::str_remove_all(pattern = " \\[(preliminary|focal)\\]"),
        # add in the requested adornments
        scheme_name = glue::glue(
          "{scheme_name} ",
          "[{scheme_code}] \n",
          "({run_stage} ",
          "{year_baseline} to {year_horizon})"
        )
      )
  }

  # create a list to hold the working dat objects
  dat_list <- list(dat)

  # should nee values be displayed?
  if (toggle_heatmap_nee && heatmap_type != "value_binary") {
    dat_nee <-
      dat |>
      # keep all mitigators, even if contain no NEE data
      # this is useful for aligning plots
      dplyr::distinct(.data$mitigator_code, .keep_all = TRUE) |>
      dplyr::mutate(
        scheme_name = "National Elicitation Exercise",
        scheme_code = "NEE",
        year_baseline = 2019,
        year_horizon = 2041,
        value_lo = .data$nee_p10,
        value_mid = .data$nee_p50,
        value_hi = .data$nee_p90
      ) |>
      dplyr::select(
        # scheme
        .data$scheme_name,
        .data$scheme_code,
        # mitigator
        dplyr::starts_with("mitigator"),
        # years
        .data$year_baseline,
        .data$year_horizon,
        # values
        .data$value_lo,
        .data$value_mid,
        .data$value_hi
      ) |>
      dplyr::distinct() |>
      dplyr::mutate(value_range = abs(.data$value_hi - .data$value_lo))

    # add to the data list
    dat_list[length(dat_list) + 1] <- list(dat_nee)
  }

  # should min/max/mean values be displayed?
  if (toggle_heatmap_aggregate_summaries && heatmap_type != "value_binary") {
    # define a list of values to aggregate over
    aggregate_values <- c("value_lo", "value_mid", "value_hi", "value_range")

    # get aggregate summaries for mitigators (displayed on right)
    dat_agg_mit <-
      dplyr::bind_rows(
        # low values
        dat |>
          dplyr::filter(!is.na(.data$mitigator_code)) |>
          dplyr::summarise(
            dplyr::across(
              .cols = dplyr::any_of(aggregate_values),
              .fns = ~ min(.x, na.rm = TRUE)
            ),
            scheme_name = "Minimum",
            scheme_code = "MIN",
            .by = dplyr::starts_with("mitigator")
          ),

        # max values
        dat |>
          dplyr::filter(!is.na(.data$mitigator_code)) |>
          dplyr::summarise(
            dplyr::across(
              .cols = dplyr::any_of(aggregate_values),
              .fns = ~ max(.x, na.rm = TRUE)
            ),
            scheme_name = "Maximum",
            scheme_code = "MAX",
            .by = dplyr::starts_with("mitigator")
          ),

        # mean values
        dat |>
          dplyr::filter(!is.na(.data$mitigator_code)) |>
          dplyr::summarise(
            dplyr::across(
              .cols = dplyr::any_of(aggregate_values),
              .fns = ~ mean(.x, na.rm = TRUE)
            ),
            scheme_name = "Average",
            scheme_code = "MEAN",
            .by = dplyr::starts_with("mitigator")
          ),
      )

    # add to the data list
    dat_list[length(dat_list) + 1] <- list(dat_agg_mit)

    # get aggregate summaries for schemes (displayed at bottom)
    dat_agg_sch <-
      dplyr::bind_rows(
        # low values
        dat |>
          dplyr::filter(!is.na(.data$scheme_code)) |>
          dplyr::summarise(
            dplyr::across(
              .cols = dplyr::any_of(aggregate_values),
              .fns = ~ min(.x, na.rm = TRUE)
            ),
            mitigator_name = "Minimum",
            mitigator_code = "MIN",
            .by = dplyr::starts_with("scheme")
          ),

        # max values
        dat |>
          dplyr::filter(!is.na(.data$scheme_code)) |>
          dplyr::summarise(
            dplyr::across(
              .cols = dplyr::any_of(aggregate_values),
              .fns = ~ max(.x, na.rm = TRUE)
            ),
            mitigator_name = "Maximum",
            mitigator_code = "MAX",
            .by = dplyr::starts_with("scheme")
          ),

        # mean values
        dat |>
          dplyr::filter(!is.na(.data$scheme_code)) |>
          dplyr::summarise(
            dplyr::across(
              .cols = dplyr::any_of(aggregate_values),
              .fns = ~ mean(.x, na.rm = TRUE)
            ),
            mitigator_name = "Average",
            mitigator_code = "MEAN",
            .by = dplyr::starts_with("scheme")
          ),
      )

    # add to the data list
    dat_list[length(dat_list) + 1] <- list(dat_agg_sch)
  }

  # consolidate the data
  dat <-
    dplyr::bind_rows(dat_list)

  # calculate the heatmap values
  dat <-
    dat |>
    dplyr::mutate(
      # produce a binary value to show if a value has been set
      value_binary = !is.na(.data$value_lo),
      # round the values to avoid precision clutter
      dplyr::across(
        c(.data$value_lo, .data$value_hi, .data$value_mid, .data$value_range),
        \(.x) janitor::round_half_up(.x, 3)
      ),
      # capture values as text (for tooltip)
      value_description = glue::glue(
        "{scales::percent(value_mid, accuracy = 0.1)} ",
        "({scales::percent(value_lo, accuracy = 0.1)} to ",
        "{scales::percent(value_hi, accuracy = 0.1)})"
      ),
      # flag the focal scheme
      focal_scheme_text = dplyr::if_else(
        condition = .data$scheme_code %in% focal_scheme_code,
        true = "Focal scheme",
        false = ""
      )
    ) |>
    # put each value type on its own row
    tidyr::pivot_longer(
      c(
        .data$value_lo,
        .data$value_hi,
        .data$value_mid,
        .data$value_range,
        .data$value_binary
      ),
      names_to = "value_type",
      values_to = "value"
    ) |>
    # limit to specified heatmap type
    dplyr::filter(
      .data$value_type == heatmap_type
    ) |>
    dplyr::mutate(
      # standardise values across mitigators (for heatmap colouring across rows)
      value_scaled = .data$value |>
        scales::rescale() |>
        janitor::round_half_up(digits = 3),

      # find min/max/average for each mitigator
      # don't include context schemes in these calculation
      value_min_mit = .data$value[!.data$scheme_code %in% scheme_additional] |>
        min(na.rm = TRUE),
      value_max_mit = .data$value[!.data$scheme_code %in% scheme_additional] |>
        max(na.rm = TRUE),
      value_mean_mit = .data$value[!.data$scheme_code %in% scheme_additional] |>
        mean(na.rm = TRUE),

      # rank schemes within each mitigator
      # NB, this causes issues if trying to subset value,
      # e.g. value_rank = value[!scheme_code %in% scheme_additional]
      # because of mis-matched row counts. Instead will create a temp_value
      # with scheme_additional set to NA, then rank on these.
      temp_value = dplyr::if_else(
        condition = .data$scheme_code %in% scheme_additional,
        true = NA,
        false = .data$value
      ),
      value_rank = .data$temp_value |> dplyr::desc() |> dplyr::min_rank(),
      value_rank_total = length(.data$value_rank[
        !.data$scheme_code %in% scheme_additional
      ]),

      .by = .data$mitigator_code
    ) |>
    dplyr::select(-.data$temp_value) |>
    dplyr::mutate(
      value_min_sch = .data$value |> min(na.rm = TRUE),
      value_max_sch = .data$value |> max(na.rm = TRUE),
      value_mean_sch = .data$value |> mean(na.rm = TRUE),

      .by = .data$scheme_code
    ) |>
    # create the tooltip text
    dplyr::mutate(
      # extract just the scheme name where it has been 'adorned' with other details
      scheme_name_bare = .data$scheme_name |>
        stringr::str_match(pattern = ".*?(?=\\s\\[)") |>
        as.character(),
      scheme_name_bare = dplyr::coalesce(
        .data$scheme_name_bare,
        .data$scheme_name
      ),

      # construct the tooltip text
      tooltip_text = dplyr::case_when(
        .data$scheme_code %in% c("NEE") & is.na(.data$value) ~
          "There is no NEE for this mitigator",
        .data$scheme_code %in% c("NEE") ~
          glue::glue(
            "<b>{scheme_name_bare}</b> [{scheme_code}]\n",
            "{mitigator_name} [{mitigator_code}]\n",
            "<b>{scales::percent(value, accuracy = 0.1)}</b> {values_displayed}\n",
            "<i>Mitigation:</i> {value_description}\n"
          ),
        .data$scheme_code %in% c("MIN", "MAX", "MEAN") ~
          glue::glue(
            "<b>{scheme_name_bare}</b> [{scheme_code}]\n",
            "{mitigator_name} [{mitigator_code}]\n",
            "<b>{scales::percent(value, accuracy = 0.1)}</b> {values_displayed}\n"
          ),
        .data$mitigator_code %in% c("MIN", "MAX", "MEAN") ~
          glue::glue(
            "<b>{scheme_name_bare}</b> [{scheme_code}]\n",
            "{mitigator_name} [{mitigator_code}]\n",
            "<b>{scales::percent(value, accuracy = 0.1)}</b> {values_displayed}\n"
          ),
        .default = glue::glue(
          "<b>{scheme_name_bare}</b> [{scheme_code}]",
          " {focal_scheme_text}\n",
          "{mitigator_name} [{mitigator_code}]\n",
          "Run stage: {run_stage}\n", # run stage
          "Baseline: {year_baseline}, Horizon: {year_horizon}\n", # baseline to horizon year
          "<b>{scales::percent(value, accuracy = 0.1)}</b> {values_displayed}\n",
          "<i>Scheme rank:</i> {value_rank} of {value_rank_total}\n",
          "<i>Mitigation:</i> {value_description}\n",
          "Mitigator ",
          "<i>min:</i> {scales::percent(value_min_mit, accuracy = 0.1)}, ",
          "<i>max:</i> {scales::percent(value_max_mit, accuracy = 0.1)}, ",
          "<i>mean:</i> {scales::percent(value_mean_mit, accuracy = 0.1)}\n",
          "Scheme ",
          "<i>min:</i> {scales::percent(value_min_sch, accuracy = 0.1)}, ",
          "<i>max:</i> {scales::percent(value_max_sch, accuracy = 0.1)}, ",
          "<i>mean:</i> {scales::percent(value_mean_sch, accuracy = 0.1)}"
        )
      )
    )

  ### order schemes ----
  # order schemes according to user preference
  base::switch(
    EXPR = scheme_order,

    "scheme_name_asc" = {
      dat <-
        dat |>
        dplyr::mutate(
          scheme_code = forcats::fct(
            x = .data$scheme_code,
            levels = dat |>
              dplyr::select(.data$scheme_code, .data$scheme_name) |>
              dplyr::filter(!is.na(.data$scheme_code)) |>
              dplyr::distinct(.data$scheme_code, .keep_all = TRUE) |>
              dplyr::arrange(.data$scheme_name) |>
              dplyr::pull(.data$scheme_code) |>
              c(scheme_additional) |>
              unique()
          )
        )
    },

    "scheme_name_desc" = {
      dat <-
        dat |>
        dplyr::mutate(
          scheme_code = forcats::fct(
            x = .data$scheme_code,
            levels = dat |>
              dplyr::select(.data$scheme_code, .data$scheme_name) |>
              dplyr::filter(!is.na(.data$scheme_code)) |>
              dplyr::distinct(.data$scheme_code, .keep_all = TRUE) |>
              dplyr::arrange(dplyr::desc(.data$scheme_name)) |>
              dplyr::pull(.data$scheme_code) |>
              c(scheme_additional) |>
              unique()
          )
        )
    },

    "scheme_mitigator_count_asc" = {
      dat <-
        dat |>
        dplyr::mutate(
          scheme_code = forcats::fct(
            x = .data$scheme_code,
            levels = dat |>
              dplyr::select(.data$scheme_code, .data$mitigator_code) |>
              dplyr::filter(!is.na(.data$scheme_code)) |>
              dplyr::filter(!is.na(.data$mitigator_code)) |>
              dplyr::summarise(
                mitigator_count = dplyr::n_distinct(
                  .data$mitigator_code,
                  na.rm = TRUE
                ),
                .by = .data$scheme_code
              ) |>
              dplyr::arrange(.data$mitigator_count, .data$scheme_code) |>
              dplyr::pull(.data$scheme_code) |>
              c(scheme_additional) |>
              unique()
          )
        )
    },

    "scheme_mitigator_count_desc" = {
      dat <-
        dat |>
        dplyr::mutate(
          scheme_code = forcats::fct(
            x = .data$scheme_code,
            levels = dat |>
              dplyr::select(.data$scheme_code, .data$mitigator_code) |>
              dplyr::filter(!is.na(.data$scheme_code)) |>
              dplyr::filter(!is.na(.data$mitigator_code)) |>
              dplyr::summarise(
                mitigator_count = dplyr::n_distinct(
                  .data$mitigator_code,
                  na.rm = TRUE
                ),
                .by = .data$scheme_code
              ) |>
              dplyr::arrange(
                dplyr::desc(.data$mitigator_count),
                .data$scheme_code
              ) |>
              dplyr::pull(.data$scheme_code) |>
              c(scheme_additional) |>
              unique()
          )
        )
    },

    "scheme_average_asc" = {
      dat <-
        dat |>
        dplyr::mutate(
          scheme_code = forcats::fct(
            x = .data$scheme_code,
            levels = dat |>
              dplyr::select(.data$scheme_code, .data$value) |>
              dplyr::filter(!is.na(.data$scheme_code)) |>
              dplyr::summarise(
                average = mean(.data$value, na.rm = TRUE),
                .by = .data$scheme_code
              ) |>
              dplyr::arrange(.data$average, .data$scheme_code) |>
              dplyr::pull(.data$scheme_code) |>
              c(scheme_additional) |>
              unique()
          )
        )
    },

    "scheme_average_desc" = {
      dat <-
        dat |>
        dplyr::mutate(
          scheme_code = forcats::fct(
            x = .data$scheme_code,
            levels = dat |>
              dplyr::select(.data$scheme_code, .data$value) |>
              dplyr::filter(!is.na(.data$scheme_code)) |>
              dplyr::summarise(
                average = mean(.data$value, na.rm = TRUE),
                .by = .data$scheme_code
              ) |>
              dplyr::arrange(dplyr::desc(.data$average), .data$scheme_code) |>
              dplyr::pull(.data$scheme_code) |>
              c(scheme_additional) |>
              unique()
          )
        )
    },
  )

  ### order mitigators ----
  # order mitigators according to user preference
  base::switch(
    EXPR = mitigator_order,

    "mitigator_name_asc" = {
      dat <-
        dat |>
        dplyr::mutate(
          mitigator_code = forcats::fct(
            x = .data$mitigator_code,
            levels = dat |>
              dplyr::select(.data$mitigator_code, .data$mitigator_name) |>
              dplyr::filter(!is.na(.data$mitigator_code)) |>
              dplyr::distinct(.data$mitigator_code, .keep_all = TRUE) |>
              dplyr::arrange(.data$mitigator_name) |>
              dplyr::pull(.data$mitigator_code)
          )
        )
    },

    "mitigator_name_desc" = {
      dat <-
        dat |>
        dplyr::mutate(
          mitigator_code = forcats::fct(
            x = .data$mitigator_code,
            levels = dat |>
              dplyr::select(.data$mitigator_code, .data$mitigator_name) |>
              dplyr::filter(!is.na(.data$mitigator_code)) |>
              dplyr::distinct(.data$mitigator_code, .keep_all = TRUE) |>
              dplyr::arrange(dplyr::desc(.data$mitigator_name)) |>
              dplyr::pull(.data$mitigator_code)
          )
        )
    },

    "mitigator_scheme_count_asc" = {
      dat <-
        dat |>
        dplyr::mutate(
          mitigator_code = forcats::fct(
            x = .data$mitigator_code,
            levels = dat |>
              dplyr::select(.data$mitigator_code, .data$scheme_code) |>
              dplyr::filter(!is.na(.data$mitigator_code)) |>
              dplyr::filter(!is.na(.data$scheme_code)) |>
              dplyr::summarise(
                scheme_count = dplyr::n_distinct(
                  .data$scheme_code,
                  na.rm = TRUE
                ),
                .by = .data$mitigator_code
              ) |>
              dplyr::arrange(.data$scheme_count, .data$mitigator_code) |>
              dplyr::pull(.data$mitigator_code)
          )
        )
    },

    "mitigator_scheme_count_desc" = {
      dat <-
        dat |>
        dplyr::mutate(
          mitigator_code = forcats::fct(
            x = .data$mitigator_code,
            levels = dat |>
              dplyr::select(.data$mitigator_code, .data$scheme_code) |>
              dplyr::filter(!is.na(.data$mitigator_code)) |>
              dplyr::filter(!is.na(.data$scheme_code)) |>
              dplyr::summarise(
                scheme_count = dplyr::n_distinct(
                  .data$scheme_code,
                  na.rm = TRUE
                ),
                .by = .data$mitigator_code
              ) |>
              dplyr::arrange(
                dplyr::desc(.data$scheme_count),
                .data$mitigator_code
              ) |>
              dplyr::pull(.data$mitigator_code)
          )
        )
    },

    "mitigator_average_asc" = {
      dat <-
        dat |>
        dplyr::mutate(
          mitigator_code = forcats::fct(
            x = .data$mitigator_code,
            levels = dat |>
              dplyr::select(.data$mitigator_code, .data$value) |>
              dplyr::filter(!is.na(.data$mitigator_code)) |>
              dplyr::summarise(
                average = mean(.data$value, na.rm = TRUE),
                .by = .data$mitigator_code
              ) |>
              dplyr::arrange(.data$average, .data$mitigator_code) |>
              dplyr::pull(.data$mitigator_code)
          )
        )
    },

    "mitigator_average_desc" = {
      dat <-
        dat |>
        dplyr::mutate(
          mitigator_code = forcats::fct(
            x = .data$mitigator_code,
            levels = dat |>
              dplyr::select(.data$mitigator_code, .data$value) |>
              dplyr::filter(!is.na(.data$mitigator_code)) |>
              dplyr::summarise(
                average = mean(.data$value, na.rm = TRUE),
                .by = .data$mitigator_code
              ) |>
              dplyr::arrange(
                dplyr::desc(.data$average),
                .data$mitigator_code
              ) |>
              dplyr::pull(.data$mitigator_code)
          )
        )
    }
  )

  # order scheme and mitigator names to match codes
  dat |>
    dplyr::mutate(
      # need to reverse ordering for y-axis to ensure correct display in ggplot2
      mitigator_code = .data$mitigator_code |>
        forcats::fct_rev(),

      # ensure context items are listed last
      scheme_code = .data$scheme_code |>
        forcats::fct_expand(scheme_additional) |>
        forcats::fct_relevel(
          "NEE",
          "MIN",
          "MAX",
          "MEAN",
          after = Inf
        ),

      mitigator_code = .data$mitigator_code |>
        forcats::fct_expand(scheme_additional) |>
        forcats::fct_relevel(
          "MEAN",
          "MAX",
          "MIN", # NB, reverse ordering
          after = Inf
        ),

      # order scheme name to match scheme code
      scheme_name = .data$scheme_name |>
        forcats::fct() |>
        stats::reorder(as.numeric(.data$scheme_code)),
      # order mitigator name to match mitigator code
      mitigator_name = .data$mitigator_name |>
        forcats::fct() |>
        stats::reorder(as.numeric(.data$mitigator_code)),
    )
}

#' Plot heatmap
#'
#' Produces a \{plotly\} version of the heatmap showing mitigators on the y-axis
#' and schemes across the x-axis with the selected mitigator value (mid, low,
#' high, submitted) controlling the fill colour.
#'
#' @param dat Tibble of mitigator data as produced by `populate_table()` in
#'   `fct_tabulate.R`
#' @param toggle_mitigator_name Boolean - value from
#'   `input$toggle_mitigator_name` - used to decide what to display on y-axis
#' @param toggle_scale_fill_by_mitigator Boolean - value from
#'   `input$toggle_heatmap_scale_fill_by_mitigator` - used to control whether
#'   the colour is scaled across the whole heatmap (FALSE) or applied across
#'   each mitigator (TRUE)
#' @param values_displayed Character - value from `input$values_displayed` -
#'   describing if values represent either percent of activity mitigated or the
#'   80\% prediction interval - used to label the scales
#' @param heatmap_type Character - value from `input$heatmap_type` - used to
#'   control whether a solid colour is applied (binary) or colour is dependent
#'   on the value
#' @param colour_binary Character - value from `input$heatmap_binary_colour` -
#'   hex colour string to use in the binary plots
#' @param colour_value_low Character - value from
#'   `input$heatmap_value_colour_low` - hex colour string to use in the gradient
#'   fill for low values
#' @param colour_value_high Character - value from
#'   `input$heatmap_value_colour_high` - hex colour string to use in the
#'   gradient fill for high values
#' @param plot_height Integer - value from `ra$heatmap_min_height` - the number
#'   of pixels to set as the height of the plot
#' @param font_family Character - (default = 'Arial, Helvetica, Droid Sans,
#'   sans') a list of font families to use in the plot
#' @param focal_scheme_code Code for the scheme chosen as the focal scheme by
#'   user
#'
#' @returns {plotly} object showing heatmap
plot_heatmap <- function(
  # data
  dat,
  focal_scheme_code,

  # options
  toggle_mitigator_name,
  toggle_scale_fill_by_mitigator,
  values_displayed,
  heatmap_type,

  # formatting
  colour_binary,
  colour_value_low,
  colour_value_high,
  plot_height,
  font_family = "Arial, Helvetica, Droid Sans, sans"
) {
  # preparation ----
  # set up a list of 'context' scheme codes, i.e. are for display purposes
  scheme_additional <- c("NEE", "MIN", "MAX", "MEAN")

  # gather information
  y_max_char <- dat$mitigator_name |>
    as.character() |>
    nchar() |>
    max()

  y_char_wrap <- (y_max_char / 1.8) |>
    ceiling()

  x_scheme_count <- dat$scheme_name |>
    unique() |>
    length()

  y_mitigator_count <- dat$mitigator_code |>
    unique() |>
    length()

  y_mitigator_count_main <- dat |>
    dplyr::select(.data$mitigator_code) |>
    dplyr::distinct() |>
    dplyr::filter(!.data$mitigator_code %in% scheme_additional) |>
    dplyr::pull(.data$mitigator_code) |>
    length()

  # logic ----

  ## separate data from mitigator and scheme summaries
  dat_mitigator <-
    dat |>
    dplyr::filter(.data$scheme_code %in% scheme_additional)

  dat_scheme <-
    dat |>
    dplyr::filter(.data$mitigator_code %in% scheme_additional)

  dat <-
    dat |>
    dplyr::filter(
      !.data$scheme_code %in% scheme_additional,
      !.data$mitigator_code %in% scheme_additional
    )

  x_scheme_count_main <-
    dat$scheme_name |>
    unique() |>
    length()

  ## y-axis ----
  # decide whether to plot the mitigator code or name on the y-xais
  ifelse(
    test = toggle_mitigator_name,
    yes = var_y_axis <- "mitigator_name",
    no = var_y_axis <- "mitigator_code"
  )

  ## fill ----
  # decide whether fill colour applies across mitigators or across whole plot
  ifelse(
    test = toggle_scale_fill_by_mitigator,
    yes = var_fill <- "value_scaled", # mitigator-wise fill
    no = var_fill <- "value" # plot-wise fill
  )

  ## x-axis ----
  # decide how scheme names should be plotted on the x-axis
  if (x_scheme_count < 6) {
    x_scheme_text <- ggplot2::element_text(
      angle = 0,
      hjust = 0.5,
      vjust = 0.5
    )
    x_scheme_wrap <- 10
  } else if (x_scheme_count < 12) {
    x_scheme_text <- ggplot2::element_text(
      angle = 45,
      hjust = 0,
      vjust = -1
    )
    x_scheme_wrap <- 100
  } else {
    x_scheme_text <- ggplot2::element_text(
      angle = 90,
      hjust = 0,
      vjust = -1
    )
    x_scheme_wrap <- 100
  }

  # plotting ----
  heatmap_main <-
    heatmap_base(
      dat = dat,
      var_y_axis = var_y_axis,
      var_fill = var_fill,
      x_scheme_wrap = x_scheme_wrap,
      x_scheme_text = x_scheme_text,
      values_displayed = values_displayed,
      y_char_wrap = y_char_wrap,
      colour_binary = colour_binary,
      colour_value_low = colour_value_low,
      colour_value_high = colour_value_high,
      heatmap_type = heatmap_type,
      plot_height = plot_height,
      font_family = font_family,
      context = FALSE
    )
  # add to a list of heatmaps
  heatmap_list <- list(heatmap_main = heatmap_main)

  if (nrow(dat_mitigator) > 0) {
    heatmap_summary_mit <-
      heatmap_base(
        dat = dat_mitigator,
        var_y_axis = var_y_axis,
        var_fill = var_fill,
        x_scheme_wrap = x_scheme_wrap,
        x_scheme_text = x_scheme_text,
        values_displayed = values_displayed,
        y_char_wrap = y_char_wrap,
        colour_binary = colour_binary,
        colour_value_low = colour_value_low,
        colour_value_high = colour_value_high,
        heatmap_type = heatmap_type,
        plot_height = plot_height,
        font_family = font_family,
        context = TRUE
      )
    # add to the list of heatmaps
    heatmap_list <- c(
      heatmap_list,
      heatmap_summary_mit = list(heatmap_summary_mit)
    )
  }

  if (nrow(dat_scheme) > 0) {
    heatmap_summary_sch <-
      heatmap_base(
        dat = dat_scheme,
        var_y_axis = var_y_axis,
        var_fill = var_fill,
        x_scheme_wrap = x_scheme_wrap,
        x_scheme_text = x_scheme_text,
        values_displayed = values_displayed,
        y_char_wrap = y_char_wrap,
        colour_binary = colour_binary,
        colour_value_low = colour_value_low,
        colour_value_high = colour_value_high,
        heatmap_type = heatmap_type,
        plot_height = plot_height,
        font_family = font_family,
        include_x_axis = FALSE,
        context = TRUE
      )
    # add to the list of heatmaps
    heatmap_list <- c(
      heatmap_list,
      heatmap_summary_sch = list(heatmap_summary_sch)
    )
  }

  # decide how to subplot the components of the heatmap
  base::switch(
    EXPR = as.character(length(heatmap_list)),

    "1" = {
      # just the main heatmap without summaries
      heatmap <-
        plotly::subplot(
          heatmap_list,
          nrows = 1,
          margin = 0L
        )
    },

    "2" = {
      # the main heatmap and NEE
      heatmap <-
        plotly::subplot(
          heatmap_list,
          nrows = 1,
          margin = 0L,
          widths = c(
            # main heatmap body
            x_scheme_count_main / x_scheme_count,
            # context columns e.g. NEE
            (x_scheme_count - x_scheme_count_main) / x_scheme_count
          ),
          shareY = TRUE
        )
    },

    "3" = {
      # the main heatmap and either / or NEE & aggregate
      heatmap <-
        plotly::subplot(
          heatmap_list,
          nrows = 2,
          margin = 0L,
          widths = c(
            # main heatmap body
            x_scheme_count_main / x_scheme_count,
            # context columns e.g. NEE
            (x_scheme_count - x_scheme_count_main) / x_scheme_count
          ),
          heights = c(
            # main heatmap body
            y_mitigator_count_main / y_mitigator_count,
            # context rows e.g. MIN
            (y_mitigator_count - y_mitigator_count_main) / y_mitigator_count
          ),
          shareY = TRUE
        )
    }
  )

  # do final config to heatmap
  heatmap |>
    plotly::config(
      displaylogo = FALSE,
      modeBarButtons = list(list("toImage")),
      toImageButtonOptions = list(
        "format" = "svg",
        "filename" = glue::glue(
          "nhp_heatmap_", # name for this plot
          "{paste0(focal_scheme_code, collapse = '_')}_", # focal scheme code
          "{strftime(Sys.time(), '%Y%m%d_%H%M%S')}"
        ) # datetime
      )
    )
}


#' Heatmap generator
#'
#' Base function used to generate heatmaps or 'context' adornments such as
#' National Elicitation Exercise (NEE) values and aggregate summaries such as
#' min, max and mean values across schemes and mitigators.
#'
#' @param dat Tibble - mitigator data, filtered to either the 'main' heatmap
#'   data or the 'context' data
#' @param var_y_axis Character - the name of the variable in `dat` to use when
#'   plotting the y-axis
#' @param var_fill Character - the name of the variable in `dat` to use when
#'   deciding the fill colour of the heatmap ('main' heatmaps only)
#' @param x_scheme_wrap Integer - the number of characters to string wrap the
#'   x-axis text on
#' @param x_scheme_text ggplot2::element_text - defining how the x-axis labels
#'   are to be displayed
#' @param y_char_wrap Integer - the number of characters to string wrap the
#'   y-axis text on
#' @param values_displayed Character - value from `input$values_displayed` -
#'   describing if values represent either percent of activity mitigated or the
#'   80\% prediction interval - used to label the scales
#' @param heatmap_type Character - value from `input$heatmap_type` - used to
#'   control whether a solid colour is applied (binary) or colour is dependent
#'   on the value
#' @param colour_binary Character - value from `input$heatmap_binary_colour` -
#'   hex colour string to use in the binary plots
#' @param colour_value_low Character - value from
#'   `input$heatmap_value_colour_low` - hex colour string to use in the gradient
#'   fill for low values
#' @param colour_value_high Character - value from
#'   `input$heatmap_value_colour_high` - hex colour string to use in the
#'   gradient fill for high values
#' @param plot_height Integer - value from `ra$heatmap_min_height` - the number
#'   of pixels to set as the height of the plot
#' @param font_family Character - (default = 'Arial, Helvetica, Droid Sans,
#'   sans') a list of font families to use in the plot
#' @param include_x_axis Boolean (default = TRUE) should the x-axis be included
#'   in the plot
#' @param context Boolean (default = FALSE) is this a context (e.g. NEE, min /
#'   max / mean) plot
#'
#' @returns {plotly} object showing heatmap component - e.g. the main plot, or a
#'   context plot for the x or y-axis.
heatmap_base <- function(
  dat,
  var_y_axis,
  var_fill,
  x_scheme_wrap,
  x_scheme_text,
  y_char_wrap,
  values_displayed,
  heatmap_type,
  colour_binary,
  colour_value_low,
  colour_value_high,
  plot_height,
  font_family,
  include_x_axis = TRUE,
  context = FALSE
) {
  # convert these strings to named objects
  var_y_axis <- as.name(var_y_axis)
  var_fill <- as.name(var_fill)

  # start the ggplot object
  heatmap <-
    dat |>
    ggplot2::ggplot(
      ggplot2::aes(
        x = .data$scheme_name,
        y = {{ var_y_axis }},
        text = .data$tooltip_text
      )
    )

  # set the fill variable (if not a context plot)
  if (!context) {
    heatmap <-
      heatmap +
      ggplot2::aes(fill = {{ var_fill }}, colour = "white") +
      ggplot2::geom_tile(colour = "white") +
      ggplot2::scale_x_discrete(
        position = "top",
        labels = \(x) stringr::str_wrap(x, width = x_scheme_wrap),
        guide = ggplot2::guide_axis(angle = ggplot2::waiver())
      )
  } else {
    # context plots (e.g. NEE, then colour white with grey border)
    heatmap <-
      heatmap +
      ggplot2::geom_tile(fill = "white", colour = "grey80") +
      ggplot2::theme(
        axis.text.x = ggplot2::element_blank()
      )
  }

  # continue to build the heatmap
  heatmap <-
    heatmap +
    ggplot2::scale_y_discrete(
      labels = \(.y) {
        stringr::str_wrap(
          string = .y,
          width = y_char_wrap,
          whitespace_only = TRUE
        )
      }
    ) +
    ggplot2::theme_minimal(base_size = 16) +
    ggplot2::theme(
      axis.title.x = ggplot2::element_blank(),
      axis.title.y = ggplot2::element_blank(),
      axis.text.x = x_scheme_text,
    ) +
    ggplot2::labs(fill = values_displayed) +
    # remove the legend
    ggplot2::theme(legend.position = "none")

  # handle non-'submitted' (non-binary) plots
  if (heatmap_type != "value_binary") {
    heatmap <-
      heatmap +
      ggplot2::geom_text(
        ggplot2::aes(
          label = .data$value |> scales::percent(accuracy = 1)
        ),
        colour = ifelse(context, "black", "white"),
        size = 4
      ) +
      ggplot2::scale_fill_gradient(
        low = colour_value_low,
        high = colour_value_high,
        labels = scales::label_percent()
      )
  }

  # handle 'submitted' (binary) plots
  if (heatmap_type == "value_binary") {
    heatmap <-
      heatmap +
      ggplot2::theme(legend.position = "none") +
      ggplot2::aes(fill = as.character(.data$value), colour = "white") +
      ggplot2::scale_fill_manual(values = c("1" = colour_binary))
  }

  # convert to plotly
  heatmap <-
    heatmap |>
    plotly::ggplotly(tooltip = c("text"), height = plot_height) |>
    plotly::layout(
      font = list(family = font_family),
      xaxis = list(
        side = "top",
        tickfont = list(
          family = font_family,
          size = 15
        )
      ),
      yaxis = list(
        tickfont = list(
          family = font_family,
          size = 14
        )
      ),
      hoverlabel = list(
        font = list(
          family = font_family,
          size = 14
        )
      ),
      margin = list(t = 50, b = 0, l = 0, r = 0)
    )

  if (include_x_axis == FALSE) {
    heatmap <-
      heatmap |>
      plotly::layout(
        xaxis = list(showticklabels = FALSE)
      )
  }

  heatmap
}
