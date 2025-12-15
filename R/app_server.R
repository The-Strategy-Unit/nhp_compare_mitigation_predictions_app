#' The application server-side
#'
#' @param input,output,session Internal parameters for {shiny}.
#'     DO NOT REMOVE.
#' @import shiny
#' @noRd
app_server <- function(input, output, session) {
  # Prepare data ----

  ## Make connections ----

  container_support <-
    get_container(container_name = Sys.getenv("AZ_STORAGE_CONTAINER_SUPPORT"))

  container_inputs <-
    get_container(container_name = Sys.getenv("AZ_STORAGE_CONTAINER_INPUTS"))

  board <- pins::board_connect(server = Sys.getenv("CONNECT_SERVER"))

  ## Read data ----

  # Datasets

  rates_data <- container_inputs |>
    read_provider_data("rates") |>
    dplyr::select(
      procode = .data$provider,
      .data$strategy,
      .data$fyear,
      .data$rate,
      n = .data$denominator
    )

  nee_results <- container_support |> read_nee("nee_table.rds")

  # Lookups

  trust_code_lookup <- get_trust_lookup(container_support = container_support)

  mitigator_lookup <- container_support |>
    AzureStor::storage_read_csv("mitigator-lookup.csv", col_types = "c") |>
    prepare_mitigators()

  mitigator_reference <- mitigator_lookup |> prepare_mitigators_ref()

  peers <- container_support |>
    AzureStor::storage_load_rds("trust-peers.rds") |>
    dplyr::rename(scheme = .data$procode)

  # Metadata
  yaml <- yaml::read_yaml(
    file = here::here("inst", "ref_inputs-golem-config.yml"),
    eval.expr = FALSE
  )
  yaml_df <- get_mitigator_baseline_description(yaml)

  # Parameters
  params <- pins::pin_read(
    board,
    name = "matt.dray/nhp_tagged_runs_params_test"
  )
  runs_meta <- pins::pin_read(
    board,
    name = "matt.dray/nhp_tagged_runs_meta_test"
  )
  extracted_params <- extract_params(
    params,
    runs_meta,
    mitigator_lookup,
    "final_report_ndg2"
  )
  skeleton_table <- prepare_skeleton_table(extracted_params)

  ## Prep data ----
  dat <- populate_table(
    skeleton_table,
    extracted_params,
    trust_code_lookup,
    mitigator_lookup,
    nee_results,
    yaml_df
  )

  all_schemes <- get_all_schemes(dat)

  # Reactive Values
  ra <- shiny::reactiveValues(
    heatmap_min_height = NA,
    pointrange_min_height = NA,
    mixturedist_min_height = NA
  )
  ra$heatmap_min_height <- 100
  ra$pointrange_min_height <- 100
  ra$mixturedist_min_height <- 100

  # Mitigator selections
  mitigator_server <- datamods::select_group_server(
    id = "mitigator_filters",
    data = mitigator_reference,
    vars = c(
      "mitigator_type",
      "activity_type",
      "grouping",
      "strategy_subset",
      "mitigator_name"
    )
  )

  # Bookmarking ----
  # restore some settings using the manual 'onRestored' function because these
  # aren't automatically restored.
  shiny::onRestored(function(state) {
    # schemes ---
    # select bookmarked focal scheme
    shiny::updateSelectInput(
      inputId = "focus_scheme",
      selected = state$input$focus_scheme
    )

    # select bookmarked schemes to visualise
    shiny::updateSelectInput(
      inputId = "schemes",
      selected = state$input$schemes
    )

    # mitigators ---
    # get a formatted list of mitigators
    mitigators_selected <- add_to_selected_mitigators(
      df = mitigator_reference,
      selected_currently = NA,
      new_selections = state$input$mitigators
    )

    # update the selected mitigators list
    shiny::updateSelectizeInput(
      inputId = "mitigators",
      selected = mitigators_selected,
      choices = mitigators_selected,
      server = TRUE
    )
  })

  # Reactives ----

  # ensure dat reflects the user's preferred view
  dat_reactive <- shiny::reactive({
    dat_return <- update_dat_values(
      dat = dat,
      values_displayed = input$values_displayed,
      include_point_estimates = input$include_point_estimates,
      focal_scheme_code = input$focus_scheme
    )

    return(dat_return)
  })

  peer_set <- shiny::reactive({
    peers |>
      dplyr::filter(.data$scheme == input$focus_scheme) |>
      dplyr::pull(.data$peer)
  })

  dat_filtered <- reactive({
    dat_return <- dat_reactive()

    # standardise values to 2041 if requested
    if (input$standardise_2041) {
      dat_return <- dat_return |>
        dplyr::mutate(
          value_lo = forecast_value(
            year_baseline = .data$year_baseline,
            year_horizon = .data$year_horizon,
            year_forecast = 2041,
            value_horizon = .data$value_lo,
            value_displayed = input$values_displayed
          ),
          value_hi = forecast_value(
            year_baseline = .data$year_baseline,
            year_horizon = .data$year_horizon,
            year_forecast = 2041,
            value_horizon = .data$value_hi,
            value_displayed = input$values_displayed
          ),
          value_mid = (
            .data$value_hi - ((.data$value_hi - .data$value_lo) / 2) # fmt: skip
          ) |>
            round(digits = 3)
        )
    }

    return(dat_return)
  })

  ## dat_selected_pointrange ----
  dat_selected_pointrange <- shiny::reactive({
    # ensure at least one selection is made for scheme and mitigator
    shiny::validate(
      shiny::need(
        input$schemes,
        message = "Select at least one scheme from the sidebar."
      )
    )

    shiny::validate(
      shiny::need(
        input$mitigators,
        message = "Select at least one TMPA from the sidebar."
      )
    )

    # get filtered data
    dat <- dat_filtered()

    # plot scheme names in reverse order so displayed correctly in ggplot
    if (!input$toggle_invert_facets) {
      dat <- dat |>
        dplyr::mutate(scheme_name = forcats::fct_rev(.data$scheme_name))
    }

    # plot mitigators in order
    if (input$toggle_invert_facets) {
      dat <- dat |>
        dplyr::mutate(
          mitigator_code = forcats::fct_rev(.data$mitigator_code),
          mitigator_name = stats::reorder(
            .data$mitigator_name,
            as.numeric(.data$mitigator_code)
          ) # order mitigator_name to match mitigator_code
        )
    }

    # further filtering
    dat <- dat |>
      dplyr::filter(
        .data$scheme_code %in% input$schemes,
        .data$mitigator_code %in% input$mitigators
      )

    # show an aggregate summary where requested
    if (input$toggle_aggregate_summary) {
      # summarise all-but-the-focal scheme
      if (input$toggle_aggregate_summary_minmaxrange) {
        # use the full min/max range
        dat_summary <-
          dat |>
          dplyr::filter(
            !.data$scheme_code %in% input$focus_scheme, # exclude focal scheme
            !is.na(.data$value_mid) # need at least a mid-point
          ) |>
          dplyr::summarise(
            scheme_code = "All",
            scheme_name = "Summary",
            value_mid = mean(.data$value_mid, na.rm = TRUE),
            value_lo = min(.data$value_lo, na.rm = TRUE),
            value_hi = max(.data$value_hi, na.rm = TRUE),
            .by = c(.data$mitigator_code, .data$mitigator_name)
          )
      } else {
        # use average values
        dat_summary <-
          dat |>
          dplyr::filter(
            !.data$scheme_code %in% input$focus_scheme, # exclude focal scheme
            !is.na(.data$value_mid) # need at least a mid-point
          ) |>
          dplyr::summarise(
            scheme_code = "All",
            scheme_name = "Summary",
            value_mid = mean(.data$value_mid, na.rm = TRUE),
            value_lo = mean(.data$value_lo, na.rm = TRUE),
            value_hi = mean(.data$value_hi, na.rm = TRUE),
            .by = c(.data$mitigator_code, .data$mitigator_name)
          )
      }

      # add in nee data
      dat_summary <-
        dat_summary |>
        dplyr::left_join(
          y = dat |>
            dplyr::select(
              .data$mitigator_code,
              .data$nee_mean,
              .data$nee_p90,
              .data$nee_p10
            ) |>
            dplyr::distinct(),
          by = "mitigator_code"
        )

      # add summary to dat as a new row
      dat <- dplyr::bind_rows(
        dat,
        dat_summary
      ) |>
        # sort schemes by name alphabetically and the summary placed last
        dplyr::mutate(
          scheme_name = .data$scheme_name |>
            base::factor() |>
            forcats::fct_relevel("Summary", after = Inf) |>
            forcats::fct_rev(),

          # sort scheme codes to match scheme names
          scheme_code = .data$scheme_code |>
            base::factor(
              levels = unique(.data$scheme_code[order(.data$scheme_name)])
            )
        )
    }

    # add colour coding
    dat <-
      dat |>
      dplyr::mutate(
        point_colour = dplyr::case_when(
          .data$scheme_code == input$focus_scheme ~ "red",
          .data$scheme_code == "All" ~ "blue",
          .default = "black"
        )
      )

    # return the result
    return(dat)
  })

  ## dat_selected_heatmap ----
  dat_selected_heatmap <- shiny::reactive({
    shiny::validate(
      shiny::need(
        input$schemes,
        message = "Select at least one scheme from the sidebar."
      )
    )

    shiny::validate(
      shiny::need(
        input$mitigators,
        message = "Select at least one TMPA from the sidebar."
      )
    )

    dat <-
      dat_filtered() |>
      prepare_heatmap_dat(
        dat = _,
        mitigator_codes = input$mitigators,
        scheme_codes = input$schemes,
        focal_scheme_code = input$focus_scheme,
        heatmap_type = input$heatmap_type,
        scheme_order = input$heatmap_scheme_order,
        mitigator_order = input$heatmap_mitigator_order,
        values_displayed = input$values_displayed,
        toggle_heatmap_nee = input$toggle_heatmap_nee,
        toggle_heatmap_aggregate_summaries = input$toggle_heatmap_aggregate_summaries,
        toggle_heatmap_scheme_adornments = input$toggle_heatmap_scheme_adornments
      )

    return(dat)
  })

  dat_selected_mixture_distributions <- shiny::reactive({
    shiny::validate(
      shiny::need(
        input$schemes,
        message = "Select at least one scheme from the sidebar."
      )
    )

    shiny::validate(
      shiny::need(
        input$mitigators,
        message = "Select at least one TMPA from the sidebar."
      )
    )

    dat <- dat_filtered() |>
      dplyr::filter(
        .data$mitigator_code %in% input$mitigators
      )

    dat <- get_mixture_distributions_dat(dat = dat)
  })

  # Observers ----

  ## Updates ----

  shiny::observe({
    shiny::updateSelectInput(
      session,
      "focus_scheme",
      choices = all_schemes,
      selected = "RWG" # default because it has several peers
    )
  })

  shiny::observe({
    scheme_and_peers <- c(input$focus_scheme, peer_set())
    selected_schemes <- all_schemes[which(all_schemes %in% scheme_and_peers)] |>
      tibble::enframe() |>
      dplyr::arrange(.data$name) |> # sort on name, not code
      tibble::deframe()

    if (input$toggle_all_schemes) {
      selected_schemes <- all_schemes
    }

    shiny::updateSelectInput(
      session,
      "schemes",
      choices = all_schemes,
      selected = selected_schemes
    )
  })

  # indicate how many mitigators are available for selection
  shiny::observe({
    # how many mitigators are available
    n_mit <- mitigator_server() |> nrow()

    # update the button indicating how many mitigators are in scope
    shiny::updateActionButton(
      inputId = "mitigators_add_to_selected",
      label = glue::glue("Add to selected [{n_mit}]"),
      icon = shiny::icon("arrow-down", lib = "font-awesome")
    )
  })

  # compile a list of selected mitigators
  mitigators_selected <- shiny::reactive({
    add_to_selected_mitigators(
      df = mitigator_reference,
      selected_currently = input$mitigators,
      new_selections = mitigator_server()$mitigator_code
    )
  })

  # start with a default mitigator, should agree with select_group_ui()
  shiny::observe({
    shiny::updateSelectizeInput(
      inputId = "mitigators",
      selected = mitigators_selected(),
      choices = mitigators_selected(),
    )
  })

  # as above on button-based trigger
  shiny::observeEvent(input$mitigators_add_to_selected, {
    shiny::updateSelectizeInput(
      inputId = "mitigators",
      selected = mitigators_selected(),
      choices = mitigators_selected()
    )
  })

  # removing all mitigator selections
  shiny::observeEvent(input$clear_selected_mitigators, {
    shinyjs::reset("mitigators")
  })

  ## Enablers ----

  shiny::observe({
    if (input$heatmap_type == "value_binary") {
      # enable

      # disable
      shinyjs::disable("toggle_horizon_heatmap")
      shinyjs::disable("toggle_heatmap_scale_fill_by_mitigator")
      shinyjs::disable("toggle_heatmap_nee")
      shinyjs::disable("toggle_heatmap_aggregate_summaries")

      # show
      shinyjs::show("heatmap_binary_colour")

      # hide
      shinyjs::hide("heatmap_value_colour_low")
      shinyjs::hide("heatmap_value_colour_high")
    }

    if (input$heatmap_type != "value_binary") {
      # enable
      shinyjs::enable("toggle_horizon_heatmap")
      shinyjs::enable("toggle_heatmap_scale_fill_by_mitigator")
      shinyjs::enable("toggle_heatmap_nee")
      shinyjs::enable("toggle_heatmap_aggregate_summaries")

      # disable

      # show
      shinyjs::show("heatmap_value_colour_low")
      shinyjs::show("heatmap_value_colour_high")

      # hide
      shinyjs::hide("heatmap_binary_colour")
    }

    # disable 'range of selected schemes' switch if 'summary of selected
    # schemes' is disabled
    if (input$toggle_aggregate_summary) {
      shinyjs::enable("toggle_aggregate_summary_minmaxrange")
    } else {
      shinyjs::disable("toggle_aggregate_summary_minmaxrange")
    }
  })

  # Renders ----

  ## Plots ----

  ### pointrange ----

  # adjust the 'number of charts per row' (number of facet columns) slider to
  # match the number of facets i.e. so can select up to a minimum of one facet
  # per row or list all on a single row
  shiny::observe({
    dat <- dat_selected_pointrange()

    # what is the maximum number of columns to facet over?
    max_facet_cols <- if (input$toggle_invert_facets) {
      # count the number of selected schemes
      dat |>
        dplyr::filter(.data$scheme_code %in% input$schemes) |>
        dplyr::pull(.data$scheme_code) |>
        unique() |>
        length()
    } else {
      # count the number of selected mitigators
      dat |>
        dplyr::filter(.data$mitigator_code %in% input$mitigators) |>
        dplyr::pull(.data$mitigator_code) |>
        unique() |>
        length()
    }

    # update the ui
    shiny::updateSliderInput(
      inputId = "facet_columns",
      max = max_facet_cols
    )
  })

  # adjust pointrange height in response to the number of facet rows
  # this will be base_height + (y_rows * facet_rows)
  shiny::observe({
    dat <- dat_selected_pointrange()

    # how many columns are specified?
    facet_cols <- input$facet_columns

    # how many mitigators are shown
    n_mitigators <- dat |>
      dplyr::filter(.data$mitigator_code %in% input$mitigators) |>
      dplyr::pull(.data$mitigator_code) |>
      unique() |>
      length()

    # how many schemes are shown
    n_schemes <- dat |>
      dplyr::filter(.data$scheme_code %in% input$schemes) |>
      dplyr::pull(.data$scheme_code) |>
      unique() |>
      length()

    # determine the number of rows and facet rows
    if (input$toggle_invert_facets) {
      # facetted on scheme with mitigator in the y-axis

      n_rows <- n_mitigators
      n_facets <- n_schemes
    } else {
      # facetted on mitigator with schemes in the y-axis

      n_rows <- n_schemes
      n_facets <- n_mitigators
    }

    # estimate facet rows
    n_facet_rows <- ceiling(n_facets / facet_cols)

    # specify the height for the pointrange plot
    base_height <- 150

    # update reactive values
    # update the min_height to base + pro-rata for each row and facet row
    ra$pointrange_min_height <- base_height +
      (n_facet_rows * n_rows * 35) + # add room for each row on each facet
      (n_facet_rows * 100) # add room for facet labels on each row
  })

  # wrap the plot call in an observer to enable the dynamic height setting
  shiny::observe({
    output$pointrange <- shiny::renderPlot(
      {
        shiny::validate(
          shiny::need(
            nrow(dat_selected_pointrange()) > 0,
            message = "Insufficient data for this plot."
          )
        )

        dat_selected_pointrange() |> plot_pointrange(input)
      },
      height = ra$pointrange_min_height
    )
  })

  ### heatmap ----

  # adjust heatmap height in response to the number of mitigators to display
  shiny::observe({
    dat <- dat_selected_heatmap()

    # count the number of selected mitigators
    temp_mitigator_count <- dat |>
      dplyr::filter(.data$mitigator_code %in% input$mitigators) |>
      dplyr::pull(.data$mitigator_code) |>
      unique() |>
      length()

    # count the number of selected schemes
    temp_scheme_count <- dat |>
      dplyr::filter(.data$scheme_code %in% input$schemes) |>
      dplyr::pull(.data$scheme_code) |>
      unique() |>
      length()

    # decide the base height (the space taken up by scheme names and legend)
    # NB, scheme names are rotated as the scheme count increases to fit them
    # on screen, so they take up more vertical space
    if (temp_scheme_count < 6) {
      base_height <- 100
    } else if (temp_scheme_count < 12) {
      base_height <- 200
    } else {
      if (input$toggle_heatmap_scheme_adornments) {
        base_height <- 600
      } else {
        base_height <- 300
      }
    }

    # update reactive values
    # update the min_height to base + pro-rata for each mitigator
    ra$heatmap_min_height <- base_height + (temp_mitigator_count * 55)
  })

  # produce the heatmap plot
  output$heatmap <- plotly::renderPlotly({
    shiny::validate(
      shiny::need(
        nrow(dat_selected_heatmap()) > 0,
        message = "Insufficient data for this plot."
      )
    )

    dat_selected_heatmap() |>
      plot_heatmap(
        # data
        focal_scheme_code = input$focus_scheme,

        # options
        toggle_mitigator_name = input$toggle_mitigator_name,
        toggle_scale_fill_by_mitigator = input$toggle_heatmap_scale_fill_by_mitigator,
        values_displayed = input$values_displayed,
        heatmap_type = input$heatmap_type,

        # formatting
        colour_binary = input$heatmap_binary_colour,
        colour_value_low = input$heatmap_value_colour_low,
        colour_value_high = input$heatmap_value_colour_high,
        plot_height = ra$heatmap_min_height
      )
  })

  ### density functions ----
  # adjust distribution height in response to number of mitigators to display
  shiny::observe({
    dat <- dat_selected_heatmap()

    # count the number of selected mitigators
    temp_mitigator_count <- dat |>
      dplyr::filter(.data$mitigator_code %in% input$mitigators) |>
      dplyr::pull(.data$mitigator_code) |>
      unique() |>
      length()

    # update reactive values
    # update the min_height to base + pro-rata for each mitigator
    ra$mixturedist_min_height <- (temp_mitigator_count * 200)
  })

  # wrap the plot call in an observer to enable the dynamic height setting
  shiny::observe({
    output$mixture_distributions <- shiny::renderPlot(
      {
        plot_mixture_distributions(
          dat_selected_mixture_distributions = dat_selected_mixture_distributions(),
          dat_filtered = dat_filtered(),
          dat_focal_scheme_code = input$focus_scheme,
          input = input
        )
      },
      height = ra$mixturedist_min_height
    )
  })

  ### contextual baseline -----

  output$contextual_baseline <- plotly::renderPlotly({
    # notify user if no data is available
    shiny::validate(
      shiny::need(
        input$schemes,
        message = "Select at least one scheme from the sidebar."
      )
    )

    shiny::validate(
      shiny::need(input$mitigators, message = "Select at least one mitigator.")
    )

    # plot the baseline contextual
    dat_filtered() |>
      plot_baseline_comparison(
        rates_data = rates_data,
        mitigator_codes = input$mitigators,
        focal_scheme_code = input$focus_scheme,
        rate_title = "Baseline rate",
        value_title = input$values_displayed,
        trendline = FALSE, # trendlines don't seem to work
        range = input$toggle_contextual_baseline_range,
        scheme_label = input$toggle_contextual_baseline_schemecode,
        quadrants = input$toggle_contextual_baseline_quadrants,
        facet_columns = 1, # not enabling, feel a row per mitigator is best
        facet_height_px = input$slider_contextual_baseline_height
      )
  })

  ### contextual trendline ----
  output$contextual_trendline <- plotly::renderPlotly({
    # notify user if no data is available
    shiny::validate(
      shiny::need(
        input$schemes,
        message = "Select at least one scheme from the sidebar."
      )
    )

    shiny::validate(
      shiny::need(
        input$mitigators,
        message = "Select at least one TMPA from the sidebar."
      )
    )

    # plot the trendline contextual
    dat_filtered() |>
      dplyr::filter(.data$mitigator_code %in% input$mitigators) |>
      plot_faceted_trendlines(
        rates_data = rates_data,
        mitigator_codes = input$mitigators,
        focal_scheme_code = input$focus_scheme,
        scheme_codes = input$schemes,
        show_other_schemes = input$toggle_contextual_trendline_otherschemes,
        show_horizon_timeline = input$toggle_contextual_trendline_horizon_timeline,
        show_horizon_overlay = input$toggle_contextual_trendline_horizon_overlay,
        show_prebaseline_average = input$toggle_contextual_trendline_average,
        facet_height_px = input$slider_contextual_trendline_height
      )
  })

  ## Tables ----

  output$raw_data_dt <- DT::renderDT(
    {
      # NB, using the original dat tibble as want to present the un-edited data
      dat |> make_raw_dt()
    },
    server = FALSE
  ) # to download all data via CSV button

  output$mitigator_lookup_dt <- DT::renderDT(
    {
      mitigator_lookup |> make_mitigator_dt()
    },
    server = FALSE
  ) # to download all data via CSV button

  output$scheme_lookup_dt <- DT::renderDT(
    {
      trust_code_lookup |> make_scheme_dt()
    },
    server = FALSE
  ) # to download all data via CSV button

  ### mitigator uptake ----
  output$mitigator_uptake_dt <- DT::renderDT({
    make_mitigator_uptake_dt(
      dat = dat_reactive(),
      selected_schemes = input$schemes
    )
  })

  ### scheme uptake ----
  output$scheme_uptake_dt <- DT::renderDT({
    make_scheme_uptake_dt(
      dat = dat_reactive(),
      selected_mitigators = input$mitigators,
      selected_schemes = input$schemes,
      focal_scheme = input$focus_scheme
    )
  })

  ## Downloads ----

  ### point-range ----
  output$point_range_download_data <- shiny::downloadHandler(
    filename = function() {
      glue::glue(
        "nhp_pointrange_data_", # name for this plot
        "{strftime(Sys.time(), '%Y%m%d_%H%M%S')}.csv"
      ) # datetime
    },
    content = function(file) {
      readr::write_csv(
        x = dat_selected_pointrange(),
        file = file,
        na = "",
        quote = "needed"
      )
    },
    contentType = "text/csv"
  )

  output$point_range_download_plot <- shiny::downloadHandler(
    filename = function() {
      glue::glue(
        "nhp_pointrange_plot_", # name for this plot
        "{strftime(Sys.time(), '%Y%m%d_%H%M%S')}.svg"
      ) # datetime
    },
    content = function(file) {
      ggplot2::ggsave(
        filename = file,
        plot = dat_selected_pointrange() |> plot_pointrange(input),
        scale = 4,
        device = "svg",
        units = "px",
        height = ra$pointrange_min_height,
        width = 1100,
        limitsize = FALSE
      )
    }
  )

  ### mixture distributions ----
  output$mixture_distributions_download_data <- shiny::downloadHandler(
    filename = function() {
      glue::glue(
        "nhp_mixture_distribution_data_", # name for this plot
        "{strftime(Sys.time(), '%Y%m%d_%H%M%S')}.csv"
      ) # datetime
    },
    content = function(file) {
      readr::write_csv(
        x = dat_selected_mixture_distributions(),
        file = file,
        na = "",
        quote = "needed"
      )
    },
    contentType = "text/csv"
  )

  output$mixture_distributions_download_plot <- shiny::downloadHandler(
    filename = function() {
      glue::glue(
        "nhp_mixture_distribution_plot_", # name for this plot
        "{strftime(Sys.time(), '%Y%m%d_%H%M%S')}.svg"
      ) # datetime
    },
    content = function(file) {
      ggplot2::ggsave(
        filename = file,
        plot = plot_mixture_distributions(
          dat_selected_mixture_distributions = dat_selected_mixture_distributions(),
          dat_filtered = dat_filtered(),
          dat_focal_scheme_code = input$focus_scheme,
          input = input
        ),
        scale = 4,
        device = "svg",
        units = "px",
        height = ra$mixturedist_min_height,
        width = 900,
        limitsize = FALSE
      )
    }
  )

  ### heatmaps ----
  output$heatmaps_download_data <- shiny::downloadHandler(
    filename = function() {
      glue::glue(
        "nhp_heatmap_data_", # name for this plot
        "{strftime(Sys.time(), '%Y%m%d_%H%M%S')}.csv"
      ) # datetime
    },
    content = function(file) {
      readr::write_csv(
        x = dat_selected_heatmap(),
        file = file,
        na = "",
        quote = "needed"
      )
    },
    contentType = "text/csv"
  )

  output$heatmaps_download_plot <- shiny::downloadHandler(
    filename = function() {
      glue::glue(
        "nhp_heatmap_plot_", # name for this plot
        "{strftime(Sys.time(), '%Y%m%d_%H%M%S')}.html"
      ) # datetime
    },
    content = function(file) {
      htmlwidgets::saveWidget(
        widget = dat_selected_heatmap() |>
          plot_heatmap(
            # data
            focal_scheme_code = input$focus_scheme,

            # options
            toggle_mitigator_name = input$toggle_mitigator_name,
            toggle_scale_fill_by_mitigator = input$toggle_heatmap_scale_fill_by_mitigator,
            values_displayed = input$values_displayed,
            heatmap_type = input$heatmap_type,

            # formatting
            colour_binary = input$heatmap_binary_colour,
            colour_value_low = input$heatmap_value_colour_low,
            colour_value_high = input$heatmap_value_colour_high,
            plot_height = ra$heatmap_min_height
          ) |>
          plotly::partial_bundle(minified = TRUE),
        file = file,
        selfcontained = TRUE,
        title = "NHP Heatmap Plot"
      )
    }
  )

  ### mitigator coverage ----
  output$mitigator_coverage_download_data <- shiny::downloadHandler(
    filename = function() {
      glue::glue(
        "nhp_mitigator_coverage_data_", # name for this plot
        "{strftime(Sys.time(), '%Y%m%d_%H%M%S')}.csv"
      ) # datetime
    },
    content = function(file) {
      readr::write_csv(
        x = make_mitigator_uptake_dat(
          dat = dat_reactive(),
          selected_schemes = input$schemes
        ),
        file = file,
        na = "",
        quote = "needed"
      )
    },
    contentType = "text/csv"
  )

  ### scheme coverage ----
  output$scheme_coverage_download_data <- shiny::downloadHandler(
    filename = function() {
      glue::glue(
        "nhp_scheme_coverage_data_", # name for this plot
        "{strftime(Sys.time(), '%Y%m%d_%H%M%S')}.csv"
      ) # datetime
    },
    content = function(file) {
      readr::write_csv(
        x = make_scheme_uptake_dat(
          dat = dat_reactive(),
          selected_mitigators = input$mitigators,
          selected_schemes = input$schemes,
          focal_scheme = input$focus_scheme
        ),
        file = file,
        na = "",
        quote = "needed"
      )
    },
    contentType = "text/csv"
  )

  ### context baseline ----
  output$context_baseline_download_data <- shiny::downloadHandler(
    filename = function() {
      glue::glue(
        "nhp_context_baseline_data_", # name for this plot
        "{strftime(Sys.time(), '%Y%m%d_%H%M%S')}.csv"
      ) # datetime
    },
    content = function(file) {
      readr::write_csv(
        x = dat_filtered() |>
          prep_baseline_comparison(
            rates_data = rates_data,
            mitigator_codes = input$mitigators,
            focal_scheme_code = input$focus_scheme,
            rate_title = "Baseline rate",
            value_title = input$values_displayed,
            trendline = FALSE, # trendlines don't seem to work
            range = input$toggle_contextual_baseline_range,
            scheme_label = input$toggle_contextual_baseline_schemecode,
            quadrants = input$toggle_contextual_baseline_quadrants,
            facet_columns = 1, # not enabling, feel a row per mitigator is best
            facet_height_px = input$slider_contextual_baseline_height
          ),
        file = file,
        na = "",
        quote = "needed"
      )
    },
    contentType = "text/csv"
  )

  output$context_baseline_download_plot <- shiny::downloadHandler(
    filename = function() {
      glue::glue(
        "nhp_context_baseline_plot_", # name for this plot
        "{strftime(Sys.time(), '%Y%m%d_%H%M%S')}.html"
      ) # datetime
    },
    content = function(file) {
      htmlwidgets::saveWidget(
        widget = dat_filtered() |>
          plot_baseline_comparison(
            rates_data = rates_data,
            mitigator_codes = input$mitigators,
            focal_scheme_code = input$focus_scheme,
            rate_title = "Baseline rate",
            value_title = input$values_displayed,
            trendline = FALSE, # trendlines don't seem to work
            range = input$toggle_contextual_baseline_range,
            scheme_label = input$toggle_contextual_baseline_schemecode,
            quadrants = input$toggle_contextual_baseline_quadrants,
            facet_columns = 1, # not enabling, feel a row per mitigator is best
            facet_height_px = input$slider_contextual_baseline_height
          ) |>
          plotly::partial_bundle(minified = TRUE),
        file = file,
        selfcontained = TRUE,
        title = "NHP Baseline Plot"
      )
    }
  )

  ### context trendline ----
  output$context_trendline_download_data <- shiny::downloadHandler(
    filename = function() {
      glue::glue(
        "nhp_context_trendline_data_", # name for this plot
        "{strftime(Sys.time(), '%Y%m%d_%H%M%S')}.csv"
      ) # datetime
    },

    content = function(file) {
      df_list <-
        dat_filtered() |>
        dplyr::filter(.data$mitigator_code %in% input$mitigators) |>
        plot_faceted_trendlines(
          rates_data = rates_data,
          mitigator_codes = input$mitigators,
          focal_scheme_code = input$focus_scheme,
          scheme_codes = input$schemes,
          show_other_schemes = input$toggle_contextual_trendline_otherschemes,
          show_horizon_timeline = input$toggle_contextual_trendline_horizon_timeline,
          show_horizon_overlay = input$toggle_contextual_trendline_horizon_overlay,
          show_prebaseline_average = input$toggle_contextual_trendline_average,
          facet_height_px = input$slider_contextual_trendline_height,
          return_data = TRUE
        )

      readr::write_csv(
        x = df_list$plot_data,
        file = file,
        na = "",
        quote = "needed"
      )
    },
    contentType = "text/csv"
  )

  output$context_trendline_download_plot <- shiny::downloadHandler(
    filename = function() {
      glue::glue(
        "nhp_context_trendline_plot_", # name for this plot
        "{strftime(Sys.time(), '%Y%m%d_%H%M%S')}.html"
      ) # datetime
    },
    content = function(file) {
      plot_widget <-
        dat_filtered() |>
        dplyr::filter(.data$mitigator_code %in% input$mitigators) |>
        plot_faceted_trendlines(
          rates_data = rates_data,
          mitigator_codes = input$mitigators,
          focal_scheme_code = input$focus_scheme,
          scheme_codes = input$schemes,
          show_other_schemes = input$toggle_contextual_trendline_otherschemes,
          show_horizon_timeline = input$toggle_contextual_trendline_horizon_timeline,
          show_horizon_overlay = input$toggle_contextual_trendline_horizon_overlay,
          show_prebaseline_average = input$toggle_contextual_trendline_average,
          facet_height_px = input$slider_contextual_trendline_height
        ) |>
        plotly::partial_bundle(minified = TRUE)

      htmlwidgets::saveWidget(
        widget = plot_widget,
        file = file,
        selfcontained = TRUE
      )
    }
  )
}
