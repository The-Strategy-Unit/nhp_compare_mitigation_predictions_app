# Compare NHP Activity Mitigation Predictions

## Purpose

Compare predictions of potentially-mitigatable activity by users of the New Hospital Programme (NHP) [demand model](https://github.com/the-Strategy-Unit/nhp_project_information).
Intended primarily to help schemes refine and finalise their scenarios.

The app is [deployed to Posit Connect](https://connect.strategyunitwm.nhs.uk/nhp/mitigator-comparisons/) (login/permissions required).

## For developers

### Tools

The app is built primarily with the R packages [{shiny}](https://shiny.posit.co/), [{golem}](https://thinkr-open.github.io/golem/) and [{bslib}](https://rstudio.github.io/bslib/).

### Run the app locally

#### Install packages

You must ensure you have installed the packages listed in the `DESCRIPTION`.
These can be installed with `devtools::install_deps(dependencies = TRUE)`.
This repo doesn't use {renv}.

#### Add environmental variables

You must set some environmental variables before you can run the app locally on your machine.
To do this, add an `.Renviron` file to the project directory that contains the variables named in the `.Renviron.example` file.
You can ask a member of the Data Science team to help populate this file.
Note that you don't technically need to provide the `CONNECT_API_KEY` variable if you're using RStudio and have already [configured your publishing account](https://docs.posit.co/connect/user/publishing-rstudio/).

Remember to restart your session or run `readRenviron(".Renviron")` after you've updated your `.Renviron` file.
If you're having authorisation issues (e.g. a 403 is being returned), try clearing your tokens with `AzureAuth::clean_token_directory()` and try again.

#### Run the app

Source the `dev/run_dev.R` script to run the app, assuming you've installed the packages and set up the environment variables.

### Deploy

You can redeploy the app to Posit Connect using the `dev/deploy.R` script.
There are two versions of the app you can deploy to:

* ['dev' for developers](https://connect.strategyunitwm.nhs.uk/nhp/compare-mitigation-predictions-dev/) (login/permissions required), which you can deploy to after pull requests to check any changes
* ['prod' for users](https://connect.strategyunitwm.nhs.uk/nhp/compare-mitigation-predictions/) (login/permissions required), which you can deploy to after a new GitHub release/Git tag

### Data

#### Parameters

There is a system to pre-prepare user-selected parameters ('params') data for quick access by the app.
[A Quarto document on Posit Connect](https://connect.strategyunitwm.nhs.uk/nhp/tagged-runs-params-report/) runs on schedule to read the params and [save them as an RDS file to a pin](https://connect.strategyunitwm.nhs.uk/content/32c7f642-e420-448d-b888-bf655fc8fa8b/) on Posit Connect.
It also saves [a CSV file to another pin](https://connect.strategyunitwm.nhs.uk/content/811dbaf9-18fe-43aa-bf8e-06b0df66004e/) that contains metadata about the model scenarios.
The app then reads data from these pins using [the {pins} package](https://pins.rstudio.com/).
A [blogpost by the Data Science team](https://the-strategy-unit.github.io/data_science/blogs/posts/2024-05-22-storing-data-safely/#posit-connect-pins) contains a note on authenticating RStudio with Posit Connect, should you need to.

Schemes run many scenarios, but the app only displays data from a single scenario, preferably the one used to compile their outputs ('final') report.
MRMs tell the Data Science team which particular results file should be labelled manually on Azure with the 'run stage' metadata of 'final' (and possibly 'intermediate' or 'initial').
There's [a handy lookup table](https://connect.strategyunitwm.nhs.uk/nhp/tagged_runs/nhp-tagged-runs.html) (login/permissions required) where you can see the scenario files that have been given run-stage metadata.

#### Supporting

Supporting data is fetched from a specific Azure container.
This includes lookups for schemes and for types of potentially-mitigatable activity, baseline trend data, and data from [the National Elicitation Exercise (NEE)](https://doi.org/10.1136/bmjopen-2024-084632).
