#### Purpose

Use this section to ask:

> "How does my prediction compare to other schemes' for this type of activity?"

Heatmaps show values in a 2D grid of cells by scheme and Types of Potentially Mitigatable Activity (TPMAs). The colour of cells indicates the value shown.

Use these for an at-a-glance sense of how activity estimates vary by scheme and across TPMAs.

#### Structure

TPMAs are listed on the y-axis and schemes are spread across the x-axis.

Values are shown as cells at intersections between scheme and TPMA and represented by a colour, often with a percentage value too.

Empty spaces indicate where a scheme has not set a value for a TPMA.

#### Data Availability

Note that schemes and TPMAs will not be plotted if there is no data to show.
Schemes may have chosen not to set a TPMA, or a TPMA [may not have been available](https://connect.strategyunitwm.nhs.uk/nhp/project_information/user_guide/mitigators_lookup.html) when developing a scenario.

#### Settings

##### Value type

Select a value from the drop-down list to be displayed in the heatmap. The default option is 'Midpoint', with options for:

* 'Range' - the difference the low and high values of the selected 80% prediction interval/activity mitigated
* 'Low' - the 10% value
* 'High' - the 90% value
* 'Submitted' - binary value indicating where a response has or has not been provided by schemes

##### Standardise by horizon?

The 80% prediction-interval ranges are estimates for reduced activity by a chosen horizon year ("How much of this type of activity will we still be providing in the horizon year?". 
The percent of activity mitigated are the inverse - an estimate for how much activity is mitigated against by the chosen horizon year ("How much of this activity that we provided in the baseline year will we no longer be providing by the horizon year?").

Schemes have chosen different horizon years, which may explain some of the variation between schemes.

To compare the change **per year** you can enable this option to better compare schemes.

##### Show TPMA names

Enable this option to switch between TPMA names and the TPMA codes.

Using TPMA codes is useful if horizontal space is limited.

##### Set colour within TPMA?

This option controls how the colours in the heatmap are produced.

Enable this option to colour values by TPMA. This means the full range of colours is used for each TPMA, highlighting which schemes have provided the lowest and highest values for each TPMA.

Disable this option to colour values across the whole heatmap. This highlights the lowest and highest value across the range of schemes and TPMAs.

##### Add NEE?

This options controls the visibility of [the National Elicitation Exercise (NEE)](https://doi.org/10.1136/bmjopen-2024-084632) mitigator summary column.

Enable this option to see the relevant NEE value column (midpoint, low, high or interval) as context to the TPMA values in the heatmap.

##### Add aggregate summaries?

This option controls the visibility of aggregate summaries (minimum, maximum and mean) for the TPMAs (shown as additional columns) and schemes (shown as additional rows).

Enable this option to include the minimum, maximum and average values on the heatmap plot which is useful when exporting to image - where the tooltip context information is lost.

##### Add run information to scheme name?

This option controls whether the scheme name includes additional details, useful when exporting the plot as a static image and the tooltip context information is lost.

Enable this option to extend the scheme names to include scheme code, run stage and years for baseline and horizon.

##### Order schemes by

Select a value from the drop-down list for how schemes are ordered on the x-axis. The default option is 'Number of TPMAs (desc)', with options for:

* 'Scheme name (asc/desc)' - order schemes alphabetically by their names
* 'Number of TPMAs (asc/desc)' - order schemes by the number of TPMAs they have used
* 'Average mitigation (asc/desc)' - order schemes by their average values set for the TPMAs

##### Order TPMAs by

Select a value from the drop-down list for how TPMAs are ordered on the y-axis. The default option is 'Number of schemes (desc)', with options for:

* 'TPMA name (asc/desc)' - order TPMAs alphabetically by their names
* 'Number of schemes (asc/desc)' - order TPMAs by the number of schemes that have set values for it
* 'Average mitigation (asc/desc)' - order TPMAs by their average values set for them by schemes

##### Colour for 'submitted' plot

The colour to use for the binary 'submitted' heatmap types. Either click into the cell and choose a colour from the palette or specify a hexadecimal colour code.

##### Colour for low values

The colour to use for the extreme low values in the colour gradient heatmaps. Either click into the cell and choose a colour from the palette or specify a hexadecimal colour code.

##### Colour for high values

The colour to use for the extreme high values in the colour gradient heatmaps. Either click into the cell and choose a colour from the palette or specify a hexadecimal colour code.
