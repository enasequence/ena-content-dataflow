import argparse
import sys
import os

"""
Commands to generate input:
   - Sequences : curl -X POST -H "Content-Type: application/x-www-form-urlencoded" -d 'result=sequence&query=tax_tree(2697049)&fields=country,collection_date,first_public&format=tsv&limit=0' "https://www.ebi.ac.uk/ena/portal/api/search" > sequence.custom_fields.tsv
   - Raw Reads : curl -X POST -H "Content-Type: application/x-www-form-urlencoded" -d 'result=read_experiment&query=tax_tree(2697049)&fields=experiment_accession,first_public,country&limit=0&format=tsv' "https://www.ebi.ac.uk/ena/portal/api/search" > reads.custom_fields.tsv
"""

# fetch command line arguments
region_filter = None
date_filter = None
date_type = None

parser = argparse.ArgumentParser(description="Generate map from ENA Advanced Search output (TSV format)")
parser.add_argument('--tsv', help="input file")
parser.add_argument('--region', help="optional list of regions (e.g. EU, US)")
parser.add_argument('--date_range', help="filter to this date range, date format YYYY-MM-DD (e.g. 2020-01-01:2021-01-01)")
parser.add_argument('--date_type', help="date type ")
opts = parser.parse_args(sys.argv[1:])

if opts.region:
    region_filter = opts.region.split(',')
if opts.date_range:
    if not opts.date_type:
        print("Can't apply date filter without --date_type")
        sys.exit(1)
    date_filter = opts.date_range.split(':')
    date_type = opts.date_type


from urllib.request import urlopen
import json

# load set of polygons for each country
geojson_path = "{0}/{1}".format("/".join(os.path.realpath(__file__).split('/')[:-1]), 'custom_with_ids.geo.json')
with open(geojson_path) as json_file:
    countries = json.load(json_file)

import plotly.graph_objects as go
import pandas as pd
import pycountry as pc
import pycountry_convert as pcc
import math

# these countries are not named according to their official pycountry names
# we need to include custom mapping
custom_codes = {
    'Russia':'RUS', 'USA':'USA', 'Czech Republic':'CZE', 'South Korea':'KOR',
    'State of Palestine':'PSE', 'Iran':'IRN', 'West Bank':'PSE'
}

ena_df = pd.read_csv(opts.tsv, sep="\t")

# apply date filters
if date_filter:
    ena_df = ena_df[(ena_df[date_type] > date_filter[0]) & (ena_df[date_type] < date_filter[1])]

# fetch ISO3 codes and counts for each country
# and apply relevant filters
map_data = {}
for country in ena_df.country:
    try:
        country = country.split(':')[0]
    except AttributeError:
        continue

    this_country_code = ''
    try:
        this_country_code = custom_codes[country]
    except KeyError:
        try:
            country_obj = pc.countries.get(name=country)
            if country_obj == None:
                country_obj = pc.countries.get(common_name=country)
            this_country_code = country_obj.alpha_3
        except AttributeError:
            print("Cannot find ISO3 code for '{0}'".format(country))
            sys.exit()

    if region_filter:
        try:
            continent = pcc.country_alpha2_to_continent_code(pc.countries.get(alpha_3=this_country_code).alpha_2)
            if continent not in region_filter:
                continue
        except KeyError:
            continue

    try:
        map_data[country] = [this_country_code, map_data[country][1]+1]
    except KeyError:
        map_data[country] = [this_country_code, 1]

# format the data  specifically for map display
for country in map_data:
    country_code, country_count = map_data[country]
    hover_text = 'Country : {0}<br>Count: {1}'.format(pc.countries.get(alpha_3=country_code).name, f"{country_count:,}")
    map_data[country] = [country, country_code, country_count, math.log(country_count), hover_text]

df = pd.DataFrame.from_dict(map_data, orient='index', columns=['country', 'country_code', 'count', 'log_count', 'text'])

# set up colorbar with raw counts in place of log values
min_max_count = [f"{x:,}" for x in (df['count'].min(), int(df['count'].mean()), df['count'].max())]
min_max_log = [0, 6, 12] # is 12 always the max or just this time - and why?
count_colorbar = go.choroplethmapbox.ColorBar(
    tickmode='array', tickvals=min_max_log, ticktext=min_max_count, tickfont={"size":20}
)

# create the map and display
fig = go.Figure(go.Choroplethmapbox(
    geojson=countries, locations=df.country_code, z=df.log_count, colorscale='Blues', # colorscale="Viridis",
    zmin=0, zmax=12, marker_opacity=0.5, marker_line_width=0, colorbar=count_colorbar,
    text=df.text, hoverinfo='text'
))
fig.update_layout(mapbox_style="carto-positron",  mapbox_zoom=3, mapbox_center = {"lat": 50, "lon": 4}) 
fig.update_layout(margin={"r":10,"t":10,"l":10,"b":10}, coloraxis_colorbar_x=-0.15)

fig.show()
