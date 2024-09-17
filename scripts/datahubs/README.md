README for Data Hubs Management scripts
=======================================

These scripts are for monitoring the usage of Data Hubs at the ENA, to ensure Data Hubs no longer in use are recycled
as part of the Data Hubs Life Cycle Policy. 
See [Data Hubs Documentation](https://www.ebi.ac.uk/seqdb/confluence/display/EMBL/Data+Hubs+Documentation)
for more information.

Set up of Environment
---------------------
- install package requirements for the script:

Python version 3.6, SQLAlchemy version 1.4 (requirement already satisifed by default codon environment)

```
pip install numpy
pip install pandas
pip install oracledb
pip install sqlalchemy
pip install plotly.express
pip install jinja2
```

- add the datahubs-usage-monitoring.py and config file to codon cluster in a suitable location.
- Edit the config.yaml file to include database credentials and any exempt Data Hubs (exempt Data Hubs will not be checked)
- Run the script

Running the script
-------------------

The arguments for the script are:

```-c -config ``` : the file path for the config file (config.yaml by default)

``` -o -outputdir ``` : the path location to hold outputs of the datahubs tracking script

e.g.

```
source ~/.bash_profile
python3 /nfs/production/cochrane/ena/users/<uname>/datahubs-check-usage.py -c /pathto/config.yaml -o pathto/outputfolder
```
To access the output files, copy them from the cluster. The directory holding the files will be provided in the email 
output (dhub_usage_report-YYYYMMDD-HHMM/):
```
scp servername:pathto/outputfolder/dhub_usage_report-YYYYMMDD-HHMM/ /pathto/Downloads/outputfile.ext
```
Output files:

- datahubs.csv - table containing statistics and contact details for a data hubs
- projects.csv - table containing statistics for data hub projects
- datahubs_plot_run.html - visualisation of data hubs data
- datahubs_plot_analysis.html - visualisation of data hubs data
 