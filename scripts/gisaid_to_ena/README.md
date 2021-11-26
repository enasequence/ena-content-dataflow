# Converting GISAID metadata to ENA-compatible format

Script to convert GISAID metadata spreadsheets into ENA formatted ones using a mapping of equivalent metadata fields (the GISAID headers in this mapping must be unique).
The script can handle CSV, or any format handled by the `pandas.read_excel` method as input ([pandas documentation](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html)).

Output format will be in samples XML format for programmatic submission, or Microsoft Excel format (`.xlsx`) for interactive, as set by the `--outformat` flag. Please note that `.xlsx` files should be inspected and converted to `.tsv` prior to submission to the [Webin interactive service](https://www.ebi.ac.uk/ena/submit/webin/login).

### Download and run the script
The script can be downloaded from our GitHub repository and requires Python3.7 or higher.
```bash
git clone https://github.com/enasequence/ena-content-dataflow.git
cd ena-content-dataflow/scripts/gisaid_to_ena/

python3.7 gisaid_to_ena.py --help
```

### Input options
```
-h, --help              show a help message and exit
--csv CSV               path to input file (CSV format)
--xls XLS               path to input file (Excel format)
--out OUT               output file name
--outformat (xml|excel) xml or excel output
--taxon TAXON           (optional) taxon name or id of samples (default: detect from GISAID sheet)
--map FILE              (optional) path to custom metadata mapping (default: ./metadata_mapping.tsv)
--sheet SHEET           (optional) name of excel sheet (default: 'Submissions')
```

### Examples
```bash
# convert GISAID spreadsheet in CSV format to ENA in excel format
gisaid_to_ena.py --csv gisaid.csv --outfile ena.xlsx --outformat excel
# convert GISAID metadata from sheet called 'Samples' to ENA spreadsheet
gisaid_to_ena.py --xls gisaid.xlsx --sheet Samples --outfile ena.xml --outformat xml
# convert using a custom metadata mapping file
gisaid_to_ena.py --xls gisaid.xlsx --outfile ena.xml --outformat xml --map path/to/mapping.tsv
```
