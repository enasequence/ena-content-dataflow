# Converting GISAID metadata to ENA-compatible format

Script to convert GISAID metadata spreadsheets into ENA formatted ones using a mapping of equivalent metadata fields.
The script can handle CSV, or any format handled by the `pandas.read_excel` method as input ([pandas documentation](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html)).

Output format will be in Microsoft Excel format (`.xlsx`).

### Download and run the script
The script can be downloaded from our GitHub repository and requires Python3.7 or higher.
```bash
git clone https://github.com/enasequence/ena-content-dataflow.git
cd ena-content-dataflow/scripts/

python3.7 gisaid_to_ena.py --help
```

### Input options
```
-h, --help     show a help message and exit
--csv CSV      path to input file (CSV format)
--xls XLS      path to input file (Excel format)
--sheet SHEET  (optional) name of excel sheet (default: 'Submissions')
--out OUT      output file name
--taxon TAXON  taxon name or id of samples
```

### Examples
```bash
# convert GISAID spreadsheet in CSV format to ENA in excel format
gisaid_to_ena.py --csv gisaid.csv --out ena.xlsx --taxon 2697049
# convert GISAID metadata from sheet called 'Samples' to ENA spreadsheet
gisaid_to_ena.py --xls gisaid.xlsx --sheet Samples --out ena.xlsx --taxon 2697049
```
