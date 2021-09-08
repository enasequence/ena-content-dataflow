# Linking human and viral biosamples using the "derived from" Biosamples relationship field
This script links source (e.g. viral ENA) biosamples with target (e.g. human EGA) biosamples, using the "derived from" relationships field in Biosamples: https://www.ebi.ac.uk/biosamples/docs/guides/relationships 

The script currently supports a 1:1 linking of source to target biosample and accepts a .txt or .csv input file containing source and target accessions (see test_sample_accs_list.txt).
It outputs both the original and linked json files for each biosample.

This is a collaboration between the ENA and EGA.

# Usage options 
    -s, --spreadsheet : (mandatory) filename for spreadsheet (csv or .txt) containing source and target biosample accessions, with 1 source and 1 corresponding target accession per line.
    -prod            : (optional) link biosamples in production (if -prod not specified, biosamples will be linked in development by default)

# Examples
Example 1: link biosamples in development environment:
    python3 biosamples_relationships.py -s test_sample_accs.txt

Example 2: link biosamples in production:
    python3 biosamples_relationships.py -s test_sample_accs.txt -prod

