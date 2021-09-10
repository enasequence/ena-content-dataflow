# Linking human and viral biosamples using the "derived from" relationship field

This script links source (e.g. viral ENA) biosamples with target (e.g. human EGA) biosamples, using the "derived from" relationships field in Biosamples. For more information on the relationships fields, please see here: https://www.ebi.ac.uk/biosamples/docs/guides/relationships .  

The script accepts an input file containing source and target accessions for linking, and currently supports a 1:1 linking of source to target biosample.   
  

This is a collaboration between the ENA and EGA.   




# Input spreadsheet file format

The script accepts a tab separated .txt, or .csv input file containing source and target accessions (see test_sample_accs_list.txt) and outputs both the original and linked json files for each biosample.   
The input file should contain two columns in the following order: 'ena_biosample_id' and 'ega_biosample_id' , with 1 source (i.e ENA biosample accession) and 1 corresponding target (i.e EGA biosample accession) per line. See example below:

```
ena_biosample_id    ega_biosample_id
SAMEA8203562	SAMEA8203561
SAMEA8203563	SAMEA8203566
```



# Usage options 

    -s, --spreadsheet : (mandatory) filename for spreadsheet (csv or .txt) containing source and target biosample accessions, with 1 source and 1 corresponding target accession per line.
    -prod            : (optional) link biosamples in production (if -prod not specified, biosamples will be linked in development by default)


# Examples

Example 1: link biosamples in development environment:
```python3 biosamples_relationships.py -s test_sample_accs.txt```

Example 2: link biosamples in production:
 ```python3 biosamples_relationships.py -s test_sample_accs.txt -prod```
    
