# Linking human and viral biosamples using the "derived from" relationship field

This script links ``source`` (e.g. viral ENA sample) biosamples with ``target`` (e.g. human EGA sample) biosamples, using the ``derived from`` relationships field in BioSamples (e.g. ``Viral sample`` - ``derived from`` - ``Human sample``). For more information on the relationships fields, please see [BioSample's documentation](https://www.ebi.ac.uk/biosamples/docs/guides/relationships).  

The script ``src/biosamples_relationships.py`` accepts an input file containing source and target accessions (BioSample's IDs - see [format](https://www.ebi.ac.uk/biosamples/docs/faq#_what_pattern_do_biosamples_accessions_follow)) to be linked.
  
This is a collaboration between the [ENA](https://www.ebi.ac.uk/ena/browser/home) and [EGA](https://ega-archive.org/).   

# Input spreadsheet file format

The script accepts a tab separated ``.txt`` or ``.csv`` input file containing source and target accessions (see example ``data/test_sample_accs_list.txt``).

The input file should contain two columns in the following order: ``source_biosample_id`` and ``target_biosample_id``, with 1 source (e.g. ENA BioSample accession) and 1 corresponding target (e.g. EGA BioSample accession) per line. See example below:

```
source_biosample_id    target_biosample_id
SAMEA7616999	SAMEA6941288
SAMEA8785882	SAMEA8123975
SAMEA7616999	SAMEA8698068
```


# Usage 

## Installation
This tool was programmed in **Python** (**version 3.8+**) and depends on additional packages, listed within ``requirements.txt``. 

You may want to install the latest versions of these packages individually and check if it works (running the [examples](#Examples) provided in this README). In case you want to install the specific versions we used to develop this tool, you are advised to create a **virtual environment** (to avoid overwriting other versions you may use).

To install Python dependencies:
```bash
# Step 1. Cloning the tools repository
git clone https://github.com/enasequence/ena-content-dataflow.git
cd ena-content-dataflow/scripts/biosamples_relationships
# Step 2. Creating and activating the virtual environment
virtualenv -p python3 venv_bsdrel
source venv_bsdrel/bin/activate
# Step 3. Installing dependencies
pip3 install -r requirements.txt
# Step 4. Deactivating the virtual environment
deactivate
```
If you wish to install dependencies on your working environment, you will only need to run the two commands from steps 1 and 3 (skip steps 2 and 4). In case you do create a virtual environment, remember to always activate it (using `source venv_bsdrel/bin/activate`) prior running the scripts.

## Credentials
In order to push changes to a BioSample record you will need authority over it: either being the **original owner** of such samples (i.e. samples were submitted using your account) or being the **Webin root user**. 

To provide credentials of your account you can either (1) provide them in a file (use optional argument ``-c``) or (2) set them as environmental variables. 
1. Create a JSON file with the following format (see ``data/test_credentials.json`` for reference):
````
{
    "username": "<put-your-username-here>",
    "password": "<put-your-password-here>"
}
````
2. Export both your username and password with the following command in your terminal before running the script:
````
export bsd_username='<put-your-username-here>'
export bsd_password='<put-your-password-here>'
````

## Usage options

    optional arguments:
        -h, --help            show this help message and exit
        -s SPREADSHEET, --spreadsheet-file SPREADSHEET
                                (required) filename for spreadsheet (csv or .txt) containing source and target biosample accessions, with 1 source and 1 corresponding target accession per line.
        -c [CREDENTIALS_FILE], --credentials-file [CREDENTIALS_FILE]
                                (optional) JSON file containing the credentials (either root or original owner credentials - see data/test_credentials.json for its format) for the linkage to be pushed (default: "credentials.json"). If not given, environment variables 'bsd_username' and 'bsd_password' will be used.
        -prod, --production   (optional) link biosamples in production (if -prod not specified, biosamples will be linked in development by default).
        --verbose             A boolean switch to add verbosity to the scripts (printing initial token, source and target lists...)
  

## Examples

Example 1: link biosamples in **development** environment:`
````
python3 src/biosamples_relationships.py -s data/test_sample_accs_list.txt -c credentials.json --verbose
````
Example 2: link biosamples in **production**:
````
python3 src/biosamples_relationships.py -s data/test_sample_accs_list.txt -c credentials.json --verbose -prod
````
    
# Output
There are 2 output files produced for each source biosample:
- a source biosample json
- a linked source biosample json 

All output files are found in the ``biosamples_output`` directory. To verify that the "derived from field" has been added correctly please check the ``dev`` ([example](https://wwwdev.ebi.ac.uk/biosamples/samples/SAMEA8698068) for SAMEA8698068) or ``prod`` ([example](https://www.ebi.ac.uk/biosamples/samples/SAMN20032469) for SAMN20032469) BioSamples site.
