# Bulk Handling Data Hub Users
This script enables for the bulk assignment of users to a Data Hub. This is particularly useful for coordinators who manage a very large collaboration. They must request this by contacting us first and upon receiving appropriate information, we run this script. In scope:
- Add multiple users to a Data Hub
- Assign CONSUMER or PROVIDER roles to multiple users at once

### Download and run the script
The script can be downloaded from our GitHub repository and requires Python3.7 or higher.
```bash
git clone https://github.com/enasequence/ena-content-dataflow.git
cd ena-content-dataflow/scripts/bulk_datahub_user/

python bulk_datahub_user.py --help
```

### Before running the script

Ensure that you meet the requirements:
1) You need an OPS user for ERA databases.
2) Download any missing Python packages.
3) Add your variables for the database connection in 'config.yaml'. Must include the host, port number and service name for both test and production databases. 
4) Complete an appropriate spreadsheet (template: users.tsv).

After running your script, check the database table to see if changes have occurred.

### Input options
```
-h, --help              show a help message and exit
-m, --meta_key          The name of the Data Hub (e.g. dcc_XXXX) - REQUIRED
-u  --users             Path to spreadsheet of user assignment information for the Data Hub (template included in this repository - users.tsv) - REQUIRED
-a  --action            Action to be committed. Options: ADD, REMOVE. Default = ADD (this script currently does not support REMOVE functionality)
-s  --submit            Flag to submit the query and action before enacting on the database. Default = TRUE if used.
-t  --test              Flag to specify whether to use test servers. Default = TRUE if used.
```

### Examples
```bash
# run a bulk assignment using the file users.tsv on dcc_chopin, in the test database and enact the change
python bulk_datahub_user.py -m dcc_chopin -u users.tsv -t -s
```
