README for ENA Assembly Tracking
================================

These scripts are for tracking the processing and publication of assemblies submitted to the ENA, primarily from the 
DToL and ASG projects. This is to ensure the release of all assemblies and to resolve errors.

Assembly tracking (Automating) SOP link:

https://docs.google.com/document/d/1nPvh8xYBjBwL79RqFiSLI75ntSvzJ_oJCg4lpZ5KyBM/edit#heading=h.xj6oat7og6us 

Set up of Environment
---------------------
- install packages for use in your environment:
```
pip install numpy
pip install pandas
pip install openpyxl
pip install elementtree
pip install sqlalchemy
```
- An NCBI API key can be obtained from your NCBI account, but you need to contact NCBI if you plan to make a high rate
  of requests
- Edit the config.yaml file to include database credentials and NCBI API key
- save the edited file with credentials as **config_private.yaml** and check the config_private.yaml file has been added
  to the .gitignore file

Back up and download assembly tracking files
--------------------------------------------
An up-to-date copy of the latest tracking files is stored on shared drive in addition to a local copy used when running
the tracking scripts.

- DToL tracking files folder

https://drive.google.com/drive/folders/1eOMJ8unxDyj9Ek8nB0gyJq_3PbaQWRL5

- ASG tracking files folder

https://drive.google.com/drive/folders/1FtDJBoEpYndyHckt8pyzsoSpyJkmnplX


Each project specific folder contains a tracking_file.txt, this is an indexed master list of assemblies for 
the project and lists all accessions linked to each assembly, and the status of the data in ENA and NCBI.

A google sheet document is the master file for tracking assemblies progress for each project. Before running tracking,
it is necessary to download the latest assembly tracking spreadsheet from google drive in excel format, with the newly
submitted assemblies added for tracking, and save this in the 'project-tracking-files' folder for reading by the tracking scripts:

- DToL google sheet

https://docs.google.com/spreadsheets/d/1j7NEKfwqHoXCo5yrb25YE7o6A6GagFdetRk3t4keeh4/edit#gid=903646295
- ASG google sheet

https://docs.google.com/spreadsheets/d/1HtCbI7fvAOnpGOocUkKMiytz0odQeB9_OfnK-Dqb7RU/edit#gid=0

Running the scripts
-------------------
The scripts can be run individually or multiple scripts can be run at once. The scripts can be run using the command line
or using a run configuration from an IDE.

The arguments for the scripts are:

```-p -project ``` : the project to track (e.g. DToL or ASG)

``` -w -workingdir ``` : the folder location containing the tracking scripts and subfolders holding 
  the tracking files (scripts/assemblytracking/ by default)

``` -c -config ```: the file path for the config file (config_private.yaml by default)

``` -a -action ```: this argument is for the assembly_tracking.py script only. It defines which scripts will be run.
  The options are:
- sql - runs database searching step only (requires database credentials in config)
- add - adds assemblies listed in a Releasing_Sequences.txt to the tracking file and then runs all tracking scripts
- track - runs tracking scripts only 
- all - runs all assembly tracking scripts from start to finish

e.g. to run the sql database step and **then** all the remaining assembly tracking steps, for DToL, using the current dir, use:
```
python3 assembly_tracking.py -project DToL -workingdir .  -config config_private.yaml -action sql
python3 assembly_tracking.py -project DToL -workingdir .  -config config_private.yaml -action add
```
or:
```
python3 assembly_tracking.py -p DToL -w .  -c config_private.yaml -a sql
python3 assembly_tracking.py -p DToL -w .  -c config_private.yaml -a add
```
or to run all tracking scripts at once use the 'all' option:
```
python3 assembly_tracking.py -p DToL -w .  -c config_private.yaml -a all
```
or to run a script alone, use:
```
python3 step4_processingatNCBI.py -p DToL -w .  -c config_private.yaml
```



 