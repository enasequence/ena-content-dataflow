README file for Assembly tracking scripts.
=========================================

Project still in progress.

These scripts are designed to track the processing and publication of assemblies submitted to the ENA, primarily from the DTOL and ASG projects.
This is to ensure the release of all assemblies and to resolve errors.

- Assembly tracking SOP: https://docs.google.com/document/d/1uAeiAGMUC3zva2eGkmz4iir7spBeiRaGiLkhjsmXimA/edit?usp=sharing.

Set up of Environment
---------------------
install packages for use in your environment (you can use conda or pip to install the packages):

pip install numpy
pip install pandas
pip install openpyxl
pip install elementtree
pip install oracledb
pip install sqlalchemy

Edit your config.yaml file to include oracle database credentials and NCBI API key

Before running scripts - back up and download assembly tracking files
--------------------------------------------
An up-to-date copy of the latest tracking files is stored on shared drive in addition to a local copy used when running
the tracking scripts.

- DToL tracking files

https://drive.google.com/drive/folders/1eOMJ8unxDyj9Ek8nB0gyJq_3PbaQWRL5
- ASG tracking files

https://drive.google.com/drive/folders/1FtDJBoEpYndyHckt8pyzsoSpyJkmnplX

The tracking files save the outputs of the scripts at various stages for backing up tracking results at each stage of 
tracking. This is because each step in the tracking may take some time.

In addition, each project specific folder contains a tracking_file.txt, this is an indexed master list of assemblies for 
the project and lists all accessions linked to each assembly, and the status of the data in ENA and NCBI.

A live google sheets document is the master file for tracking assemblies progress for each project. Before running tracking,
it is necessary to download the latest assembly tracking spreadsheet from google drive in excel format, with the newly
submitted assemblies added for tracking, and save this the 'project-tracking-files' folder for reading by the tracking scripts:

- DToL google sheet

https://docs.google.com/spreadsheets/d/1j7NEKfwqHoXCo5yrb25YE7o6A6GagFdetRk3t4keeh4/edit#gid=903646295
- ASG google sheet

https://docs.google.com/spreadsheets/d/1HtCbI7fvAOnpGOocUkKMiytz0odQeB9_OfnK-Dqb7RU/edit#gid=0

Summary of Scripts
-------------------
At the start of each script there is a step to set the working directory and the file folder containing the tracking 
files. Please ensure this is correct, and that the assembly tracking files are backed up before progressing any further 
with the tracking.

- **sql** - checks ENA database for assembly progress and identify errors (in progress)

- **import/step1** - import public assebmly list, gets taxon information and adds assemblies to master list of project assemblies 
in the tracking.txt file

- **step2** - uses the ENA browser API to check that assemblies have been linked to their accessions correctly

- **step3** - uses the ENA portal API to check that assemblies have been linked to their accessions correctly

- **step4** - uses the NCBI datasets API to check sequence accessions have been made available by NCBI

- **export** - exports results of tracking back to master assemblies list in tracking file




 