
---### STUDIES AND PROJECTS ###---

select * from project;
select distinct project_id, tax_id, scientific_name from project group by tax_id;
select * from project where project_id in ('PRJEB37722');
select * from project where status_id=2;
select * from project where project_type in ('');
select * from submission_contact where submission_account_id in
    (select submission_account_id from STUDY where project_id in (''));

select * from study;
select * from study where study_id in ('ERP122169');
select * from study where submission_id in ('Webin-52935');
select * from study where project_id in ('PRJNA634119');
select * from study where submission_account_id in ('Webin-52935');
select * from study where trunc(first_created) = TO_DATE('DD-MMM-YY');


---### SAMPLES ###---

select * from sample where sample_id in ('SAMEA5063104');
select * from sample where sample_alias in ('');
select * from sample where biosample_id in ('SAMEA5063104');
select * from sample where submission_account_id in ('');
select * from sample where sample_id between '' and '';
select * from sample where checklist_id='';

select * from sample where sample_id in
  (select sample_id from experiment where experiment_id in (''));

select * from sample where sample_id in
  (select sample_id from experiment where experiment_id in
    select experiment_id from run where run_id in ('')));

select unique(sample_id) from sample where sample_id in
  (select sample_id from experiment_sample join experiment
  on experiment.experiment_id = experiment_sample.experiment_id
  where experiment.study_id in (''));

---### SAMPLE CHECKLISTS ###---

select * from cv_checklist_field where checklist_field_name like '%isolation source non-host-associated%';
select * from cv_checklist_field where checklist_field_description like '%cell culture%';
select * from cv_checklist_content where checklist_field_id='517'; --#find out which checklist the field is in
select * from cv_checklist_field_value where checklist_field_id='517'; --# find all the possible checklist field values for a particular checklist field

---### EXPERIMENTS ###---

select * from experiment;
select * from experiment where experiment_id in ('ERX4113337');
select * from experiment where submission_account_id in ('');
select * from experiment where study_id in ('');
select * from experiment where sample_id in ('');
select * from experiment where instrument_model like ('%');
select * from experiment where experiment_id between '' and '';


---### RUNS ###---

select * from run;
select * from run where status_id=2;
select * from run where run_id in ('SRR12008187');
select * from run where submission_account_id in ('');  
select * from run where run_id between '' and '';


---### SAMPLE MATCHES ###---

select * from experiment_sample;
select * from experiment_sample where experiment_id in ('');
select * from experiment_sample where sample_id in ('');

select * from run_sample;
select * from run_sample where run_id in ('');
select * from run_sample where sample_id in ('');

select * from analysis_sample where sample_id in ('');
select * from analysis_sample where analysis_id in ('');


---### ANALYSES ###---

select * from analysis where analysis_id in ('ERZ1431265');
select * from analysis where study_id in ('ERP122169');


---### SUBMISSIONS ###---

select * from submission;
select * from submission where submission_id in ('ERA2410191'); --submission_account_id = Webin acc #
select * from submission where submission_account_id in (''); --ERA or SRA#######

---### FILES AND PROCESSING ###---

select * from webin_file;
select * from webin_file where data_file_owner_id in ('ERP122169');
select * from webin_file where data_file_format='BAM'
and data_file_owner_id in (select run_id from run where status_id = '4')
select * from webin_file where data_file_owner_id in (select run_id from run where experiment_id in (select experiment_id from experiment where study_id = ''));
select * from webin_file where data_file_owner_id in (select analysis_id from analysis where study_id = '');

select * from cv_process_status;

select * from pipelite_process;
select * from pipelite_process where process_id in ('');
select * from pipelite_process where process_id in (select run_id from run where experiment_id in (select experiment_id from experiment where study_id = 'ERP122169'));
select * from pipelite_process where process_id in (select analysis_id from analysis where study_id = 'ERP122169');


select * from run_file;
select * from run_file where run_id in ('');
select * from run_file where data_file_path like '%';
select * from run_file where run_id in (select run_id from run where experiment_id in (select experiment_id from experiment where study_id = ''));

select * from run_process;
select * from run_process where run_id in ('SRR12008188');
select run_id, LOAD_ERROR, FASTQ_ERROR, STATS_ERROR, CHECK_ERROR, SAMPLING_ERROR, DATA_ERROR, MIRROR_ERROR from run_process where run_id in ('SRR12008186');
select * from run_process where run_id in (select run_id from run where experiment_id in (select experiment_id from experiment where study_id = 'ERP122169'));

select * from cv_analysis_file_group;

select * from cv_file_group_format;

select * from DATA_FILE_META;

select 'ls -la /fire/staging/era/'|| data_file_path from webin_file where submission_id = 'ERA1686791' AND process_status_id not in ( 4 );





---### ENAPRO TABLES ###---

select * from GCS_ASSEMBLY where assembly_id = 'ERZ';
select * from GCS_ASSEMBLY where gc_id = 'GCA_902459835';
select * from GCS_ASSEMBLY where submission_account_id='';

update GCS_ASSEMBLY set organism='' where assembly_id = 'ERZ';

select * from v_assembly_report where process_status not in ('');

select primary, secondary from accpair where primary in ('');
select * from accpair order by timestamp;

select * from ERA.V_TEMPLATE_REPORT where process_status = '';
select * from PIPELITE_STAGE where PROCESS_ID='ERZ';

select * from dbentry;
select * from dbentry where primaryacc# in ('');
select * from dbentry where primaryacc# like '%';

select v_assembly_report.*, submission.submission_tool
from analysis
inner join v_assembly_report on analysis.analysis_id=v_assembly_report.analysis_id
inner join submission on analysis.submission_id=submission.submission_id
where process_status='USER_ERROR' and organism not like '%metagenome%';

select * from v_analysis_report;

select * from cv_database_prefix;
select * from prefix;
select count(*) from cv_database_prefix;

---### ACCOUNTS AND CONTACTS ###---

select * from submission_contact;
select * from submission_contact where lower(email_address) like lower('t.desilva@sheffield.ac.uk');
select * from submission_contact where submission_account_id in ('Webin-52935'); --submission account id = webin account
select * from submission_contact where lower(first_name) like lower('%') and lower(surname) like lower('%');

select * from submission_account;
select * from submission_account where submission_account_id in ('Webin-332');
select * from submission_account where center_name like '%';
select * from submission_account where lower(broker_name) like lower('%');

select * from submission_account join submission_contact
on submission_account.submission_account_id = submission_contact.submission_account_id
where submission_account.submission_account_id in ('');

select * from cv_center_name;
select * from cv_center_name where lower(center_name) like lower('%');
update cv_center_name set description='' where center_name='';
insert into cv_center_name (center_name, description) values ('', '');

update submission_account set
  --center_name='',
  --laboratory_name='',
  --description='',
  --address=''
  --tel_no=''
where submission_account_id='';

insert into cv_broker_name(broker_name, description) values ('', '');

update submission_account set
broker_name = ''
where SUBMISSION_ACCOUNT_ID in ('');

update submission_account set description = 'ACCOUNT WITHDRAWN AT SUBMITTER REQUEST RT - ' where submission_account_id = '';

---### MGNIFY ###---

update submission_account set
 role_metagenome_analysis = 'Y',
 role_metagenome_submitter = 'Y'
where submission_account_id in ('Webin-53224');


---### MISC ###---

select * from DCC_META_KEY where meta_Key = 'dcc_basie';


---### STUDY-BASED CANCELLATION ###---

update study set status_id='3', hold_date= null where study_id in ('ERP121350');
-- Expected:
update project set status_id='3', hold_date=null where project_id in (select project_id from study where study_id in ('ERP121350'));
-- Expected:
update sample set status_id='3', hold_date= null where sample_id in (select distinct sample_id from run_sample where run_id in (select distinct run_id from run where experiment_id in (select experiment_id from experiment where study_id='')));
-- Expected:
update experiment set status_id='3', hold_date= null where experiment_id in (select experiment_id from experiment where study_id='ERP121350');
-- Expected:
update run set status_id='3', hold_date= null where run_id in (select run_id from run where experiment_id in (select experiment_id from experiment where study_id='ERP121350'));
-- Expected:
update analysis set status_id='3',hold_date=NULL where study_id in ('');
-- Expected:


---### OBJECT-BASED CANCELLATION ###---

--- By Sample ID:
update sample set status_id='3', hold_date=null where sample_id in ('ERS4528695','ERS4528767');
-- Expected:
update experiment set status_id='3', hold_date=null where experiment_id in (select experiment_id from experiment_sample where sample_id in ());
-- Expected:
update run set status_id='3', hold_date=null where run_id in (select run_id from run_sample where sample_id in ());
-- Expected:

--- By Experiment ID:
update experiment set status_id='3', hold_date=null where experiment_id in ('');
-- Expected:
update run set status_id='3', hold_date=null where experiment_id in ('');
-- Expected:

--- By Run ID:
update run set status_id='3', hold_date=null where run_id in ('');
-- Expected:

--- By Analysis ID:
update analysis set status_id='3', hold_date=null where analysis_id in ('');
-- Expected:


---### STUDY-BASED SUPPRESSION ###---

update study set status_id='5', hold_date=null, status_comment='' where study_id in ('');
update project set status_id='5', hold_date=null, status_comment='' where project_id in (select project_id from study where study_id in (''));
update sample set status_id='5', hold_date= null, status_comment='' where sample_id in (select distinct sample_id from run_sample where run_id in (select distinct run_id from run where experiment_id in (select experiment_id from experiment where study_id in (''))));
update experiment set status_id='5', hold_date= null, status_comment='' where experiment_id in (select experiment_id from experiment where study_id in (''));
update run set status_id='5', hold_date= null, status_comment='' where run_id in (select run_id from run where experiment_id in (select experiment_id from experiment where study_id in ('')));
update analysis set status_id='5',hold_date=NULL, status_comment='' where study_id in ('');


---### OBJECT-BASED SUPPRESSION ###---

update study set status_id='5', hold_date=null, status_comment='' where study_id in ('');
update project set status_id='5', hold_date=null, status_comment='' where project_id in ('');
update sample set status_id='5', hold_date=null, status_comment='' where sample_id in ('');
update experiment set status_id='5', hold_date=null, status_comment='' where experiment_id in ('');
update run set status_id='5', hold_date=null, status_comment='' where run_id in ('');
update analysis set status_id='5', hold_date=null, status_comment='' where analysis_id in ('');


---### STUDY-BASED RELEASE ###---

update study set status_id='4', hold_date=null, status_comment='' where study_id in ('');
update project set status_id='4', hold_date=null, status_comment='' where project_id in (select project_id from study where study_id in (''));
update sample set status_id='4', hold_date= null, status_comment='' where sample_id in (select distinct sample_id from run_sample where run_id in (select distinct run_id from run where experiment_id in (select experiment_id from experiment where study_id in (''))));
update experiment set status_id='4', hold_date= null, status_comment='' where experiment_id in (select experiment_id from experiment where study_id in (''));
update run set status_id='4', hold_date= null, status_comment='' where run_id in (select run_id from run where experiment_id in (select experiment_id from experiment where study_id in ('')));
update analysis set status_id='4',hold_date=NULL, status_comment='' where study_id in ('');


---### OBJECT-BASED RELEASE ###---

update study set status_id='4', hold_date=null, status_comment='' where study_id in ('');
update project set status_id='4', hold_date=null, status_comment='' where project_id in ('');
update sample set status_id='4', hold_date=null, status_comment='' where sample_id in ('');
update experiment set status_id='4', hold_date=null, status_comment='' where experiment_id in ('');
update run set status_id='4', hold_date=null, status_comment='' where run_id in ('');
update analysis set status_id='4', hold_date=null, status_comment='' where analysis_id in ('');


---### PKG SUPPRESSION ###--- https://www.ebi.ac.uk/seqdb/confluence/display/EMBL/Withdrawing+non-sequence+entries

--Permanently suppress an entry without suppressing samples associated with runs or analyses
exec withdraw_pkg.suppress( submission_account_id => 'Webin-', id => '<accession number or range> ', comment => 'ENA Helpdesk ticket #', include_samples => false );
--Permanently suppress an entry with samples associated with runs or analyses
exec withdraw_pkg.suppress( submission_account_id => 'Webin-', id => '<accession number or range>', comment => 'ENA Helpdesk ticket #' );
--Temporarily suppress a study without suppressing samples associated with runs or analyses
exec withdraw_pkg.suppress( submission_account_id => 'Webin-', id => '<accession number or range>', comment => 'ENA Helpdesk ticket #', hold_date => TO_DATE('<date in format DD-MON-YYYY>', 'DD-MON-YYYY'), include_samples => false );
--Temporarily suppress a study with samples associated with runs or analyses
exec withdraw_pkg.suppress( submission_account_id => 'Webin-', id => '<accession number or range>', comment => 'ENA Helpdesk ticket #', hold_date => TO_DATE('31-DEC-2019', 'DD-MON-YYYY') );


---### META

--- Get explanation:
explain plan for
    <SQL_Query_Here>;
--- View the explanation:
select plan_table_output from table(dbms_xplan.display('plan_table', null, 'advanced'));
