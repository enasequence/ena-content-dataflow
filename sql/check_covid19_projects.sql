---------------------------------------
--- Checking SARS-CoV-2 Projects ---
---------------------------------------

-- set accession var (will be called as '&&proj_acc' in following queries) ---
define proj_acc = "'PRJNA630716'";

-- check project, incl sample and run counts --
select d.meta_key as datahub, p.project_id, p.first_created, s.study_id, s.study_title, p.scientific_name,
       count(unique(sm.sample_id)) as sample_count, count(unique(r.run_id)) as run_count, p.center_name
from study s 
    join project p on s.project_id = p.project_id left join dcc_meta_key d on d.project_id = p.project_id left join experiment e on e.study_id = s.study_id
    left join experiment_sample es on es.experiment_id = e.experiment_id left join sample sm on sm.sample_id = es.sample_id left join run r on e.experiment_id = r.experiment_id
where p.project_id = &&proj_acc -- uses variable defined above
group by d.meta_key, p.project_id, p.first_created, s.study_id, s.study_title, p.scientific_name, s.study_type, p.center_name
order by p.first_created desc;

-- check samples for name and host --
-- Note: change <TAG>host</TAG> to <TAG>host scientific name</TAG> if proj_acc LIKE 'PRJE%'
select s.tax_id, s.scientific_name, -- s.sample_xml
       REGEXP_SUBSTR(s.sample_xml, '<TAG>host</TAG>\s+<VALUE>([^>]+)</VALUE>', 1, 1, 'i', 1) as host_name,
       count(s.sample_id)
from sample s join experiment_sample es on s.sample_id = es.sample_id join experiment e on es.experiment_id = e.experiment_id join study st on st.study_id = e.study_id
where st.project_id = &&proj_acc
group by s.tax_id, s.scientific_name, REGEXP_SUBSTR(s.sample_xml, '<TAG>host</TAG>\s+<VALUE>([^>]+)</VALUE>', 1, 1, 'i', 1);

-- check runs for processing errors --
select r.*, p.*
from run r join experiment e on e.experiment_id = r.experiment_id join study st on st.study_id = e.study_id join run_process p on p.run_id = r.run_id
where (fastq_error is not null or (mirror_error is not null and mirror_error != 'SKIPPED') or stats_error is not null or check_error is not null and load_error != 'SKIPPED' and sampling_error != 'SKIPPED')
      and st.project_id = &&proj_acc;

-- check datahub contents --
select * from dcc_meta_key where meta_key = 'dcc_grusin';
