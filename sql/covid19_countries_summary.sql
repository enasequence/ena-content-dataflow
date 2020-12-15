-- note that submission_account.country is only set correctly for
-- those submitting directly to us
select sa.country, st.first_created, max(s.last_updated) as last_activity,
    count(unique(s.sample_id)) as sample_count, count(unique(r.run_id)) as run_count, count(unique(e.experiment_id)) as experiment_count,
    sa.center_name, st.project_id, st.status_id, st.study_title
from sample s
    join experiment_sample es on s.sample_id = es.sample_id
    join experiment e on es.experiment_id = e.experiment_id
    join run r on e.experiment_id = r.experiment_id
    join study st on st.study_id = e.study_id
    join submission_account sa on s.submission_account_id = sa.submission_account_id
where s.tax_id = 2697049 and sa.country is not null
group by sa.country, sa.center_name, st.project_id, st.study_title, st.first_created, st.status_id
order by last_activity desc
