select l.to_id as umbrella_project_id, up.project_alias as umbrella_project_alias,
       count(unique(p.project_id)) as project_count,
       count(unique(e.experiment_id)) as experiment_count,
       count(unique(r.run_id)) as run_count
from project p
    JOIN study s on s.project_id = p.project_id
    JOIN ena_link l on l.from_id = p.project_id
    JOIN project up on l.to_id = up.project_id
    LEFT JOIN experiment e on e.study_id = s.study_id
    LEFT JOIN run r on e.experiment_id = r.experiment_id
where l.to_id in ('PRJEB39908', 'PRJEB40349', 'PRJEB40770', 'PRJEB40771', 'PRJEB40772')
      and (r.status_id = 4 and e.status_id = 4)
group by l.to_id, up.project_alias
order by l.to_id
