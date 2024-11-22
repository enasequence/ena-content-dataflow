-- checklist content, i.e  what fields are in a checklist and which mandatory for specific ones
select CVC.CHECKLIST_ID, CVC.CHECKLIST_FIELD_ID, CVC.CHECKLIST_FIELD_MANDATORY, CV.CHECKLIST_NAME, CVF.CHECKLIST_FIELD_NAME
from 
CV_CHECKLIST_CONTENT CVC  LEFT JOIN CV_CHECKLIST CV on ( CVC.CHECKLIST_ID = CV.CHECKLIST_ID )
JOIN CV_CHECKLIST_FIELD CVF on ( CVC.CHECKLIST_FIELD_ID = CVF.CHECKLIST_FIELD_ID )
---where 
---CVC.checklist_id in ('ERC000011', 'ERC000012','ERC000020','ERC000021','ERC000022','ERC000023','ERC000024','ERC000025','ERC000027','ERC000055','ERC000030','ERC000031','ERC000036', 'ERC000053')
order by CHECKLIST_ID, CHECKLIST_FIELD_NAME