create or replace force view camp_v_attmpt_last_30d as
with caf as
(
     select /*+ MATERIALIZE */ CAF.DATE_CALL,CUID,contact_info, lower(call_result)call_result
		 from AP_CRM.CONTACT_ATTEMPT_FACT_CRM CAF
		 where CAF.DATE_CALL >= trunc(sysdate)-20 and campaign_name in ('MPF_OFFER','FLEXY_FAST')
)
select /*+ USE_HASH(CAF ELI) */ trunc(CAF.DATE_CALL) date_call ,CUID,count(contact_info) Attempt
from CAF
--join camp_elig_base eli on eli.id_cuid = caf.cuid and eli.eligible_final_flag = 1 and eli.priority_actual > 0
group by  trunc(CAF.DATE_CALL),CAF.cuid
;

