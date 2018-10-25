create or replace force view camp_v_attmpt_last_3d as
with cfg as
(
     select lower(Call_Result)call_result from AP_CRM.CAMP_CFG_GEN_RESULT
     where lower(trim(Remark)) not in ('ringing')
),
caf as
(
     select date_call, CUID, Contact_info, lower(call_result)call_result
		 from AP_CRM.CONTACT_ATTEMPT_FACT_CRM CAF
		 where date_call >= trunc(sysdate-3)
)
select trunc(CAF.DATE_CALL) date_call ,CUID, count(caf.Contact_info) Attempt
from CAF
join cfg on cfg.call_result = caf.call_result
group by  trunc(CAF.DATE_CALL), CAF.cuid;

