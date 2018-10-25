create or replace force view camp_v_attmpt_last_30d_ring as
select trunc(CAF.DATE_CALL) date_call ,CUID, count(contact_info)Attempt
from AP_CRM.CONTACT_ATTEMPT_FACT_CRM CAF
where 1=1
and lower(trim(CAF.CALL_RESULT))
not in (select lower(Call_Result) from AP_CRM.CAMP_CFG_GEN_RESULT where lower(trim(Remark)) not in ('ringing'))
and CAF.DATE_CALL >= trunc(sysdate)-20 and campaign_name in ('MPF_OFFER','FLEXY_FAST')
group by trunc(CAF.DATE_CALL),CAF.cuid;

