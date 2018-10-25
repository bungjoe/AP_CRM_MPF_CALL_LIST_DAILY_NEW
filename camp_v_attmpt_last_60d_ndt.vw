create or replace force view camp_v_attmpt_last_60d_ndt as
with base as
(
		select /*+ MATERIALIZE */ trunc(CAF.DATE_CALL) date_call ,CUID, count(contact_info)Attempt
		from AP_CRM.CONTACT_ATTEMPT_FACT_CRM CAF
		where CAF.DATE_CALL >= trunc(sysdate)-60
  		and CAF.CALL_RESULT = 'No Dial Tone'
			and campaign_name in ('MPF_OFFER','FLEXY_FAST')
		group by trunc(CAF.DATE_CALL),CAF.cuid
),
ndt as
(
		select /*+ MATERIALIZE */ cuid, sum(attempt)attempt from base
		group by cuid
)
select cuid, attempt from ndt where attempt > 50;

