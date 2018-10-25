create or replace force view camp_v_attmpt_ocs_cm_ring as
select log_date, cuid, sum(attempt)attempt from log_camp_ocs_mpf_offer lcs
where (log_Date, time_inserted) in
(
    select log_Date, max(time_inserted)time_inserted from log_camp_ocs_mpf_offer
    where log_date >= trunc(sysdate,'MM')
    group by log_date
)
and lcs.call_result not in
(select result_key from AP_CRM.CAMP_CFG_GEN_RESULT where lower(trim(Remark)) not in ('ringing'))
group by lcs.log_date, lcs.cuid;

