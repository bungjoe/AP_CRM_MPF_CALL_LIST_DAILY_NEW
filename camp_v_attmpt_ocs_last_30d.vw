create or replace force view camp_v_attmpt_ocs_last_30d as
select log_date, cuid, SUM(ATTEMPT)attempt from log_camp_ocs_mpf_offer lcs
where (log_Date, time_inserted) in
(
    select log_Date, max(time_inserted)time_inserted from log_camp_ocs_mpf_offer
    where log_date >= trunc(sysdate-20)
    group by log_date
)
group by lcs.log_date, lcs.cuid;

