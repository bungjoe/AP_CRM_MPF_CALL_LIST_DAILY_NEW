create or replace force view v_camp_mpf_reject as
with Rejected
as
(
    select SKP_CLIENT,NAME_CREDIT_STATUS, Max(DTIME_REJECTION) Reject_date
    from AP_CRM.CAMP_MPF_CONTRACTS
    where
    name_credit_status in (select NAME_CREDIT_STATUS from AP_CRM.CAMP_CFG_CREDIT_STATUS
                                  where Offer_strategy = 'Y' and lower(trim(SKP_STRATEGY)) in ('rjct') and lower(trim(purpose))='offer' )
    and SKP_CLIENT not in
    (
        select distinct skp_client from AP_CRM.V_CAMP_MPF_ACTIVE vcam where vcam.create_date >= trunc(add_months(sysdate,-10))
    )
    group by SKP_CLIENT,NAME_CREDIT_STATUS
    union all
    select SKP_CLIENT,NAME_CREDIT_STATUS, Max(DTIME_CANCELLATION) Reject_date
    from AP_CRM.CAMP_MPF_CONTRACTS
    where
    name_credit_status in (select NAME_CREDIT_STATUS from AP_CRM.CAMP_CFG_CREDIT_STATUS where SKP_STRATEGY in ('Cncl') and lower(trim(purpose))='offer' )
    and SKP_CLIENT not in
    (
        select distinct skp_client from AP_CRM.V_CAMP_MPF_ACTIVE vcam where vcam.create_date >=  trunc(add_months(sysdate,-10))
    )
    and text_credit_status_reason in ('LAP_CANCEL')
    group by SKP_CLIENT,NAME_CREDIT_STATUS
),
delay as
(
      select NAME_CREDIT_STATUS,CNT_DELAY_DAYS from AP_CRM.CAMP_CFG_CREDIT_STATUS
      where Offer_strategy ='Y' and lower(trim(SKP_STRATEGY)) in ('rjct') and lower(trim(purpose))='offer'
)
select Rejected.SKP_CLIENT,Rejected.Reject_date,delay.CNT_DELAY_DAYS ,trunc(Rejected.Reject_date) + CNT_DELAY_DAYS Released
       from Rejected inner join delay on Rejected.NAME_CREDIT_STATUS = delay.NAME_CREDIT_STATUS
       where trunc(sysdate)-trunc(Rejected.Reject_date) < delay.CNT_DELAY_DAYS;

