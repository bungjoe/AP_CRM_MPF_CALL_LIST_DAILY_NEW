CREATE OR REPLACE FORCE VIEW V_CAMP_MPF_CANCEL AS
with MPF1
as
(
    select SKP_CLIENT, Max(DTIME_CANCELLATION) cancel_date
    from AP_CRM.CAMP_MPF_CONTRACTS
    where
    name_credit_status in (select NAME_CREDIT_STATUS from AP_CRM.CAMP_CFG_CREDIT_STATUS where SKP_STRATEGY in ('Cncl') and lower(trim(purpose))='offer' )
    and SKP_CLIENT not in
    (
        select distinct skp_client from AP_CRM.V_CAMP_MPF_ACTIVE where create_date >= trunc(add_months(sysdate,-10))
    )
    group by SKP_CLIENT,Contract
),
delay as
(
    SELECT skp_client,contract, DTIME_CANCELLATION cancel_date
           ,TEXT_CANCELLATION_REASON,CNT_DELAY_DAYS
    from AP_CRM.CAMP_MPF_CONTRACTS MPF
    left join
    (
      SELECT TRIM(TEXT_CANCELLATION_REASON) Cncl_Reason, CNT_DELAY_DAYS
             FROM AP_CRM.CAMP_CFG_CREDIT_STAT_STRATEGY
             WHERE LOWER(TRIM(SKP_STRATEGY)) = 'cncl'
             and LOWER(TRIM(purpose)) = 'offer'
             and LOWER(TRIM(CAMP_STATUS)) = 'a'
    ) strategy on trim(lower(strategy.Cncl_Reason)) =trim(lower(MPF.TEXT_CANCELLATION_REASON))

    where name_credit_status in (select NAME_CREDIT_STATUS from AP_CRM.CAMP_CFG_CREDIT_STATUS where SKP_STRATEGY in ('Cncl') and lower(trim(purpose))='offer' )
    and SKP_CLIENT not in (select distinct skp_client from AP_CRM.V_CAMP_MPF_ACTIVE)
    and trunc(MPF.DTIME_CANCELLATION) not in (to_date('01-JAN-3000','DD-MON-YY'),to_date('01-JAN-2000','DD-MON-YY'))
)
select delay.SKP_CLIENT,delay.CONTRACT,delay.cancel_date,delay.TEXT_CANCELLATION_REASON,delay.CNT_DELAY_DAYS ,trunc(delay.cancel_date) + CNT_DELAY_DAYS Released
       from delay inner join MPF1 on delay.skp_client = MPF1.skp_client and delay.cancel_date = MPF1.cancel_date
       where trunc(sysdate)-trunc(delay.cancel_date) < delay.CNT_DELAY_DAYS;

