create or replace force view v_camp_mpf_active as
select SKP_CLIENT,Max(DTIME_APPL_CREATION) create_date
from AP_CRM.CAMP_MPF_CONTRACTS
where
name_credit_status in (select NAME_CREDIT_STATUS from AP_CRM.CAMP_CFG_CREDIT_STATUS where SKP_STRATEGY in ('Actv','Bad') and lower(trim(purpose))='offer' )
group by SKP_CLIENT;

