create or replace force view v_camp_ib_complaint as
select distinct Inbound.SKP_CLIENT,Inbound.Date_call,Inbound.SKP_COMMUNICATION_SUBTYPE
       ,Complaint.NAME_COMMUNICATION_SUBTYPE,complaint.CNT_DELAY_DAYS,complaint.Offer_ACTION
       ,trunc(sysdate)-trunc(date_call) detention_days
from AP_CRM.CAMP_COMM_REC_IB Inbound
inner join
(select SKP_COMMUNICATION_SUBTYPE ,CODE_COMMUNICATION_SUBTYPE,NAME_COMMUNICATION_SUBTYPE,CNT_DELAY_DAYS,Offer_ACTION
        from AP_CRM.CAMP_CFG_COMM_IB
            where lower(trim(product)) = 'flexifast'
            and lower(trim(campaign_type)) = 'inbound'
            and lower(trim(purpose)) = 'offer'
            and lower(trim(comm_category))='complaint'
            and camp_status = 'A'
) complaint on Inbound.SKP_COMMUNICATION_SUBTYPE=COmplaint.SKP_COMMUNICATION_SUBTYPE
where trunc(sysdate)-trunc(date_call) <=complaint.CNT_DELAY_DAYS
and inbound.skp_client in (select nvl(skp_client, -99999999) from camp_elig_base where nvl(eligible_final_flag,0) = 1);

