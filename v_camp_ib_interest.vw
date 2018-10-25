create or replace force view v_camp_ib_interest as
select INBOUND.skp_client,date_call,INBOUND.SKP_COMMUNICATION_SUBTYPE,CODE_SUBTYPE,INTERESTED.NAME_COMMUNICATION_SUBTYPE,INTERESTED.CNT_DELAY_DAYS,INTERESTED.Offer_ACTION
from AP_CRM.CAMP_COMM_REC_IB Inbound
inner join
(
      select SKP_COMMUNICATION_SUBTYPE,CODE_COMMUNICATION_SUBTYPE,NAME_COMMUNICATION_SUBTYPE,CNT_DELAY_DAYS,Offer_ACTION
      from AP_CRM.CAMP_CFG_COMM_IB
      where lower(trim(product)) = 'flexifast'
      and lower(trim(campaign_type)) = 'inbound'
      and lower(trim(purpose)) = 'offer'
      and lower(trim(comm_category))='interested'
      and camp_status = 'A'

) interested on Inbound.SKP_COMMUNICATION_SUBTYPE=INTERESTED.SKP_COMMUNICATION_SUBTYPE
where Inbound.code_channel in
(
      select code_communication_channel
      from owner_dwh.CL_communication_channel
      where lower(name_communication_channel) like '%incoming %' or lower(name_communication_channel) like 'media'

)
and inbound.skp_client in (select nvl(skp_client, -99999999) from camp_elig_base where nvl(eligible_final_flag,0) = 1);

