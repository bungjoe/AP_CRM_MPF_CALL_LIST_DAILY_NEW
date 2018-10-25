create or replace force view v_camp_callback as
select OB.SKP_CLIENT, MAX(CB.Call_Back_Dt) Call_Back_Dt
from owner_Dwh.f_Communication_record_tt ob
     left join
     (
       select skf_communication_record,trunc(to_date(TEXT_VALUE,'dd/mm/yyyy HH24:MI')) Call_Back_Dt
       from owner_Dwh.f_Comm_Result_Part_Tt tt where 1=1
       and skp_communication_result_type in (1911)
       and trim(lower(code_comm_result_part)) in ('call_on','datetime')
     ) CB on OB.SKF_COMMUNICATION_RECORD=CB.SKF_COMMUNICATION_RECORD
     inner join AP_CRM.CAMP_CFG_COMM_STATUS Sts  on OB.Code_status =sts.Code_status
where  1=1
     and lower(trim(sts.Call_Result)) = 'call back'
     and lower(trim(sts.product)) = 'flexifast'
     and lower(trim(sts.Camp_Status)) = 'a'
     and lower(trim(sts.purpose)) = 'offer'
     and lower(trim(sts.Sub_type)) = 'call'
     and trunc(CB.Call_Back_Dt) - trunc(sysdate)<= CNT_DELAY_DAYS
     and trunc(CB.Call_Back_Dt)>= trunc(sysdate)
     and ob.skp_client in (select nvl(skp_client, -99999999) from camp_elig_base where nvl(eligible_final_flag,0) = 1)
group by OB.SKP_CLIENT;

