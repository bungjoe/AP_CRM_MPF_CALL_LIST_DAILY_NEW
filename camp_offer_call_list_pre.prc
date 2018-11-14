CREATE OR REPLACE PROCEDURE "CAMP_OFFER_CALL_LIST_PRE" AS

  PROCEDURE pStats( acTable VARCHAR2, anPerc NUMBER DEFAULT 0.01) IS
  BEGIN
    AP_PUBLIC.CORE_LOG_PKG.pStart( 'Stat:'||acTable );
    DBMS_STATS.Gather_Table_Stats( OwnName => 'AP_CRM', TabName => acTable,Estimate_Percent => anPerc );
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
  END ;
  PROCEDURE pTruncate( acTable VARCHAR2)
    IS
  BEGIN
    AP_PUBLIC.CORE_LOG_PKG.pStart( 'Trunc:'||acTable );
    EXECUTE IMMEDIATE 'TRUNCATE TABLE AP_CRM.'||acTable ;
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
  END ;

BEGIN
    AP_PUBLIC.CORE_LOG_PKG.pInit( 'AP_CRM', 'CAMP_OFFER_CALL_LIST_PRE') ;
		
    AP_PUBLIC.CORE_LOG_PKG.pStart('ins:camp_mob_activity_log');
    insert /*+ APPEND */ into camp_mob_activity_log
    with exs as
    (
        select /*+ MATERIALIZE */ created_date, modules, user_id from camp_mob_activity_log
        where created_date >= trunc(sysdate-2) 
    )
    select /*+ USE_HASH(LOGS EXS) FULL(EXS) FULL(LOGS) */ logs.created_date, logs.app_version, logs.description, logs.method_name, logs.modules, logs.user_id 
    from APP_BICC.STGV_MOB_ACTIVITY_LOG logs
    left join exs on logs."CREATED_DATE" = exs.created_date and logs."MODULES" = exs.modules and logs."USER_ID" = exs.user_id
    where logs.created_date >= trunc(sysdate-2) and logs.user_id is not null
      and exs.modules is null; 
    commit;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('camp_mob_activity_log');
    
    pTruncate('gtt_camp_final_phone');
    AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:gtt_camp_final_phone');
    insert /*+ APPEND  */ into gtt_camp_final_phone
    select /*+ */ * from AP_CRM.V_CAMP_FINAL_PHONENUM;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('gtt_camp_final_phone');
		
    pTruncate('CAMP_TDY_CALL_LIST');
    --execute immediate 'truncate table ap_crm.CAMP_TDY_CALL_LIST';
    AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:CAMP_TDY_CALL_LIST');
    insert /*+ APPEND */ into ap_CRM.Camp_Tdy_Call_List
  with inbound_comm as
    (select /*+ MATERIALIZE */ id_cuid,greatest(nvl(trunc(DT_intrst_inb),to_date('01/01/1970','mm/dd/yyyy')),nvl(trunc(dt_info_inb),to_date('01/01/1970','mm/dd/yyyy'))) Inbound_dt
           ,'Inbound: Asking about MPF' flag_IB
    from AP_CRM.CAMP_COMPILED_LIST
    where greatest(nvl(trunc(DT_intrst_inb),to_date('01/01/1970','mm/dd/yyyy')),nvl(trunc(dt_info_inb),to_date('01/01/1970','mm/dd/yyyy'))) > to_date('01/01/1970','mm/dd/yyyy') and lower(trim(DEAD_CUSTOMER)) = 'n' and lower(trim(HAS_ACTIVE_FF))='n' and DT_REJECT is null
    ) ,
    IVR_Drop_Call as
    (select /*+ MATERIALIZE */ id_cuid,DT_DROP,'3. From IVR Drop Abandoned' flag_IVR
            from AP_CRM.CAMP_COMPILED_LIST 
            where DT_DROP is not null and lower(trim(DEAD_CUSTOMER)) = 'n' and lower(trim(HAS_ACTIVE_FF))='n' and DT_REJECT is null
    ),
    Refuse as
    (select /*+ MATERIALIZE */ id_cuid,greatest(nvl(trunc(DT_NINTRST),to_date('01/01/1970','mm/dd/yyyy')),nvl(trunc(DT_DWTO),to_date('01/01/1970','mm/dd/yyyy'))
                             ,nvl(trunc(DT_DWTO2),to_date('01/01/1970','mm/dd/yyyy')), nvl(trunc(DT_COMPLAINT),to_date('01/01/1970','mm/dd/yyyy'))) Dt_refuse
    from AP_CRM.CAMP_COMPILED_LIST 
          where greatest(nvl(trunc(DT_NINTRST),to_date('01/01/1970','mm/dd/yyyy')),nvl(trunc(DT_DWTO),to_date('01/01/1970','mm/dd/yyyy'))
                        ,nvl(trunc(DT_DWTO2),to_date('01/01/1970','mm/dd/yyyy')), nvl(trunc(DT_COMPLAINT),to_date('01/01/1970','mm/dd/yyyy'))) >to_date('01/01/1970','mm/dd/yyyy')
                and lower(trim(DEAD_CUSTOMER)) = 'n' and lower(trim(HAS_ACTIVE_FF))='n' and DT_REJECT is null
    ),
    Mobile_offer as
    (
    select /*+ MATERIALIZE */ distinct A.ID_CUID,trunc(DT_MOBAPP) DT_MOBAPP,trunc(Dt_refuse) Dt_Refuse,trunc(Inbound_dt) inbound_dt,
            Case when trunc(Dt_refuse) is null  then '1. Call From Mobile' 
                 when trunc(Dt_refuse) is not null then
                     case when trunc(DT_MOBAPP) > trunc(Dt_refuse) then '1. Call From Mobile' 
                          when trunc(DT_MOBAPP) <= trunc(Dt_refuse) and trunc(Inbound_dt)>trunc(Dt_refuse) then '1. Call From Mobile' 
                          else null end
                end Flag_Mob
      from AP_CRM.CAMP_COMPILED_LIST A left join Refuse B on A.ID_CUID=B.ID_CUID
             left join inbound_comm C on A.ID_CUID=C.ID_CUID
      where DT_MOBAPP is not null
    ),
    Landing_Page as
    (
    select /*+ MATERIALIZE */ distinct A.ID_CUID,trunc(DT_LAND) DT_LAND,trunc(Dt_refuse) Dt_Refuse,trunc(Inbound_dt) inbound_dt,
           Case when trunc(Dt_refuse) is null  then 
                case when trunc(sysdate) - trunc(dt_land) <= 30 then '2. Call From Landing_Page 1-30'
                     when trunc(sysdate) - trunc(dt_land) <= 60 then  '2. Call From Landing_Page 31-60'
                else '2. Call From Landing_Page' end
                when trunc(Dt_refuse) is not null then
                     case when trunc(DT_LAND) > trunc(Dt_refuse) then 
                          case when trunc(sysdate) - trunc(dt_land) <= 30 then '2. Call From Landing_Page 1-30'
                               when trunc(sysdate) - trunc(dt_land) <= 60 then  '2. Call From Landing_Page 31-60'       
                          else '2. Call From Landing_Page' end
                          when trunc(DT_LAND) <= trunc(Dt_refuse) and trunc(Inbound_dt)>trunc(Dt_refuse) then 
                          case when trunc(sysdate) - trunc(dt_land) <= 30 then '2. Call From Landing_Page 1-30'
                               when trunc(sysdate) - trunc(dt_land) <= 60 then  '2. Call From Landing_Page 31-60'
                          else '2. Call From Landing_Page' end
                          else null end
                end Flag_Landing
       from AP_CRM.CAMP_COMPILED_LIST A 
            left join Refuse B on A.ID_CUID=B.ID_CUID
            left join inbound_comm C on A.ID_CUID=C.ID_CUID
       where DT_LAND is not null --and trunc(Dt_refuse)< trunc(DT_LAND)
    ),
    Cancel as
    (
    select /*+ MATERIALIZE */ A.ID_CUID,trunc(DT_Cancel) DT_Cancel, trunc(Inbound_dt) Inbound_dt,
                     case when D.DT_DROP is not null and trunc(DT_Cancel+1) < trunc(sysdate-1) then '3. From IVR Drop Abandoned'
                          when trunc(DT_Cancel+3) < trunc(Inbound_dt) then '4. Call From Reapply'
                          else null end Flag_Cancelled
       from AP_CRM.CAMP_COMPILED_LIST A 
            left join inbound_comm C on A.ID_CUID=C.ID_CUID
            left join IVR_Drop_Call D on A.ID_CUID=D.ID_CUID
       where Inbound_dt is not null and trunc(DT_Cancel) is not null
    ),
    call_back as
    ( 
        select /*+ MATERIALIZE */ id_cuid, trunc(DT_call_back) DT_call_back from AP_CRM.CAMP_COMPILED_LIST A  
        where trunc(DT_call_back) is not null 
        and trunc(DT_call_back) >= trunc(sysdate)
    ),
    sma as
    (    
        select /*+ MATERIALIZE */ sma.USER_ID, sma."CREATED_DATE", row_number() over (partition by sma.user_id order by created_date desc)nums
        from CAMP_MOB_ACTIVITY_LOG sma
        where sma.created_date >= trunc(sysdate-60) and modules in ('CALCULATOR_FOR_FLEXIFAST_OFFER','PRODUCT_CALCULATOR_FLEXIFAST')
    ),
    btx as
    ( 
        select /*+ MATERIALIZE USE_HASH(SMU SMA) */ sma.USER_ID, smu.CUID, smu.PHONE, sma."CREATED_DATE", 
        sma.nums
        from sma
        join  ap_bicc.stgv_mob_user smu on sma.USER_ID = smu.USER_ID
        where sma.nums = 1
    ),    
    calculator as
    (
        select /*+ MATERIALIZE */ user_id, cuid, phone from btx where 1=1
        and phone not in (select text_contact from camp_comm_rec_wn)
    ),
    pos_loan as
    (
        select skp_client, skp_credit_case, name_credit_status, trunc(dtime_proposal) date_apply from camp_pos_contracts
        where (skp_client, skp_Credit_case) in
        (
            select skp_Client, max(skp_credit_case)skp_credit_case 
              from camp_pos_contracts
            where dtime_proposal >= trunc(sysdate-32)
              and skp_client in (select skp_client from AP_CRM.CAMP_COMPILED_LIST)
            group by skp_client
        )
        and name_credit_status <> 'Finished'     
    ),
    ptb_attempt as(
        SELECT PP.CAMPAIGN_ID, PP.ID_CUID, PP.DECILE, ATT.ATTEMPT
        FROM PTB_POPULATION PP
        LEFT JOIN 
            (
              SELECT TO_CHAR(SYSDATE, 'YYMM') CAMPAIGN_ID, CUID, COUNT(ATTEMPT) ATTEMPT
              FROM (
                    SELECT TRUNC(CAF.DATE_CALL) DATE_CALL ,CUID, COUNT(CONTACT_INFO) ATTEMPT
                    FROM AP_CRM.CONTACT_ATTEMPT_FACT_CRM CAF
                    WHERE 1=1
                          AND CAF.DATE_CALL >= TRUNC(SYSDATE, 'MM')
                          AND CAMPAIGN_NAME IN ('MPF_OFFER','FLEXY_FAST')
                    GROUP BY TRUNC(CAF.DATE_CALL), CAF.CUID
                    )
              GROUP BY TO_CHAR(SYSDATE, 'YYMM'), CUID
              ) ATT
        ON PP.ID_CUID = ATT.CUID 
        WHERE PP.CAMPAIGN_ID = TO_CHAR(SYSDATE, 'YYMM')
    ),
    ptb_pilot as
    (
        select ptb.skp_client, ptb.id_cuid, ptb.pilot_flag, ptb.date_valid_from, ptb.date_valid_to, ptb.decile, ptb.score, nvl(atm.attempt, 0) attempt
        from ptb_population ptb
        left join ptb_attempt atm on ptb.id_cuid = atm.id_cuid
        where ptb.campaign_id = to_char(sysdate,'yymm') 
        
        --change to to_char(sysdate,'yymm') when deployed 
        --select id_cuid, pred score, decile from V_PTB_SCORE_2 where campaign_id = to_char(sysdate,'yymm')
    )
    ,price_test as
    (
        select /* MATERIALIZE */ ax.designator, ax.skp_client, ax.id_cuid, ax.interest_rate new_rate, ax.prev_interest std_rate, ax.ca_limit_final_updated credit_limit, ax.standard_instalment, ax.current_instalment, ax.standard_instalment - ax.current_instalment annuity_discount 
        from
        (
            select elig.*, cpt.prev_interest, cpt.prev_tenor, cpt.designator, 
                   ceil((((elig.interest_rate/100) * elig.ca_limit_final_updated * cpt.prev_tenor) + elig.ca_limit_final_updated)/cpt.prev_tenor) + 5000 current_instalment,
                   ceil((((cpt.prev_interest/100) * elig.ca_limit_final_updated * cpt.prev_tenor) + elig.ca_limit_final_updated)/cpt.prev_tenor) + 5000 standard_instalment
            from camp_price_test_pilot elig
            left join camp_price_test cpt on elig.rbp_segment_temp = cpt.product_code and cpt.campaign_id = to_char(sysdate,'yymm')
            where 1=1 
              and elig.campaign_id = to_char(sysdate,'yymm')
        )ax       
    )
    select /*+  */ distinct TList.ID_CUID as CUID, 
           TList.Contract as CONTRACT_ID, 
           initcap(TList.Name_First) || ' ' || initcap(tlist.name_last) as FIRST_NAME, 
           --TList.Name_Last as LAST_NAME, 
           case when trunc(sysdate) < to_Date('11/01/2018','mm/dd/yyyy') then
                  case when lower(tlist.pilot_name) = 'a+'  and tlist.rbp_segment not like '%_249_%' then
                        'A+ Limit Up to 30jt'     
                        when pt.skp_client is not null then
                             'Price Test - Discount Rp. ' || trim(to_char(pt.annuity_discount, '9,999,999')) || ' New Rate ' || new_rate || '%;' || ' Standard Rate ' || std_rate || '%;'
                        when lower(trim(tlist.pilot_name)) = 'premium offer' then 'Premium Offer'
                  end
                else
                  case when pt.skp_client is not null then
                            pt.designator || ' - Discount Rp. ' || trim(to_char(pt.annuity_discount, '9,999,999')) || ' New Rate ' || new_rate || '%;' || ' Standard Rate ' || std_rate || '%;'
                       when lower(trim(tlist.pilot_name)) = 'premium offer' then 'Premium Offer'
                  end
           end LAST_NAME, 
           TList.MAX_CREDIT_AMOUNT as MAX_CREDIT_AMOUNT, 
           TList.MAX_INSTALMENT as MAX_INSTALLMENT, 
           TList.NAME_MOTHER as MOTHER_MAIDEN_NAME, 
           TList.NAME_BIRTH_PLACE as PLACE_OF_BIRTH, 
           TList.DATE_BIRTH as BIRTH_DATE, 
           TList.ID_KTP as ID_KTP, 
           TList.EXPIRY_DATE_KTP as EXPIRY_DATE_KTP, 
           case when clc.cuid is not null then clc.phone else Phone.Phone1 end as MOBILE1, 
           Phone.Phone2 as MOBILE2, 
           mail.email as EMAIL_ADDRESS, 
           TList.FULL_ADDRESS as FULL_ADDRESS, 
           TList.NAME_TOWN as NAME_TOWN, 
           TList.NAME_SUBDISTRICT as NAME_SUBDISTRICT, 
           TList.CODE_ZIP_CODE as CODE_ZIP, 
           TList.NAME_DISTRICT as NAME_DISTRICT, 
           case when lower(tlist.pilot_name) = '2nd mpf' then 'Top UP'
                when lower(tlist.pilot_name) = 'a+' then 'A+'
								when lower(trim(tlist.pilot_name)) = 'premium offer' then 'Premium Offer'	
                when pt.skp_client is not null then 'Price Test' else ' '
           end as INFO1,					 
           case when (
													lower(name_district) like 'luwuk%'
											 or lower(name_district) like 'salakan%'
											 or lower(name_district) like 'banggai%'
											 or lower(name_district) like 'buol%'
											 or lower(name_district) like 'bungku%'
											 or lower(name_district) like 'kolonedale%'
											 or lower(name_district) like 'parigi%'
											 or lower(name_district) like 'poso%'
											 or lower(name_district) like 'ampana%'
											 or lower(name_district) like 'tolitoli%'
											 or lower(name_district) like 'sigi%'
											 or lower(name_district) like 'palu%'
											 or lower(name_district) like 'donggala%'
                     ) then 'Do Not Call - Disaster Affected'
								/*when lower(tlist.name_district) like '%mataram%' and trunc(sysdate) < to_date('09/06/2018','mm/dd/yyyy') then 'Do Not Call - Disaster Affected'
								when lower(tlist.name_district) like '%bima%' and trunc(sysdate) < to_date('09/06/2018','mm/dd/yyyy') then 'Do Not Call - Disaster Affected'
								when lower(tlist.name_district) like '%sumbawa%' and trunc(sysdate) < to_date('09/06/2018','mm/dd/yyyy') then 'Do Not Call - Disaster Affected'
								when lower(tlist.name_district) like '%dompu%' and trunc(sysdate) < to_date('09/06/2018','mm/dd/yyyy') then 'Do Not Call - Disaster Affected'*/
						    when pl.date_apply is not null then 'Do Not Call - POS loan last 32d'
                when Mobile_offer.Flag_Mob is not null then Mobile_offer.Flag_Mob
                when Landing_Page.Flag_Landing is not null then  Landing_Page.Flag_Landing
                when Cancel.Flag_Cancelled is not null then Cancel.Flag_Cancelled
                when IVR_Drop_Call.flag_IVR is not null then IVR_Drop_Call.flag_IVR
                when trunc(Refuse.Dt_refuse) < trunc(inbound_comm.Inbound_dt) then '5. refuse and want to apply'
                when call_back.DT_call_back is not null then 'Do Not call - Already in CallBack list'
                when (nvl(Attempt_last30D,0) <= 29 and nvl(Attempt_current,0) < 30) /*and avail_To_Call3D = 'Y'*/ and Refuse.Dt_refuse is null then
                    case when vcr.contract_type = 'RPOS' then
                         case when TList.MAX_CREDIT_AMOUNT>20000000 then '7.01' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' RPOS Customer Tier1'
                              when TList.MAX_CREDIT_AMOUNT>15000000 then '7.01' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' RPOS Customer Tier2'
                              when TList.MAX_CREDIT_AMOUNT>10000000 then '7.01' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' RPOS Customer Tier3'
                              when TList.MAX_CREDIT_AMOUNT>8000000 then '7.01' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' RPOS Customer Tier4'
                              when TList.MAX_CREDIT_AMOUNT>5000000 then '7.01' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' RPOS Customer Tier5'
                         else '7.01' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' RPOS Customer Tier6' end
                         when tlist.pilot_name = '2nd mpf' then
                         case when TList.MAX_CREDIT_AMOUNT>20000000 then '7.02' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' Top Up Tier1'
                              when TList.MAX_CREDIT_AMOUNT>15000000 then '7.02' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' Top Up Tier2'
                              when TList.MAX_CREDIT_AMOUNT>10000000 then '7.02' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' Top Up Tier3'
                              when TList.MAX_CREDIT_AMOUNT>8000000 then '7.02' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' Top Up Tier4'
                              when TList.MAX_CREDIT_AMOUNT>5000000 then '7.02' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' Top Up Tier5'
                         else '7.02' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' Top Up Tier6' end
                         when clc.cuid is not null then
                         case when TList.MAX_CREDIT_AMOUNT>20000000 then '7.03' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' Mob. App. Calc Tier1'
                              when TList.MAX_CREDIT_AMOUNT>15000000 then '7.03' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' Mob. App. Calc Tier2'
                              when TList.MAX_CREDIT_AMOUNT>10000000 then '7.03' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' Mob. App. Calc Tier3'
                              when TList.MAX_CREDIT_AMOUNT>8000000 then '7.03' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' Mob. App. Calc Tier4'
                              when TList.MAX_CREDIT_AMOUNT>5000000 then '7.03' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' Mob. App. Calc Tier5' 
                         else '7.03' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' Mob. App. Calc Tier6' end
                         when nvl(ptb.pilot_flag,'-') in ('CHALLENGER','CHAMPION','REGULAR') THEN
                              --case when ptb.pilot_flag = 'CHALLENGER' then 'Do not call - PTB Exclusion'
                              case when ptb.pilot_flag = 'CHALLENGER' and ptb.decile in (9, 10) and ptb.attempt >= 2 then 'Do not call - PTB Exclusion'
                              else '7.08' || trim(to_char(nvl(Tlist.attempt_last30D,0),'09')) || ' P' || trim(to_char(Tlist.tdy_priority,'09')) || ' Regular'
                              end
                    else
                         case when TList.MAX_CREDIT_AMOUNT>20000000 then '7.' || trim(to_char(case when Tlist.tdy_priority > 1 then Tlist.tdy_priority+7 else to_number(Tlist.tdy_priority+8) end,'09'))|| trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' Regular Tier1' 
                              when TList.MAX_CREDIT_AMOUNT>15000000 then '7.' || trim(to_char(case when Tlist.tdy_priority > 1 then Tlist.tdy_priority+7 else to_number(Tlist.tdy_priority+8) end,'09'))|| trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' Regular Tier2'      
                              when TList.MAX_CREDIT_AMOUNT>10000000 then '7.' || trim(to_char(case when Tlist.tdy_priority > 1 then Tlist.tdy_priority+7 else to_number(Tlist.tdy_priority+8) end,'09'))|| trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' Regular Tier3' 
                              when TList.MAX_CREDIT_AMOUNT>8000000 then '7.' || trim(to_char(case when Tlist.tdy_priority > 1 then Tlist.tdy_priority+7 else to_number(Tlist.tdy_priority+8) end,'09'))|| trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' Regular Tier4' 
                              when TList.MAX_CREDIT_AMOUNT>5000000 then  '7.' || trim(to_char(case when Tlist.tdy_priority > 1 then Tlist.tdy_priority+7 else to_number(Tlist.tdy_priority+8) end,'09'))|| trim(to_char(nvl(Tlist.attempt_last30D,0),'09'))  || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' Regular Tier5' 
                         else '7.' || trim(to_char(case when Tlist.tdy_priority > 1 then Tlist.tdy_priority+7 else to_number(Tlist.tdy_priority+8) end,'09')) || trim(to_char(nvl(Tlist.attempt_last30D,0),'09')) || ' P' || trim(to_char(Tlist.tdy_priority,'09')) ||' Regular Tier6' 
                         end
                    end     
           else 'Do Not Call'
           end as INFO2, 
           Tm_zone.CODE_TIMEZONE as TZONE,
           Mobile_offer.Flag_Mob,
           Landing_Page.Flag_Landing,
           IVR_Drop_Call.flag_IVR,
           Cancel.Flag_Cancelled,
           Refuse.Dt_refuse,
           inbound_comm.Inbound_dt,
           call_back.DT_call_back,
           avail_To_Call3D,
           Attempt_last30D,
           cpl.pilot_flag,
           null,
           nvl(ptb.score, 0),
					 NULL MOBILE3,
					 NULL MOBILE4,
					 null info3, /* Do not use this column, used for DQM marker */
					 NULL INFO4,
					 NULL INFO5,
					 NULL INFO6,
					 NULL INFO7,
					 NULL INFO8,
					 NULL INFO9,
					 NULL INFO10					 
    from AP_CRM.CAMP_COMPILED_LIST Tlist
    left join price_test pt on tlist.skp_client = pt.skp_client
    left join Mobile_offer on Tlist.ID_CUID = Mobile_offer.ID_CUID
    left join Landing_Page on Tlist.ID_CUID = Landing_Page.ID_CUID
    left join IVR_Drop_Call on Tlist.ID_CUID = IVR_Drop_Call.ID_CUID
    left join Refuse on Tlist.ID_CUID = Refuse.ID_CUID
    left join Cancel on Tlist.ID_CUID = Cancel.ID_CUID
    left join call_back on Tlist.ID_CUID = call_back.ID_CUID
    left join inbound_comm on Tlist.ID_CUID = inbound_comm.ID_CUID
    left join (select TEXT_CONTRACT_NUMBER ,CODE_TIMEZONE from AP_BICC.F_CONTRACT_TIMEZONE_AD) Tm_zone on Tm_zone.TEXT_CONTRACT_NUMBER=Tlist.CONTRACT
    left join AP_CRM.gtt_camp_final_phone phone on Tlist.ID_CUID = phone.ID_CUID
    left join AP_CRM.V_CAMP_FINAL_EMAIL_ADDR mail on Tlist.ID_CUID = mail.ID_CUID
    left join AP_CRM.CAMP_PILOT_LIST cpl on tlist.id_cuid = cpl.id_cuid and cpl.campaign_id = to_char(sysdate,'yymm')
    left join ap_crm.v_camp_rpos_contracts vcr on tlist.skp_client = vcr.skp_client
    left join calculator clc on tlist.id_cuid = clc.cuid
    left join pos_loan pl on tlist.skp_Client = pl.skp_client
    left join ptb_pilot ptb on tlist.id_cuid = ptb.id_cuid    
    --left join ap_crm.v_camp_black_list vcb on tlist.id_cuid = vcb.cuid
    where 1=1
    and nvl(dead_customer,'N') = 'N' 
    and Has_active_FF = 'N' --case when lower(tlist.pilot_name) like 'premium offer%' then 'Y' else 'N' end
    and nvl(Tdy_eligibility,'U') not in ('N','0')
--    and nvl(tdy_eligibility,'1') = 1
    and DT_Reject is null
    and phone.Phone1 is not null
    and TList.Contract is not null
    and tlist.id_cuid not in (select nvl(cuid, -999999) from ap_crm.v_camp_black_list)
--    and tlist.id_cuid not in (select nvl(cuid,-9999) from AP_CRM.CAMP_BLACK_LIST)
--    and tlist.id_cuid not in (select nvl(cuid,-9999) from AP_CRM.CAMP_BLACK_LIST_TEMPORARY WHERE TRUNC(SYSDATE) <= TRUNC(DATE_BLOCK+90))
    --and tlist.tdy_priority <= case when extract(day from trunc(sysdate)) between 1 and 10 then 15
    --                               when extract(day from trunc(sysdate)) > 10 then 25 end
    ;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('CAMP_TDY_CALL_LIST');

    AP_PUBLIC.CORE_LOG_PKG.pFinish ;
END;
