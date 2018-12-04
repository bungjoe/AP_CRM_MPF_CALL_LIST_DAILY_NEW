CREATE OR REPLACE PROCEDURE "CAMP_OFFER_FINAL_CALL_LIST" AS

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
    AP_PUBLIC.CORE_LOG_PKG.pInit( 'AP_CRM', 'CAMP_OFFER_FINAL_CALL_LIST') ;
    
    pTruncate('CAMP_OFFER_CALL_LIST_FINAL');
    AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:CAMP_OFFER_CALL_LIST_FINAL');
    insert /*+ APPEND */ into AP_CRM.CAMP_OFFER_CALL_LIST_FINAL 
    
    /********************************************* update filter on 7 May 2017 - Joeh ******************************************************/
    with w$1 as
    (
         select /*+ MATERIALIZE */ trim(nvl(cuid,'-1'))cuid from camp_ocs_mpf_offer where (/* call_result = 33 or */ record_type in (5,6)) and extracted_date >= trunc(sysdate-1)
    ),
    bcl as
    (
        select distinct tcl.cuid,  contract_id,  
        tcl.first_name, 
        --tcl.last_name,
        CASE WHEN SMS.ID_CUID IS NOT NULL THEN (CASE WHEN tcl.LAST_NAME IS NOT NULL THEN 'Toll Free, ' || tcl.LAST_NAME ELSE 'Toll Free' END) ELSE tcl.LAST_NAME END LAST_NAME,
        tcl.MAX_CREDIT_AMOUNT,
        tcl.max_installment, 
        tcl.MOTHER_MAIDEN_NAME, tcl.PLACE_OF_BIRTH, tcl.BIRTH_DATE,
        tcl.ID_KTP, tcl.EXPIRY_DATE_KTP,
        tcl.MOBILE1, tcl.mobile2,
        tcl.EMAIL_ADDRESS, tcl.FULL_ADDRESS, tcl.NAME_TOWN || ', P' || trim(to_char(ccl.priority,'00')) NAME_TOWN, tcl.NAME_SUBDISTRICT, tcl.CODE_ZIP, tcl.NAME_DISTRICT,
        tcl.info1 
        || ', ' ||
        case when vcl.NAME_CREDIT_STATUS = 'Finished' then to_char(add_months(sysdate, 1)-3,'dd Month yyyy') 
             when vcl.name_credit_status = 'Active' then 
             case when add_months(trunc(sysdate,'MM') + (extract(day from vcl.due_Date)-1),1) - trunc(sysdate) < 30 then
                            to_char(add_months(trunc(sysdate,'MM') + (extract(day from vcl.due_Date)-1),2)-3,'dd Month yyyy')
                    else to_char(add_months(trunc(sysdate,'MM') + (extract(day from vcl.due_Date)-1),1)-3,'dd Month yyyy') end
            else to_char(add_months(sysdate, 1)-3,'dd Month yyyy')
        end info1
        , tcl.info2, 
        case when tcl.tzone = 'DFT' and tzo.tzone is not null then 
                  case when tzo.tzone = 'WIT' then '1. WIT'
                       when tzo.tzone = 'WITA' then '2. WITA'
                  else '3. ' || tzo.tzone end
             when tcl.tzone = 'DFT' and tzo.tzone is null then '3. WIB'
             else 
                  case when tcl.tzone = 'WIT' then '1. WIT'
                       when tcl.tzone = 'WITA' then '2. WITA'
                  else '3. ' || tcl.tzone end 
             end tzone
             ,ccl.priority, nvl(ccl.attempt_last30d,0)attempt_last30D, ccl.risk_group, tcl.pilot_score,
        case when vcl.NAME_CREDIT_STATUS = 'Finished' then add_months(trunc(sysdate), 1)-3
             when vcl.name_credit_status = 'Active' then 
             case when add_months(trunc(sysdate,'MM') + (extract(day from vcl.due_Date)-1),1) - trunc(sysdate) < 30 then
                            add_months(trunc(sysdate,'MM') + (extract(day from vcl.due_Date)-1),2)-3
                    else add_months(trunc(sysdate,'MM') + (extract(day from vcl.due_Date)-1),1)-3 
             end
             else add_months(trunc(sysdate), 1) -3
        end first_due_Date, ccl.min_instalment, tcl.mobile3, tcl.mobile4, tcl.info3, tcl.info4, tcl.info5, tcl.info6, tcl.info7, tcl.info8, tcl.info9, tcl.info10
        from AP_CRM.CAMP_TDY_CALL_LIST tcl
        inner join ap_crm.camp_compiled_list ccl on ccl.id_cuid = tcl.cuid
        left join ap_Crm.v_Camp_last_contract vcl on tcl.contract_id = vcl.contract
        left join ap_Crm.camp_tzone_dist_map tzo on lower(tcl.name_district) = lower(tzo.name_district)
        LEFT JOIN AP_CRM.CAMP_SMS_FF_BASE SMS ON SMS.CAMPAIGN_ID = TO_CHAR(SYSDATE, 'YYMM') AND SMS.CALL_TO_ACTION = 'TOLL FREE' AND TCL.CUID = SMS.ID_CUID
        where lower(info2) not like 'do not call%' 
          and tcl.cuid not in (select cuid from w$1)
    ),
    tcl as
    (
        select distinct ax.* from 
        (
            select bcl.* from bcl where bcl.info2 not like '7%'
            union all
            select bcl.* from bcl where bcl.info2 like '7%' 
        )ax
    ),
    my_dream as
    (
        select /*+ MATERIALIZE */ cuid, loan_purpose_id, replace(replace(loan_purpose_desc,chr(10),''),chr(13),'')loan_purpose_desc, status_active from ap_bicc.STGV_MOB_MD_CUSTOMER bid
        where cuid in (select cuid from bcl) and bid."STATUS_ACTIVE" = 1    
    )
    select ax.cuid, contract_id, first_name, last_name, max_credit_amount,
    max_installment, mother_maiden_name, place_of_birth, birth_date,
    id_ktp, expiry_date_ktp, mobile1, mobile2, email_address, full_address, name_town,
    name_subdistrict, code_zip, 
    name_district, 
    case when md.loan_purpose_desc is not null then info1 || ', My Dream : ' || nvl(md.loan_purpose_desc,'') 
         else info1
    end  info1, 
    info2, 
    tzone, row_order nums, 'OFFER_REGULAR' campaign_type, first_due_Date, min_instalment, mobile3, mobile4, info3, info4, info5, info6, info7, info8, info9, info10, null
    from
    (   /* (order by timezone, channel, max_credit_amount, priority, attempt) */
        select az.*, row_number() over (order by az.nums) row_order from
        (
            select '1.' || trim(to_char(row_number() over (order by tcl.tzone, to_number(substr(tcl.info2,1,1))asc, tcl.attempt_last30d asc, tcl.max_credit_amount desc ),'00000009'))nums, tcl.cuid,  contract_id,  
            tcl.first_name, tcl.LAST_NAME, tcl.MAX_CREDIT_AMOUNT,
            tcl.MAX_INSTALLMENT, tcl.MOTHER_MAIDEN_NAME, tcl.PLACE_OF_BIRTH, tcl.BIRTH_DATE,
            tcl.ID_KTP, tcl.EXPIRY_DATE_KTP, tcl.MOBILE1, tcl.MOBILE2, tcl.EMAIL_ADDRESS,
            tcl.FULL_ADDRESS, tcl.NAME_TOWN, tcl.NAME_SUBDISTRICT, tcl.CODE_ZIP, tcl.NAME_DISTRICT,
            tcl.info1,  tcl.info2, replace(tzone, substr(tcl.TZONE,0,3),'')tzone, tcl.first_due_Date, tcl.min_instalment, tcl.mobile3, tcl.mobile4, tcl.info3, tcl.info4, tcl.info5, tcl.info6, tcl.info7, tcl.info8, tcl.info9, tcl.info10
            from tcl where to_number(substr(tcl.info2,1,1)) <> 7
            union all
            select '2.' || trim(to_char(row_number() over (order by substr(tcl.info2, 1,4), tcl.attempt_last30d asc, tcl.max_credit_amount desc),'00000009'))nums, tcl.cuid,  contract_id,  
            tcl.first_name, tcl.LAST_NAME, tcl.MAX_CREDIT_AMOUNT,
            tcl.MAX_INSTALLMENT, tcl.MOTHER_MAIDEN_NAME, tcl.PLACE_OF_BIRTH, tcl.BIRTH_DATE,
            tcl.ID_KTP, tcl.EXPIRY_DATE_KTP, tcl.MOBILE1, tcl.MOBILE2, tcl.EMAIL_ADDRESS,
            tcl.FULL_ADDRESS, tcl.NAME_TOWN, tcl.NAME_SUBDISTRICT, tcl.CODE_ZIP, tcl.NAME_DISTRICT,
            tcl.info1,  tcl.info2, replace(tzone, substr(tcl.TZONE,0,3),'')tzone, tcl.first_due_Date, tcl.min_instalment, tcl.mobile3, tcl.mobile4, tcl.info3, tcl.info4, tcl.info5, tcl.info6, tcl.info7, tcl.info8, tcl.info9, tcl.info10
            from tcl where substr(tcl.info2,1,4) in ('7.01','7.02','7.03','7.04','7.05','7.06')
            union all
            select '3.' || trim(to_char(row_number() over (order by tcl.pilot_score desc, tcl.attempt_last30d asc),'00000009'))nums, tcl.cuid,  contract_id,  
            tcl.first_name, tcl.LAST_NAME, tcl.MAX_CREDIT_AMOUNT,
            tcl.MAX_INSTALLMENT, tcl.MOTHER_MAIDEN_NAME, tcl.PLACE_OF_BIRTH, tcl.BIRTH_DATE,
            tcl.ID_KTP, tcl.EXPIRY_DATE_KTP, tcl.MOBILE1, tcl.MOBILE2, tcl.EMAIL_ADDRESS,
            tcl.FULL_ADDRESS, tcl.NAME_TOWN, tcl.NAME_SUBDISTRICT, tcl.CODE_ZIP, tcl.NAME_DISTRICT,
            tcl.info1,  tcl.info2, replace(tzone, substr(tcl.TZONE,0,3),'')tzone, tcl.first_due_Date, tcl.min_instalment, tcl.mobile3, tcl.mobile4, tcl.info3, tcl.info4, tcl.info5, tcl.info6, tcl.info7, tcl.info8, tcl.info9, tcl.info10
            from tcl where substr(tcl.info2,1,4) in ('7.07', '7.08')
            union all
            select '4.' || trim(to_char(row_number() over (order by substr(tcl.info2, 1,4), tcl.attempt_last30d asc, tcl.max_credit_amount desc),'00000009'))nums, tcl.cuid,  contract_id,  
            tcl.first_name, tcl.LAST_NAME, tcl.MAX_CREDIT_AMOUNT,
            tcl.MAX_INSTALLMENT, tcl.MOTHER_MAIDEN_NAME, tcl.PLACE_OF_BIRTH, tcl.BIRTH_DATE,
            tcl.ID_KTP, tcl.EXPIRY_DATE_KTP, tcl.MOBILE1, tcl.MOBILE2, tcl.EMAIL_ADDRESS,
            tcl.FULL_ADDRESS, tcl.NAME_TOWN, tcl.NAME_SUBDISTRICT, tcl.CODE_ZIP, tcl.NAME_DISTRICT,
            tcl.info1,  tcl.info2, replace(tzone, substr(tcl.TZONE,0,3),'')tzone, tcl.first_due_Date, tcl.min_instalment, tcl.mobile3, tcl.mobile4, tcl.info3, tcl.info4, tcl.info5, tcl.info6, tcl.info7, tcl.info8, tcl.info9, tcl.info10
            from tcl where substr(tcl.info2,1,4) not in ('7.01','7.02','7.03','7.04','7.05','7.06','7.07', '7.08') and to_number(substr(tcl.info2,1,1)) >= 7
         )az 
    )ax 
    left join my_dream md on ax.cuid = md.cuid
    where 1=1 
    and ax.row_order <= 250000
    union all
    select distinct cci.id_cuid, cmc.contract
           ,case when lower(cci.gender) = 'male' then 'Bpk. ' || cci.name_first || ' ' || cci.name_last
                 when lower(cci.gender) = 'female' then 'Ibu. ' || cci.name_first || ' ' || cci.name_last
            end name_first, null  
           ,ceb.ca_limit_final_updated, ceb.annuity_limit_final_updated
           ,cci.name_mother, cci.name_birth_place, cci.date_birth, cci.id_ktp
           ,cci.expiry_date_ktp, phone.Phone1, phone.Phone2, mail.email, cci.full_address, cci.name_town, cci.name_subdistrict, cci.code_zip_code
           ,cci.name_district
           , case when md.loan_purpose_desc is not null then 'My Dream : ' || nvl(md.loan_purpose_desc,'') else '' end info1
           , '6.Follow Up - In 1stBOD for ' || to_char(trunc(sysdate) - trunc(dtime_pre_process)) || ' days' info2
           , nvl(tzo.tzone,'WIB')
           , row_number() over (order by (trunc(sysdate) - trunc(dtime_pre_process)) desc) nums, 'OFFER_REGULAR' campaign_type, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, trunc(sysdate) 
    from camp_mpf_contracts cmc
    left join camp_client_identity cci on cmc.skp_client = cci.skp_client
    left join camp_elig_base ceb on cci.id_cuid = ceb.id_cuid
    left join AP_CRM.V_CAMP_FINAL_PHONENUM phone on cci.id_CUID = phone.ID_CUID
    left join AP_CRM.V_CAMP_FINAL_EMAIL_ADDR mail on cci.id_CUID = mail.ID_CUID
    left join my_dream md on cci.id_cuid = md.cuid
    left join ap_Crm.camp_tzone_dist_map tzo on lower(trim(cci.name_district)) = lower(tzo.name_district)
    where name_credit_status = 'In Preprocess'
    and dtime_cancellation >= to_date('01/01/3000','mm/dd/yyyy')
    and dtime_pre_process <= trunc(sysdate-2)
    and cci.id_cuid not in (select trim(nvl(cuid,'-1')) from camp_ocs_mpf_offer where (call_result = 33 or record_type in (5,6)) and extracted_date >= trunc(sysdate-1))
    and cmc.skp_client not in 
        (
              select nvl(dcr.skp_client,-99999999) from owner_Dwh.f_offer_ad foa 
              left join OWNER_DWH.DC_CREDIT_case_request dcr on foa.skp_application = dcr.skp_application
              left join owner_Dwh.dc_Credit_case dcc on dcr.skp_credit_case = dcc.skp_credit_case
              where 1=1
              and dcc.skp_credit_substatus in (15, 9)
              and (foa.dtime_valid_to >= trunc(sysdate) and foa.dtime_valid_to < to_date('01/01/3000','mm/dd/yyyy'))
              and foa.flag_deleted = 'N'
              and dcr.skp_salesroom in (61891, 2850271)
              and foa.skp_offer_type = 1 --Alternate offer generated by BLAZE
        )
    and cmc.skp_salesroom in (61891, 2850271);
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
    commit;    
    pStats('CAMP_OFFER_CALL_LIST_FINAL');
    
    AP_PUBLIC.CORE_LOG_PKG.pStart('ins:log_camp_alt_num_cl');
    insert into log_camp_alt_num_cl
    with bs as
    (
        select id_cuid, skp_client, text_contact, row_number() over (partition by skp_client order by nvl(last_call_date, to_date('01/01/1980','mm/dd/yyyy'))) nums
         from camp_alt_number
        where flag_status = 1 and skp_client not in (select skp_Client from tbl_dqm_propose_alt)
    )
    select trunc(sysdate)log_Date, to_char(sysdate,'hh24:mi:ss')time_inserted, clf.cuid, clf.info1, clf.info2, bs.text_contact alt_number from camp_offer_call_list_final clf
    left join bs on clf.cuid = bs.id_cuid and bs.nums = 1
    where mobile2 is null
    and cuid in 
    (
        select id_cuid from bs where nums = 1 
    );
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
    commit;
    pstats('log_camp_alt_num_cl');
    
    AP_PUBLIC.CORE_LOG_PKG.pStart('Apply alternate number');
    merge into camp_offer_call_list_final tgt
    using 
    (
        with bs as
        (
            select id_cuid, skp_client, text_contact, row_number() over (partition by skp_client order by nvl(last_call_date, to_date('01/01/1980','mm/dd/yyyy'))) nums
             from camp_alt_number 
            where flag_status = 1 and skp_client not in (select skp_Client from tbl_dqm_propose_alt)
        )
        select id_cuid, skp_client, text_contact from bs
        where nums = 1 
    )src on (src.id_cuid = tgt.cuid)
    when matched then update set tgt.mobile2 = src.text_contact, tgt.info1 = case when tgt.info1 = '' then 'Alternate Mobile2' else tgt.info1 || ', Alternate Mobile2' end 
      where tgt.mobile2 is null;
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
    commit;
    pstats('camp_offer_call_list_final');

    pTruncate('CAMP_ORBP_CALL_LIST_FINAL');
    AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:CAMP_ORBP_CALL_LIST_FINAL');
    insert /*+ APPEND */ into AP_CRM.CAMP_ORBP_CALL_LIST_FINAL 
    with bcl as
    (
        select distinct tcl.cuid,  contract_id,  
        tcl.first_name, 
        tcl.last_name,
				--CASE WHEN SMS.ID_CUID IS NOT NULL THEN (CASE WHEN tcl.LAST_NAME IS NOT NULL THEN 'Toll Free, ' || tcl.LAST_NAME ELSE 'Toll Free' END) ELSE tcl.LAST_NAME END LAST_NAME,
        tcl.MAX_CREDIT_AMOUNT,
        tcl.max_installment, 
        tcl.MOTHER_MAIDEN_NAME, tcl.PLACE_OF_BIRTH, tcl.BIRTH_DATE,
        tcl.ID_KTP, tcl.EXPIRY_DATE_KTP,
        tcl.MOBILE1, tcl.mobile2,
        tcl.EMAIL_ADDRESS, tcl.FULL_ADDRESS, tcl.NAME_TOWN || ', P' || trim(to_char(ccl.priority,'00')) NAME_TOWN, tcl.NAME_SUBDISTRICT, tcl.CODE_ZIP, tcl.NAME_DISTRICT,
        tcl.info1 
        || ', ' ||
        case when vcl.NAME_CREDIT_STATUS = 'Finished' then to_char(add_months(sysdate, 1)-3,'dd Month yyyy') 
             when vcl.name_credit_status = 'Active' then 
             case when add_months(trunc(sysdate,'MM') + (extract(day from vcl.due_Date)-1),1) - trunc(sysdate) < 30 then
                            to_char(add_months(trunc(sysdate,'MM') + (extract(day from vcl.due_Date)-1),2)-3,'dd Month yyyy')
                    else to_char(add_months(trunc(sysdate,'MM') + (extract(day from vcl.due_Date)-1),1)-3,'dd Month yyyy') end
            else to_char(add_months(sysdate, 1)-3,'dd Month yyyy')
        end info1
        , tcl.info2, 
        case when tcl.tzone = 'DFT' and tzo.tzone is not null then 
                  case when tzo.tzone = 'WIT' then '1. WIT'
                       when tzo.tzone = 'WITA' then '2. WITA'
                  else '3. ' || tzo.tzone end
             when tcl.tzone = 'DFT' and tzo.tzone is null then '3. WIB'
             else 
                  case when tcl.tzone = 'WIT' then '1. WIT'
                       when tcl.tzone = 'WITA' then '2. WITA'
                  else '3. ' || tcl.tzone end 
             end tzone
             ,ccl.priority, nvl(ccl.attempt_last30d,0)attempt_last30D, ccl.risk_group, tcl.pilot_score,
        case when vcl.NAME_CREDIT_STATUS = 'Finished' then add_months(trunc(sysdate), 1)-3
             when vcl.name_credit_status = 'Active' then 
             case when add_months(trunc(sysdate,'MM') + (extract(day from vcl.due_Date)-1),1) - trunc(sysdate) < 30 then
                            add_months(trunc(sysdate,'MM') + (extract(day from vcl.due_Date)-1),2)-3
                    else add_months(trunc(sysdate,'MM') + (extract(day from vcl.due_Date)-1),1)-3 
             end
             else add_months(trunc(sysdate), 1) -3
        end first_due_Date, ccl.min_instalment, tcl.mobile3, tcl.mobile4, tcl.info3, tcl.info4, tcl.info5, tcl.info6, tcl.info7, tcl.info8, tcl.info9, tcl.info10
        from AP_CRM.CAMP_ORBP_TDY_CALL_LIST tcl
        inner join ap_crm.camp_orbp_compiled_list ccl on ccl.id_cuid = tcl.cuid
        left join ap_Crm.v_Camp_last_contract vcl on tcl.contract_id = vcl.contract
        left join ap_Crm.camp_tzone_dist_map tzo on lower(tcl.name_district) = lower(tzo.name_district)
        --LEFT JOIN AP_CRM.CAMP_SMS_FF_BASE SMS ON SMS.CAMPAIGN_ID = TO_CHAR(SYSDATE, 'YYMM') AND SMS.CALL_TO_ACTION = 'TOLL FREE' AND TCL.CUID = SMS.ID_CUID
        where lower(info2) not like 'do not call%'
    ),
    tcl as
    (
        select distinct ax.* from 
        (
            select bcl.* from bcl where bcl.info2 not like '7%'
            union all
            select bcl.* from bcl where bcl.info2 like '7%' 
        )ax
    ),
    my_dream as
    (
        select /*+ MATERIALIZE */ cuid, loan_purpose_id, replace(replace(loan_purpose_desc,chr(10),''),chr(13),'')loan_purpose_desc, status_active from ap_bicc.STGV_MOB_MD_CUSTOMER bid
        where cuid in (select cuid from bcl) and bid."STATUS_ACTIVE" = 1    
    )
    select ax.cuid, contract_id, first_name, last_name, max_credit_amount,
    max_installment, mother_maiden_name, place_of_birth, birth_date,
    id_ktp, expiry_date_ktp, mobile1, mobile2, email_address, full_address, name_town,
    name_subdistrict, code_zip, 
    name_district, 
    case when md.loan_purpose_desc is not null then info1 || ', My Dream : ' || nvl(md.loan_purpose_desc,'') 
         else info1
    end  info1, 
    info2, 
    tzone, row_order nums, 'OFFER_REGULAR' campaign_type, first_due_Date, min_instalment, mobile3, mobile4, info3, info4, info5, info6, info7, info8, info9, info10, trunc(sysdate)
    from
    (   /* (order by timezone, channel, max_credit_amount, priority, attempt) */
        select az.*, row_number() over (order by az.nums) row_order from
        (
            select '1.' || trim(to_char(row_number() over (order by tcl.tzone, to_number(substr(tcl.info2,1,1))asc, tcl.attempt_last30d asc, tcl.max_credit_amount desc ),'00000009'))nums, tcl.cuid,  contract_id,  
            tcl.first_name, tcl.LAST_NAME, tcl.MAX_CREDIT_AMOUNT,
            tcl.MAX_INSTALLMENT, tcl.MOTHER_MAIDEN_NAME, tcl.PLACE_OF_BIRTH, tcl.BIRTH_DATE,
            tcl.ID_KTP, tcl.EXPIRY_DATE_KTP, tcl.MOBILE1, tcl.MOBILE2, tcl.EMAIL_ADDRESS,
            tcl.FULL_ADDRESS, tcl.NAME_TOWN, tcl.NAME_SUBDISTRICT, tcl.CODE_ZIP, tcl.NAME_DISTRICT,
            tcl.info1,  tcl.info2, replace(tzone, substr(tcl.TZONE,0,3),'')tzone, tcl.first_due_Date, tcl.min_instalment, tcl.mobile3, tcl.mobile4, tcl.info3, tcl.info4, tcl.info5, tcl.info6, tcl.info7, tcl.info8, tcl.info9, tcl.info10
            from tcl where to_number(substr(tcl.info2,1,1)) <> 7
            union all
            select '2.' || trim(to_char(row_number() over (order by substr(tcl.info2, 1,4), tcl.attempt_last30d asc, tcl.max_credit_amount desc),'00000009'))nums, tcl.cuid,  contract_id,  
            tcl.first_name, tcl.LAST_NAME, tcl.MAX_CREDIT_AMOUNT,
            tcl.MAX_INSTALLMENT, tcl.MOTHER_MAIDEN_NAME, tcl.PLACE_OF_BIRTH, tcl.BIRTH_DATE,
            tcl.ID_KTP, tcl.EXPIRY_DATE_KTP, tcl.MOBILE1, tcl.MOBILE2, tcl.EMAIL_ADDRESS,
            tcl.FULL_ADDRESS, tcl.NAME_TOWN, tcl.NAME_SUBDISTRICT, tcl.CODE_ZIP, tcl.NAME_DISTRICT,
            tcl.info1,  tcl.info2, replace(tzone, substr(tcl.TZONE,0,3),'')tzone, tcl.first_due_Date, tcl.min_instalment, tcl.mobile3, tcl.mobile4, tcl.info3, tcl.info4, tcl.info5, tcl.info6, tcl.info7, tcl.info8, tcl.info9, tcl.info10
            from tcl where substr(tcl.info2,1,4) in ('7.01','7.02','7.03','7.04','7.05','7.06')
            union all
            select '3.' || trim(to_char(row_number() over (order by tcl.pilot_score desc, tcl.attempt_last30d asc),'00000009'))nums, tcl.cuid,  contract_id,  
            tcl.first_name, tcl.LAST_NAME, tcl.MAX_CREDIT_AMOUNT,
            tcl.MAX_INSTALLMENT, tcl.MOTHER_MAIDEN_NAME, tcl.PLACE_OF_BIRTH, tcl.BIRTH_DATE,
            tcl.ID_KTP, tcl.EXPIRY_DATE_KTP, tcl.MOBILE1, tcl.MOBILE2, tcl.EMAIL_ADDRESS,
            tcl.FULL_ADDRESS, tcl.NAME_TOWN, tcl.NAME_SUBDISTRICT, tcl.CODE_ZIP, tcl.NAME_DISTRICT,
            tcl.info1,  tcl.info2, replace(tzone, substr(tcl.TZONE,0,3),'')tzone, tcl.first_due_Date, tcl.min_instalment, tcl.mobile3, tcl.mobile4, tcl.info3, tcl.info4, tcl.info5, tcl.info6, tcl.info7, tcl.info8, tcl.info9, tcl.info10
            from tcl where substr(tcl.info2,1,4) in ('7.07', '7.08')
            union all
            select '4.' || trim(to_char(row_number() over (order by substr(tcl.info2, 1,4), tcl.attempt_last30d asc, tcl.max_credit_amount desc),'00000009'))nums, tcl.cuid,  contract_id,  
            tcl.first_name, tcl.LAST_NAME, tcl.MAX_CREDIT_AMOUNT,
            tcl.MAX_INSTALLMENT, tcl.MOTHER_MAIDEN_NAME, tcl.PLACE_OF_BIRTH, tcl.BIRTH_DATE,
            tcl.ID_KTP, tcl.EXPIRY_DATE_KTP, tcl.MOBILE1, tcl.MOBILE2, tcl.EMAIL_ADDRESS,
            tcl.FULL_ADDRESS, tcl.NAME_TOWN, tcl.NAME_SUBDISTRICT, tcl.CODE_ZIP, tcl.NAME_DISTRICT,
            tcl.info1,  tcl.info2, replace(tzone, substr(tcl.TZONE,0,3),'')tzone, tcl.first_due_Date, tcl.min_instalment, tcl.mobile3, tcl.mobile4, tcl.info3, tcl.info4, tcl.info5, tcl.info6, tcl.info7, tcl.info8, tcl.info9, tcl.info10
            from tcl where substr(tcl.info2,1,4) not in ('7.01','7.02','7.03','7.04','7.05','7.06','7.07', '7.08') and to_number(substr(tcl.info2,1,1)) >= 7
         )az 
    )ax 
    left join my_dream md on ax.cuid = md.cuid
    where 1=1 
    and ax.row_order <= 180000;
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
    commit;
    pstats('CAMP_ORBP_CALL_LIST_FINAL');
<<finish_line>>
    
AP_PUBLIC.CORE_LOG_PKG.pFinish;
END;
