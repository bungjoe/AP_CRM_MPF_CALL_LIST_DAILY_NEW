CREATE OR REPLACE PROCEDURE "CAMP_RAW_PRE_COMP_CL" AS

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
    AP_PUBLIC.CORE_LOG_PKG.pInit( 'AP_CRM', 'CAMP_RAW_PRE_COMP_CL') ;
    pTruncate('CAMP_OFFER_CALL_PRE');
    --execute immediate 'truncate table ap_crm.camp_offer_call_pre';
    AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:CAMP_OFFER_CALL_PRE');
    insert into ap_crm.camp_offer_call_pre
   with eli_monthly as
    (
        select /*+ MATERIALIZE */ distinct SKP_CLIENT, ID_CUID, initcap(NAME_FULL)name_full,
               case when lower(base.gender) = 'm' then 'Bpk. ' || NAME_FIRST 
                    when lower(base.gender) = 'f' then 'Ibu. ' || NAME_FIRST else NAME_FIRST end name_first,
               NAME_LAST,NAME_BIRTH_PLACE,DATE_BIRTH
               , annuity_limit_final_updated max_instalment, ca_limit_final_updated max_credit_amount, risk_band risk_group
               , rbp_segment_temp rbp_segment, score
               ,CODE_EMPLOYMENT_TYPE,CODE_EMPLOYER_INDUSTRY,MAIN_INCOME,CODE_EDUCATION_TYPE,MAX_TENOR,trunc(valid_from) Valid_from,PRIORITY_ACTUAL priority,ELIGIBLE_FINAL_FLAG First_Eligibility
               ,pilot_name, base.fl_current_eligibility
        from AP_CRM.CAMP_ELIG_BASE Base
        where 
        SKP_Client in 
             ( SELECT skp_client FROM ap_crm.camp_client_at where lower(trim(FLAG_DELETED)) ='n' )
        and lower(trim(Base.CAMP_TYPE)) = 'mpf'
        and priority_actual > 0
        and eligible_final_flag = 1
        --and lower(nvl(pilot_name,'-nullah-')) <> 'r_pos_global_limit'
    )
    ,
    eli_daily as
    (
        select /*+ MATERIALIZE */ DISTINCT Trunc(Date_Valid_From) Valid_From, ID_CUID,MAX_CREDIT_AMOUNT,MAX_ANNUITY as MAX_INSTALMENT
              ,RBP_SEGMENT,RISK_GROUP,Type,Priority Tdy_priority,SCORE RISK_SCORE, FLAG_STILL_ELIGIBLE Tdy_eligibility
        from AP_CRM.CAMP_ELIG_DAILY_CHECK ELI
        where 1=1
--        lower(trim(FLAG_STILL_ELIGIBLE)) = 'y'
        --and (lower(trim(sid_result))= 'sid_ok' or sid_result  is null)
        and lower(trim(campaign_type)) = 'mpf'
        and id_cuid in (select id_cuid from eli_monthly)
        and lower(trim(RBP_SEGMENT)) in (select lower(trim(RBP_SEGMENT)) from AP_CRM.f_rbp_segment_price where status ='Y')
    )
    select /*+ USE_HASH (EM ID LST) */ distinct 
		       trunc(sysdate) as Period, ed.Valid_From, Em.ID_CUID, EM.SKP_CLIENT
           ,lst.contract, lst.name_salesroom
           ,EM.NAME_FULL, EM.NAME_FIRST, EM.NAME_LAST, EM.NAME_BIRTH_PLACE,EM.DATE_BIRTH
           ,EM.CODE_EMPLOYMENT_TYPE,EM.CODE_EMPLOYER_INDUSTRY,EM.MAIN_INCOME,EM.CODE_EDUCATION_TYPE,EM.MAX_TENOR
           ,Em.MAX_CREDIT_AMOUNT, em.MAX_INSTALMENT,Em.RBP_SEGMENT,Em.RISK_GROUP,Em.score RISK_SCORE,
					  ed.Type, EM.Priority, 
					  coalesce(em.priority, to_number(em.priority)) tdy_priority
           ,EM.First_Eligibility, ed.Tdy_eligibility
           ,NAME_MOTHER,ID_KTP,EXPIRY_DATE_KTP,PRIMARYM_1,PRIMARYM_2,PRIMARYM_3,PRIMARYM_4,PRIMARYM_5,CLIENT_EMAIL
           ,full_address,NAME_TOWN,NAME_SUBDISTRICT,CODE_ZIP_CODE,NAME_DISTRICT,DEAD_CUSTOMER, pilot_name
    from eli_monthly em 
		left join eli_daily ED  on ED.ID_CUID=EM.ID_CUID and ED.Valid_from=EM.Valid_from
    LEFT JOIN
    (
        select DISTINCT skp_client,ID_CUID,NAME_MOTHER,ID_KTP,EXPIRY_DATE_KTP,PRIMARYM_1,PRIMARYM_2,PRIMARYM_3,PRIMARYM_4,PRIMARYM_5,CLIENT_EMAIL
               ,full_address,NAME_TOWN,NAME_SUBDISTRICT,CODE_ZIP_CODE,NAME_DISTRICT,DEAD_CUSTOMER
        from AP_CRM.CAMP_CLIENT_IDENTITY
    ) id ON Em.ID_cuid= ID.ID_CUID
    LEFT JOIN ap_crm.v_camp_last_contract lst on EM.SKP_CLIENT =lst.SKP_CLIENT
    WHERE EM.ID_CUID NOT IN (SELECT NVL(ID_CUID,-9999999) FROM CAMP_BLOCK_OFFER WHERE CAMPAIGN_ID = TO_CHAR(SYSDATE,'YYMM'))
    ;
  AP_PUBLIC.CORE_LOG_PKG.pEnd;
  commit;
  pStats('CAMP_OFFER_CALL_PRE');
  AP_PUBLIC.CORE_LOG_PKG.pFinish ;
END;
/

