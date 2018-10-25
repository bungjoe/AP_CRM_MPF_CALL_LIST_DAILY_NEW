CREATE OR REPLACE PROCEDURE "CAMP_RAW_WRITE_LOG" AS
  type log_elig_base is table of ap_crm.log_camp_elig_base%ROWTYPE;
  elig_base log_elig_base;
  cursor curs_log_elig_base is select trunc(sysdate)log_date, to_char(sysdate,'hh24:mi:ss')time_inserted, ceb.* from ap_crm.camp_elig_base ceb;
	dt_current date;
	
  PROCEDURE pStats( acTable VARCHAR2, anPerc NUMBER DEFAULT 0.01) IS
  BEGIN
    AP_PUBLIC.CORE_LOG_PKG.pStart( 'Stat:'||acTable );
    DBMS_STATS.Gather_Table_Stats( OwnName => 'AP_CRM', TabName => acTable,Estimate_Percent => anPerc );
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
  END ;
  PROCEDURE pTruncate( acTable VARCHAR2) IS
  BEGIN
    AP_PUBLIC.CORE_LOG_PKG.pStart( 'Trunc:'||acTable );
    EXECUTE IMMEDIATE 'TRUNCATE TABLE AP_CRM.'||acTable ;
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
  END ;

begin
   AP_PUBLIC.CORE_LOG_PKG.pInit( 'AP_CRM', 'CAMP_RAW_WRITE_LOG');
   dt_current := trunc(sysdate);
if (dt_current = to_date('10/24/2018','mm/dd/yyyy')) then
	goto finish_line;
end if;

   AP_PUBLIC.CORE_LOG_PKG.pStart('INS:LOG_CAMP_CLIENT_AT');
   insert /*+ APPEND */ into AP_CRM.LOG_CAMP_CLIENT_AT
   select 
   trunc(sysdate)       ,to_char(sysdate, 'hh24:mi:ss')
  ,SKF_CAMPAIGN_CLIENT  ,CODE_SOURCE_SYSTEM
  ,ID_SOURCE            ,DATE_EFFECTIVE
  ,SKP_PROC_INSERTED    ,SKP_PROC_UPDATED
  ,FLAG_DELETED         ,SKP_CAMPAIGN
  ,SKP_CLIENT           ,SKP_CREDIT_CASE
  ,SKP_GOODS_TYPE       ,SKP_MARKETING_ACTION
  ,ID_CAMPAIGN          ,CODE_SEGMENT
  ,CODE_CAMPAIGN_TYPE   ,CODE_CAMPAIGN_SUB_TYPE
  ,CODE_PRODUCT_TYPE    ,NAME_OFFER
  ,FLAG_OFFER           ,AMT_CREDIT_MAX
  ,AMT_ANNUITY_MAX      ,AMT_DOWN_PAYMENT_MIN
  ,DATE_VALID_FROM      ,DATE_VALID_TO
  ,FLAG_RESPONDED       ,SKP_CAMPAIGN_SUBTYPE
  ,SKP_CAMPAIGN_TYPE    ,CNT_CAMPAIGN_CLIENT
  from ap_Crm.camp_client_at;
  AP_PUBLIC.CORE_LOG_PKG.pEnd;
  commit;
  pStats('LOG_CAMP_CLIENT_AT');

  AP_PUBLIC.CORE_LOG_PKG.pStart('INS:LOG_ELIG_DAILY_CHECK');
  insert /*+ APPEND */ into ap_Crm.log_elig_daily_check
  select trunc(sysdate)
  , to_Char(sysdate, 'hh24:mi:ss')  ,tgt.CAMPAIGN_ID
  , tgt.ID_CUID                     ,tgt.MAX_CREDIT_AMOUNT
  , tgt.MAX_ANNUITY                 ,tgt.VALIDITY_PERIOD
  , tgt.DPD_HISTORY                 ,tgt.DPD_12_MS
  , tgt.DPD_3_MS                    ,tgt.type
  , tgt.PRIORITY                    ,tgt.CAMPAIGN_TYPE
  , tgt.DATE_VALID_FROM             ,tgt.DATE_VALID_TO
  , tgt.SID_RESULT                  ,tgt.score
  , tgt.risk_group                  ,tgt.rbp_segment
  , tgt.lost_elig_reason            ,tgt.date_check
  , tgt.FLAG_STILL_ELIGIBLE
  --from AP_RISK.ELIGIBILILITY_DAILY_CHECK tgt
  from ap_Crm.camp_elig_daily_check tgt
  --where to_char(date_valid_from, 'yymm') = to_char(sysdate,'yymm');
  ;
  AP_PUBLIC.CORE_LOG_PKG.pEnd;
  commit;
  pStats('LOG_ELIG_DAILY_CHECK');
  
  AP_PUBLIC.CORE_LOG_PKG.pStart('INS:LOG_CAMP_ELIG_BASE');
/*  open curs_log_elig_base;
     loop
        fetch curs_log_elig_base bulk collect into elig_base limit 100000;
        forall i in elig_base.first..elig_base.last
           insert into ap_crm.log_camp_elig_base values elig_base(i);
        commit;
        exit when curs_log_elig_base%notfound;
     end loop;
     close curs_log_elig_base;*/
--  execute immediate 'alter session enable parallel dml';
  insert /*+ APPEND */ into ap_crm.log_camp_elig_base
  select  trunc(sysdate)log_date                                  ,to_char(sysdate,'hh24:mi:ss')time_inserted   
	      ,eb.skp_client                                            ,eb.id_cuid
				,trim(eb.name_full) name_full
        ,trim(eb.name_first) name_first                           ,trim(eb.NAME_MIDDLE) name_middle
        ,trim(eb.name_last)name_last                              ,eb.date_birth
        ,trim(eb.name_birth_place)name_birth_place                ,trim(eb.gender) gender
        ,trim(eb.code_employment_type) code_employment_type       ,trim(eb.CODE_EDUCATION_TYPE) CODE_EDUCATION_TYPE
        ,trim(eb.code_employer_industry) code_employer_industry   ,eb.main_income
        ,eb.OTHER_INCOME                                          ,eb.AMT_EXPENSE_DEBT
        ,eb.SUM_AMT_ANNUITY_ACTIVE                                ,eb.total_paid_amount
        ,eb.total_overpaid_amount                                 ,eb.dpd_ever
        ,eb.dpd_3m                                                ,eb.dpd_actual
        ,eb.max_pilot_flag                                        ,eb.NUMBER_OF_CL_CONTRACTS
        ,eb.PREV_PRICING_STRATEGY                                 ,eb.pricing_strategy
        ,eb.valid_from                                            ,eb.valid_to
        ,trim(eb.risk_band)risk_band                              ,trim(eb.rbp_segment_temp)rbp_segment_temp
        ,eb.INTEREST_RATE                                         ,eb.max_tenor
        ,trim(eb.camp_type)camp_type                              ,eb.x_sell_flag
        ,eb.score                                                 ,eb.ANNUITY_LIMIT_FINAL_UPDATED
        ,eb.CA_LIMIT_FINAL_UPDATED                                ,eb.priority_actual
        ,eb.eligible_final_flag                                   ,trim(eb.sid_result) sid_result
        ,trim(eb.pilot_name)                                      ,eb.score_pd
        ,EB.CNT_ACTIVE_CONTRACTS                                  ,eb.fl_current_eligibility, eb.reason_not_elig
  --from ap_risk.eligibility_base eb
  from ap_crm.camp_elig_base eb
  --where priority_actual is not null and to_char(valid_from,'yymm') = to_char(sysdate,'yymm')
  ;
  AP_PUBLIC.CORE_LOG_PKG.pEnd;
  commit;
  pStats('LOG_CAMP_ELIG_BASE');
  
  AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:LOG_CAMP_COMPILED_LIST');
  insert /*+ APPEND */ into ap_crm.log_camp_compiled_list
  select TRUNC(SYSDATE)LOG_DATE, TO_CHAR(SYSDATE,'HH24:MI:SS')TIME_INSERTED, ccl.* from ap_crm.camp_compiled_list ccl;
  AP_PUBLIC.CORE_LOG_PKG.pEnd;
  commit;
  pStats('LOG_CAMP_COMPILED_LIST');


  AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:LOG_CAMP_TDY_CALL_LIST');
  insert /*+ APPEND */ into ap_CRM.log_Camp_Tdy_Call_List
  select trunc(sysdate)log_date, to_char(sysdate, 'hh24:mi:ss')time_inserted, tdy.* from ap_crm.CAMP_TDY_CALL_LIST tdy;
  AP_PUBLIC.CORE_LOG_PKG.pEnd;
  commit;
  pStats('LOG_CAMP_TDY_CALL_LIST');

  AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:LOG_CAMP_OFFER_CALL_LIST_FINAL');
  insert /*+ APPEND */ into AP_CRM.LOG_CAMP_OFFER_CALL_LIST_FINAL
  select trunc(sysdate), to_Char(sysdate,'hh24:mi:ss'),  TCL.* from AP_CRM.CAMP_OFFER_CALL_LIST_FINAL TCL;
  AP_PUBLIC.CORE_LOG_PKG.pEnd;
  commit;
  pStats('LOG_CAMP_OFFER_CALL_LIST_FINAL');
 <<finish_line>> 
AP_PUBLIC.CORE_LOG_PKG.pFinish ;
end;
/

