CREATE OR REPLACE PROCEDURE CAMP_RAW_CONTRACTS AS

  PROCEDURE pStats( acTable VARCHAR2, anPerc NUMBER DEFAULT 0.01) IS
  BEGIN
    AP_PUBLIC.CORE_LOG_PKG.pStart( 'Stat:'||acTable );
    DBMS_STATS.Gather_Table_Stats( OwnName => 'AP_CRM', TabName => upper(acTable),Estimate_Percent => anPerc );
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
  END ;
  PROCEDURE pTruncate( acTable VARCHAR2) IS
  BEGIN
    AP_PUBLIC.CORE_LOG_PKG.pStart( 'Trunc:'||acTable );
    EXECUTE IMMEDIATE 'TRUNCATE TABLE AP_CRM.'||upper(acTable) ;
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
  END ;

BEGIN 
    AP_PUBLIC.CORE_LOG_PKG.pInit( 'AP_CRM', 'CAMP_RAW_CONTRACTS');
    
    AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:gtt_cmp_raw_cont_fca');
    insert /*+ APPEND */ into gtt_cmp_raw_cont_fca
    with base as
    (
        select /*+ MATERIALIZE */ skp_Client from camp_elig_base where eligible_final_flag = 1 and priority_actual > 0
        union all
        select skp_Client from camp_orbp_elig_base where eligible_final_flag = 1 and priority_actual > 0
    )
    select /*+ USE_HASH (FCA DCE)*/
         fca.skp_credit_case              ,fca.SKP_EMPLOYEE_CONSULTANT
        ,fex.name_producer                ,fex.name_goods_type
        ,fex.name_goods_category          ,fex.amt_goods_price
        ,fex.AMT_DOWN_PAYMENT             ,fca.amt_credit_total
        ,fex.amt_annuity                  ,bal.cnt_instalment
        ,fex.amt_fee_origination          ,fca.RATE_EFFECTIVE_INTEREST
        ,fex.NAME_INSTALMENT_SCHED_METHOD ,fca.dtime_signature_contract
        ,fca.dtime_activation             ,fca.DTIME_CLOSE
        ,bal.amt_outstanding_principal    ,bal.dtime_payment_last
        ,dce.code_employee                ,dce.name_common
    from owner_Dwh.f_contract_base_ad fca
    join owner_dwh.f_contract_extension_ad fex on fca.skp_credit_case = fex.skp_credit_case
    join owner_Dwh.f_Contract_Aggr_Balance_Ad bal on fca.skp_credit_case = bal.skp_credit_case
    join owner_Dwh.dc_employee dce on fca.skp_employee_consultant = dce.skp_employee
    where fca.skp_client in (select nvl(skp_client,-9999) from base);
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
    commit;
    pStats('gtt_cmp_raw_cont_fca');

    AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:gtt_cmp_raw_cont_cc2');
    insert /*+ APPEND */ into gtt_cmp_raw_cont_cc2
    with base as
    (
        select /*+ MATERIALIZE */ skp_Client from camp_elig_base where eligible_final_flag = 1 and priority_actual > 0
        union all
        select skp_Client from camp_orbp_elig_base where eligible_final_flag = 1 and priority_actual > 0
    )
		select /*+ USE_HASH(FCA CL CC) */ cc.SKP_Credit_Case, cc.SKP_CONTRACT, cc.SKP_APPLICATION, cc.SKP_CLIENT
    ,fca.SKP_EMPLOYEE_CONSULTANT      ,cc.skp_salesroom
    ,cc.text_contract_number          ,cl.name_credit_status
    ,cc.DTIME_PROPOSAL                ,cc.DTIME_APPL_SENT_TO_EVALUATION SENT_TO_EVALUATE
    ,cc.DATE_DECISION                 ,cc.DTIME_CANCELLATION
    ,cc.Flag_Cancelled_Automatically  ,cc.Flag_Cancelled_PRE_APPROVAL
    ,cc.FLAG_DELETED                  , fca.name_producer
    , fca.name_goods_type             , fca.name_goods_category
    , fca.amt_goods_price             , fca.AMT_DOWN_PAYMENT
    , fca.amt_credit_total            , fca.amt_annuity
    , fca.CNT_INSTALMENT instalment   , fca.amt_fee_origination
    , fca.RATE_EFFECTIVE_INTEREST     , fca.NAME_INSTALMENT_SCHED_METHOD
    , fca.dtime_signature_contract    , fca.dtime_activation
    , fca.DTIME_CLOSE                 , fca.amt_outstanding_principal
    , fca.dtime_payment_last          , fca.code_employee code_sales_agent
    , fca.name_common name_sales_agent
    from OWNER_DWH.DC_CREDIT_CASE CC 
    join owner_dwh.CL_CREDIT_STATUS CL on cc.skp_credit_status=cl.skp_credit_status
    join gtt_cmp_raw_cont_fca fca on cc.skp_credit_case = fca.skp_Credit_case
    where CC.SKP_SALESROOM not in (61891, 2850271, 5871680) and cc.FLAG_DELETED ='N'
    and cc.SKP_CREDIT_STATUS in (1, 2, 4, 6, 7, 8, 9, 10)
    and cc.skp_Client in (select nvl(skp_client,-9999) from base);
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
    commit;
    pStats('gtt_cmp_raw_cont_cc2');
    
    AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:gtt_cmp_raw_cont_fd');
    insert /*+ APPEND */ into gtt_cmp_raw_cont_fd
    select  skp_contract ,min(to_date(trunc(date_instalment))) due_date 
    from owner_dwh.f_instalment_head_ad 
    where num_instalment_number=1 and num_instalment_order = 1 and skp_contract in (select SKP_CONTRACT from gtt_cmp_raw_cont_cc2)
    group by skp_contract;
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
    commit;
    pStats('gtt_cmp_raw_cont_fd');

    AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:gtt_cmp_raw_cont_dcs' );
    insert /*+ APPEND */ into gtt_cmp_raw_cont_dcs
    select /*+ USE_HASH(DSH DSL CSC DCS)*/ distinct dcs.skp_salesroom, dcs.code_salesroom, dcs.name_salesroom, dcs.NUM_GPS_LATITUDE, dcs.NUM_GPS_LONGTITUDE
    , dsl.CODE_SELLER code_partner, dsl.NAME_SELLER name_partner , dsh.name_sales_business_area, dsh.name_sales_district
    , case when csc.name_seller_category_type = 'KA' then 'Key Account' else csc.name_seller_category_type end partner_category
    from owner_dwh.dc_salesroom dcs
    join OWNER_DWH.DC_SALES_HIERARCHY dsh on dcs.SKP_SALES_HIERARCHY = dsh.SKP_SALES_HIERARCHY
    join owner_dwh.dc_seller dsl on dcs.skp_seller = dsl.SKP_SELLER
    join OWNER_DWH.cl_seller_category_type csc on dsl.skp_seller_category_type = csc.skp_seller_category_type
    where dcs.skp_salesroom in (select nvl(skp_salesroom,-9999) from gtt_cmp_raw_cont_cc2);
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
    commit;
    pStats('gtt_cmp_raw_cont_dcs');

    pTruncate('CAMP_POS_CONTRACTS');
    AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:CAMP_POS_CONTRACTS' );
    insert /*+ APPEND */ into ap_crm.camp_pos_contracts
    select /*+  USE_HASH(CC DCS FD) */
    cc.SKP_Credit_Case                ,cc.SKP_CONTRACT
    ,cc.SKP_APPLICATION               ,cc.SKP_CLIENT
    ,cc.SKP_EMPLOYEE_CONSULTANT       ,cc.text_contract_number
    ,CC.name_credit_status           ,cc.DTIME_PROPOSAL
    ,cc.SENT_TO_EVALUATE              ,cc.DATE_DECISION
    ,cc.DTIME_CANCELLATION            ,cc.dtime_signature_contract
    ,cc.dtime_activation              ,cc.DTIME_CLOSE
    ,cc.dtime_payment_last            ,fd.due_date
    ,cc.Flag_Cancelled_Automatically  ,cc.Flag_Cancelled_PRE_APPROVAL
    ,cc.FLAG_DELETED                  ,cc.name_producer
    ,cc.name_goods_type               ,cc.name_goods_category
    ,cc.amt_goods_price               ,cc.AMT_DOWN_PAYMENT
    ,cc.amt_credit_total              ,cc.amt_annuity
    ,cc.instalment                    ,cc.amt_fee_origination
    ,cc.RATE_EFFECTIVE_INTEREST       ,cc.NAME_INSTALMENT_SCHED_METHOD
    ,cc.amt_outstanding_principal     ,cc.code_sales_agent, cc.name_sales_agent
    ,dcs.code_salesroom               ,dcs.name_salesroom
    ,dcs.NUM_GPS_LATITUDE             ,dcs.NUM_GPS_LONGTITUDE
    ,dcs.code_partner                 ,dcs.name_partner
    ,dcs.partner_category             ,dcs.name_sales_business_area
    ,dcs.name_sales_district
    from gtt_cmp_raw_cont_cc2 cc
    left join gtt_cmp_raw_cont_dcs dcs on cc.skp_salesroom = dcs.skp_salesroom
    left join gtt_cmp_raw_cont_fd fd on cc.skp_contract = fd.skp_contract;
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
    commit;
    pStats('CAMP_POS_CONTRACTS');

  AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:gtt_cmp_raw_cont_cc');
  insert /*+ APPEND */ into gtt_cmp_raw_cont_cc
  with base as
    (
        select /*+ MATERIALIZE */ skp_Client from camp_elig_base where eligible_final_flag = 1 and priority_actual > 0
        union all
        select skp_Client from camp_orbp_elig_base where eligible_final_flag = 1 and priority_actual > 0
    )
	SELECT 
    /*+ USE_HASH (CC CL) */
    SKP_CREDIT_CASE,
    SKP_CONTRACT,
    SKP_APPLICATION,
    SKP_CLIENT,
    TEXT_CONTRACT_NUMBER CONTRACT,
    NAME_CREDIT_STATUS ,
    DTIME_PROPOSAL SEND_TO_IDENTIFICATE ,
    DTIME_APPL_SENT_TO_EVALUATION SENT_TO_EVALUATE ,
    DATE_DECISION ,
    DTIME_CANCELLATION ,
    FLAG_CANCELLED_AUTOMATICALLY,
    FLAG_CANCELLED_PRE_APPROVAL,
    CC.FLAG_DELETED ,
    CC.SKP_SALESROOM,
    DCS.CODE_SALESROOM,
    UPPER(DCS.NAME_SALESROOM) AS NAME_SALESROOM
  FROM OWNER_DWH.DC_CREDIT_CASE CC
  JOIN OWNER_DWH.CL_CREDIT_STATUS CL
  ON CC.SKP_CREDIT_STATUS=CL.SKP_CREDIT_STATUS
  JOIN OWNER_DWH.DC_SALESROOM DCS
  ON CC.SKP_SALESROOM     = DCS.SKP_SALESROOM
  WHERE CC.SKP_SALESROOM IN ('61891','2850271','5871680')
  AND CC.FLAG_DELETED     ='N'
  AND CC.SKP_CLIENT       IN
    (SELECT NVL(SKP_CLIENT,-9999) FROM BASE );
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
    commit;
    pStats('gtt_cmp_raw_cont_cc');

  AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:gtt_cmp_raw_cont_op');
 insert /*+ APPEND */ into gtt_cmp_raw_cont_op 
 SELECT
    OP.SKP_CREDIT_CASE,
    NAME_RECIPIENT,
    AMT_PAYMENT ,
    DTIME_PAYMENT DISBURSE_DATE ,
    NAME_OUTGOING_PAYMENT_STATUS PAYMENT_STATUS
  FROM OWNER_DWH.F_OUTGOING_PAYMENT_TT OP
  JOIN OWNER_DWH.CL_OUTGOING_PAYMENT_TYPE PT
  ON PT.SKP_OUTGOING_PAYMENT_TYPE = OP.SKP_OUTGOING_PAYMENT_TYPE
  JOIN OWNER_DWH.CL_OUTGOING_PAYMENT_STATUS PS
  ON PS.SKP_OUTGOING_PAYMENT_STATUS = OP.SKP_OUTGOING_PAYMENT_STATUS
  WHERE OP.FLAG_DELETED             = 'N'
  AND OP.SKP_CREDIT_CASE           IN
    (SELECT NVL(SKP_CREDIT_CASE,-9999999) FROM gtt_cmp_raw_cont_cc)
  AND PT.CODE_OUTGOING_PAYMENT_TYPE = 'CL';
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
    commit;
    pStats('gtt_cmp_raw_cont_op');

  AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:gtt_cmp_raw_cont_fcc');
    insert /*+ APPEND */ into gtt_cmp_raw_cont_fcc
    SELECT
    FE.SKP_CREDIT_CASE ,
    FE.DTIME_PRE_PROCESS ,
    FE.DTIME_PROCESS ,
    FE.DTIME_REJECTION ,
    FE.DTIME_APPROVAL ,
    FE.DTIME_SIGNATURE ,
    FE.DTIME_ACTIVATION ,
    FE.TEXT_CANCELLATION_REASON,
    FE.TEXT_CREDIT_STATUS_REASON
  FROM OWNER_DWH.F_CREDIT_CASE_AD FE
  JOIN gtt_cmp_raw_cont_cc cc ON FE.SKP_CREDIT_CASE=CC.SKP_CREDIT_CASE
  WHERE FE.FLAG_DELETED   = 'N';
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
    commit;
    pStats('gtt_cmp_raw_cont_fcc');

  AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:gtt_cmp_raw_cont_apev');  
  insert /*+ APPEND */ into gtt_cmp_raw_cont_apev
  SELECT
    SKP_APPLICATION ,
    DTIME_APPL_CREATION ,
    DTIME_APPL_ARRANGED ,
    SKP_EMPLOYEE_CREATED_APPL,
    SKP_EMPLOYEE_ARRANGED_APPL,
    FLAG_APPL_FILLED_OFFLINE
  FROM OWNER_DWH.F_APPLICATION_EVENT_AT
  WHERE FLAG_DELETED   ='N'
  AND SKP_CREDIT_CASE IN
    (SELECT NVL(SKP_CREDIT_CASE,-9999999) FROM gtt_cmp_raw_cont_cc
    );
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
    commit;
    pStats('gtt_cmp_raw_cont_apev');
  
  AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:gtt_cmp_raw_cont_fd');
  insert /*+ APPEND */ into gtt_cmp_raw_cont_fd
   SELECT SKP_CONTRACT ,
    MIN(TO_DATE(TRUNC(DATE_INSTALMENT))) DUE_DATE
  FROM OWNER_DWH.F_INSTALMENT_HEAD_AD
  WHERE NUM_INSTALMENT_NUMBER=1
  AND NUM_INSTALMENT_ORDER   = 1
  AND SKP_CONTRACT          IN
    (SELECT NVL(SKP_CONTRACT,-999999999) FROM gtt_cmp_raw_cont_cc )
  GROUP BY SKP_CONTRACT ;
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
    commit;
    pStats('gtt_cmp_raw_cont_fd');

    pTruncate('CAMP_MPF_CONTRACTS');
    AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:CAMP_MPF_CONTRACTS');
    insert /*+ APPEND */ into ap_crm.camp_mpf_contracts    
    SELECT
    /*+  USE_HASH (CC OP App_event fcc First_due) */
    DISTINCT CC.SKP_CREDIT_CASE,
    CC.SKP_APPLICATION,
    CC.SKP_CLIENT,
    CC.SKP_CONTRACT ,
    CC.SKP_SALESROOM,
    CC.CODE_SALESROOM,
    CC.NAME_SALESROOM,
    CONTRACT ,
    NAME_CREDIT_STATUS ,
    DTIME_APPL_CREATION ,
    DTIME_APPL_ARRANGED ,
    DTIME_PRE_PROCESS ,
    SEND_TO_IDENTIFICATE ,
    DTIME_PROCESS ,
    SENT_TO_EVALUATE ,
    DTIME_APPROVAL ,
    DTIME_REJECTION ,
    DTIME_SIGNATURE ,
    CC.DATE_DECISION ,
    NAME_RECIPIENT ,
    AMT_PAYMENT ,
    DISBURSE_DATE ,
    DTIME_ACTIVATION ,
    DUE_DATE ,
    PAYMENT_STATUS ,
    DTIME_CANCELLATION ,
    TEXT_CANCELLATION_REASON ,
    TEXT_CREDIT_STATUS_REASON ,
    FLAG_CANCELLED_AUTOMATICALLY ,
    FLAG_CANCELLED_PRE_APPROVAL
    FROM gtt_cmp_raw_cont_cc CC
    left JOIN gtt_cmp_raw_cont_OP op  ON CC.SKP_CREDIT_CASE = OP.SKP_CREDIT_CASE
    left JOIN gtt_cmp_raw_cont_apev APP_EVENT ON APP_EVENT.SKP_APPLICATION=CC.SKP_APPLICATION
    left JOIN gtt_cmp_raw_cont_fcc FCC ON FCC.SKP_CREDIT_CASE=CC.SKP_CREDIT_CASE
    left JOIN gtt_cmp_raw_cont_fd FIRST_DUE ON FIRST_DUE.SKP_CONTRACT=CC.SKP_CONTRACT;
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
    commit;
    pStats('CAMP_MPF_CONTRACTS');
    
    AP_PUBLIC.CORE_LOG_PKG.pFinish ;
end;
