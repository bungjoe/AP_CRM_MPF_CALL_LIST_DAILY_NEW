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
    
		pTruncate('GTT_CMP_CONT_DCC');
		AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:gtt_cmp_cont_dcc');
		insert /*+ APPEND */ into GTT_CMP_CONT_DCC
		with base as
    (
        select /*+ MATERIALIZE */ skp_Client from camp_elig_base where eligible_final_flag = 1 and priority_actual > 0
        union all
        select skp_Client from camp_orbp_elig_base where eligible_final_flag = 1 and priority_actual > 0
    )
		select /*+ USE_HASH(DCC FCC EB) FULL(DCC) FULL(EB) */ 
					 dcc.skp_client,                  dcc.skp_credit_case, 
					 dcc.skp_application,             dcc.skp_contract,
					 dcc.SKP_SALESROOM_APPL_CREATED,  dcc.skp_credit_status, 
					 dcc.skp_credit_substatus,        dcc.text_contract_number, 
					 dcc.text_cancellation_reason,    dcc.text_credit_status_reason
		from owner_Dwh.dc_Credit_case dcc
		join base eb on dcc.skp_client = eb.skp_client;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pstats('GTT_CMP_CONT_DCC');

		pTruncate('GTT_CMP_CONT_FCC');
		AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:GTT_CMP_CONT_FCC');
		insert /*+ APPEND */ into GTT_CMP_CONT_FCC 
		select /*+ USE_HASH(FCC EB) FULL(FCC) FULL(EB) */ 
					 fcc.skp_client,        fcc.SKP_CREDIT_CASE,
					 fcc.DTIME_PRE_PROCESS, fcc.DTIME_PROCESS, 
					 fcc.DTIME_REJECTION,   fcc.DTIME_CANCELLATION,
					 fcc.DTIME_CLOSE,       fcc.DTIME_APPROVAL, 
					 fcc.DTIME_SIGNATURE,   fcc.DTIME_ACTIVATION
		from owner_Dwh.f_credit_case_ad fcc
		join GTT_CMP_CONT_DCC eb on fcc.skp_client = eb.skp_client 
		 and fcc.SKP_CREDIT_CASE = eb.skp_credit_case;
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pstats('GTT_CMP_CONT_FCC');

		pTruncate('GTT_CMP_CONT_FCB');
		AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:GTT_CMP_CONT_FCB');
		insert /*+ APPEND */ into GTT_CMP_CONT_FCB
		select /*+ USE_HASH(FCB DCC) FULL(FCB) FULL(DCC) */ 
					 fcb.skp_credit_case,   fcb.skp_client, 
					 fcb.skp_contract,      fcb.skp_employee_consultant, 
					 fcb.flag_early_repaid, fcb.flag_gift_payment_used,
					 fcb.amt_credit_total
		from owner_Dwh.f_contract_base_ad fcb
		join GTT_CMP_CONT_DCC dcc on fcb.skp_credit_case = dcc.skp_credit_case;
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pstats('GTT_CMP_CONT_FCB');

    pTruncate('GTT_CMP_CONT_FCE');
		AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:GTT_CMP_CONT_FCE');
		insert /*+ APPEND */ into GTT_CMP_CONT_FCE
		select /*+ USE_HASH(FCE DCC) FULL(FCE) FULL(DCC)*/
					 fce.skp_client,          fce.skp_credit_case,       
					 fce.skp_contract,        fce.amt_annuity,       
					 fce.amt_credit_signed,   fce.amt_down_payment,  
					 fce.amt_fee_origination, fce.amt_goods_price,
					 fce.name_goods_category, fce.name_goods_type,   
					 fce.name_instalment_sched_method,
					 fce.name_producer
		from owner_Dwh.f_Contract_Extension_Ad fce       
		join GTT_CMP_CONT_DCC dcc on fce.skp_credit_case = dcc.skp_credit_case;
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pstats('GTT_CMP_CONT_FCE');

		pTruncate('GTT_CMP_CONT_FCAB');
		AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:GTT_CMP_CONT_FCAB');		
		insert /*+ APPEND */ into GTT_CMP_CONT_FCAB
		select /*+ USE_HASH(FCAB DCC) FULL(FCA) FULL(DCC) */
					 fcab.skp_client,       fcab.skp_credit_case,      
					 fcab.amt_outstanding_principal,
					 fcab.cnt_instalment,   fcab.date_last_payment,
					 case when fcab.date_next_due = to_date('01/01/3000','mm/dd/yyyy') then fcab.date_last_due else fcab.date_next_due end due_date,
					 dcc.text_contract_number
		from owner_Dwh.f_Contract_Aggr_Balance_Ad fcab
		join GTT_CMP_CONT_DCC dcc on fcab.skp_credit_case = dcc.Skp_Credit_Case;
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pstats('GTT_CMP_CONT_FCAB');

		ptruncate('GTT_CMP_CONT_OP');
		AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:GTT_CMP_CONT_OP');		
		insert /*+ APPEND */ into GTT_CMP_CONT_OP
		SELECT /*+ USE_HASH(OP DCC PT PS) FULL(OP) FULL(DCC) */
				OP.SKP_CREDIT_CASE,  op.NAME_RECIPIENT,
				op.AMT_PAYMENT ,     op.DTIME_PAYMENT DISBURSE_DATE ,
				ps.NAME_OUTGOING_PAYMENT_STATUS PAYMENT_STATUS
		FROM OWNER_DWH.F_OUTGOING_PAYMENT_TT OP
		join GTT_CMP_CONT_DCC dcc on op.skp_credit_case = dcc.skp_credit_case and dcc.skp_salesroom_appl_created in (61891,2850271,5871680)
		JOIN OWNER_DWH.CL_OUTGOING_PAYMENT_TYPE PT ON PT.SKP_OUTGOING_PAYMENT_TYPE = OP.SKP_OUTGOING_PAYMENT_TYPE
		JOIN OWNER_DWH.CL_OUTGOING_PAYMENT_STATUS PS ON PS.SKP_OUTGOING_PAYMENT_STATUS = OP.SKP_OUTGOING_PAYMENT_STATUS
		WHERE OP.FLAG_DELETED = 'N'
			AND PT.CODE_OUTGOING_PAYMENT_TYPE = 'CL'
			and ps.NAME_OUTGOING_PAYMENT_STATUS <> 'Cancelled';
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pstats('GTT_CMP_CONT_OP');

		ptruncate('GTT_CMP_CONT_FAE');
		AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:GTT_CMP_CONT_FAE');
		insert /*+ APPEND */ into GTT_CMP_CONT_FAE
		SELECT /*+ USE_HASH(FAE DCC) FULL(FAE) FULL(DCC) */
					fae.skp_credit_case,           fae.SKP_APPLICATION,
					fae.DTIME_APPL_CREATION,       fae.DTIME_APPL_ARRANGED,
					fae.SKP_EMPLOYEE_CREATED_APPL, fae.SKP_EMPLOYEE_ARRANGED_APPL,
					fae.FLAG_APPL_FILLED_OFFLINE
		FROM OWNER_DWH.F_APPLICATION_EVENT_AT fae
		join GTT_CMP_CONT_DCC dcc on fae.skp_credit_case = dcc.skp_credit_case
		WHERE FLAG_DELETED ='N';
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pstats('GTT_CMP_CONT_FAE');

		ptruncate('GTT_CMP_CONT_DCS');
		AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:GTT_CMP_CONT_DCS');		
		insert /*+ APPEND */ into GTT_CMP_CONT_DCS
		select /*+ USE_HASH(DSH DSL CSC DCS)*/ 
					 distinct 
					 dcs.skp_salesroom,       dcs.code_salesroom, 
					 dcs.name_salesroom,      dcs.NUM_GPS_LATITUDE, 
					 dcs.NUM_GPS_LONGTITUDE,  dsl.CODE_SELLER code_partner, 
					 dsl.NAME_SELLER,         dsh.name_sales_business_area, 
					 dsh.name_sales_district, 
					 case when csc.name_seller_category_type = 'KA' then 'Key Account' else csc.name_seller_category_type end partner_category
		from owner_dwh.dc_salesroom dcs
		join GTT_CMP_CONT_DCC dcc on dcc.skp_salesroom_appl_created = dcs.SKP_SALESROOM
		join OWNER_DWH.DC_SALES_HIERARCHY dsh on dcs.SKP_SALES_HIERARCHY = dsh.SKP_SALES_HIERARCHY
		join owner_dwh.dc_seller dsl on dcs.skp_seller = dsl.SKP_SELLER
		join OWNER_DWH.cl_seller_category_type csc on dsl.skp_seller_category_type = csc.skp_seller_category_type
		;
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;    
		pstats('GTT_CMP_CONT_DCS');

		/* MPF Contracts */
		ptruncate('camp_mpf_contracts');
		AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:camp_mpf_contracts');
		insert /*+ APPEND */ into camp_mpf_contracts
		SELECT
    /*+ USE_HASH(DCC FCC FAE FCAB DCS FOP CCS) FULL(DCC) 
        FULL(FCC) FULL(FAE) FULL(FCAB) FULL(DCS) FULL(FOP) 
    */
    dcc.SKP_CREDIT_CASE,               dcc.SKP_APPLICATION,
    dCC.SKP_CLIENT,                    dCC.SKP_CONTRACT,
    dCC.Skp_Salesroom_Appl_Created,    dcs.CODE_SALESROOM,
    dcs.NAME_SALESROOM,                dcc.text_contract_number,
    ccs.NAME_CREDIT_STATUS,            fae.DTIME_APPL_CREATION,
    fae.DTIME_APPL_ARRANGED,           fcc.DTIME_PRE_PROCESS,
    null SEND_TO_IDENTIFICATE,         fcc.DTIME_PROCESS,
    null SENT_TO_EVALUATE,             fcc.DTIME_APPROVAL,
    fcc.DTIME_REJECTION,               fcc.DTIME_SIGNATURE,
    null DATE_DECISION,                fop.NAME_RECIPIENT,
    fop.AMT_PAYMENT,                   fop.DISBURSE_DATE,
    fcc.DTIME_ACTIVATION,              fcab.DUE_DATE,
    fop.PAYMENT_STATUS,                fcc.DTIME_CANCELLATION,
    dcc.TEXT_CANCELLATION_REASON,      dcc.TEXT_CREDIT_STATUS_REASON,
    null FLAG_CANCELLED_AUTOMATICALLY, null FLAG_CANCELLED_PRE_APPROVAL
    FROM GTT_CMP_CONT_DCC dcc
    join GTT_CMP_CONT_FCC fcc on dcc.skp_credit_case = fcc.skp_credit_case 
    join GTT_CMP_CONT_FAE fae on dcc.Skp_Credit_Case = fae.skp_credit_case
    join GTT_CMP_CONT_FCAB fcab on dcc.Skp_Credit_Case = fcab.skp_credit_case
    left join GTT_CMP_CONT_OP fop on dcc.skp_credit_case = fop.skp_credit_case
    join owner_Dwh.dc_salesroom dcs on dcc.skp_salesroom_appl_created = dcs.SKP_SALESROOM
    join owner_dwh.cl_credit_status ccs on dcc.skp_credit_status = ccs.SKP_CREDIT_STATUS
    where dcc.skp_salesroom_appl_created in (61891,2850271,5871680);
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pstats('camp_mpf_contracts');

    /* POS Contracts */
		ptruncate('camp_pos_contracts');
		AP_PUBLIC.CORE_LOG_PKG.pStart('Ins:camp_pos_contracts');		
		insert /*+ APPEND */ into camp_pos_contracts
		select /*+  USE_HASH(FCB FCC FCE FAE FCEB FOP DCS DCE CCS DCC) */
				 dcc.SKP_Credit_Case                ,dcc.SKP_CONTRACT
				,dcc.SKP_APPLICATION                ,dcc.SKP_CLIENT
				,fcb.SKP_EMPLOYEE_CONSULTANT        ,dcc.text_contract_number
				,CCs.name_credit_status             ,null DTIME_PROPOSAL
				,null SENT_TO_EVALUATE              ,null DATE_DECISION
				,fcc.DTIME_CANCELLATION             ,fcc.dtime_signature
				,fcc.dtime_activation               ,fcc.DTIME_CLOSE
				,fcab.date_last_payment             ,fcab.due_date
				,null Flag_Cancelled_Automatically  ,null Flag_Cancelled_PRE_APPROVAL
				,null FLAG_DELETED                  ,fce.name_producer
				,fce.name_goods_type                ,fce.name_goods_category
				,fce.amt_goods_price                ,fce.AMT_DOWN_PAYMENT
				,fcb.amt_credit_total               ,fce.amt_annuity
				,fcab.cnt_instalment                ,fce.amt_fee_origination
				,null RATE_EFFECTIVE_INTEREST       ,fce.NAME_INSTALMENT_SCHED_METHOD
				,fcab.amt_outstanding_principal     ,dce.CODE_EMPLOYEE
				,dce.NAME_COMMON name_sales_agent   ,dcs.code_salesroom
				,dcs.name_salesroom                 ,dcs.NUM_GPS_LATITUDE             
				,dcs.NUM_GPS_LONGTITUDE             ,dcs.code_partner                 
				,dcs.name_seller                    ,dcs.partner_category             
				,dcs.name_sales_business_area       ,dcs.name_sales_district
    from GTT_CMP_CONT_DCC dcc
    join GTT_CMP_CONT_FCB fcb on dcc.skp_credit_case = fcb.skp_credit_case 
    join GTT_CMP_CONT_FCC fcc on dcc.skp_credit_case = fcc.skp_credit_case 
    join GTT_CMP_CONT_FCE fce on dcc.skp_credit_case = fce.skp_credit_case
    join GTT_CMP_CONT_FAE fae on dcc.Skp_Credit_Case = fae.skp_credit_case
    join GTT_CMP_CONT_FCAB fcab on dcc.Skp_Credit_Case = fcab.skp_credit_case
    join GTT_CMP_CONT_DCS dcs on dcc.skp_salesroom_appl_created = dcs.skp_salesroom
    left join GTT_CMP_CONT_OP fop on dcc.skp_credit_case = fop.skp_credit_case
    left join owner_dwh.dc_employee dce on fcb.skp_employee_consultant = dce.SKP_EMPLOYEE
    join owner_dwh.cl_credit_status ccs on dcc.skp_credit_status = ccs.SKP_CREDIT_STATUS
    where dcc.skp_salesroom_appl_created not in (61891,2850271,5871680);
    ap_public.core_log_pkg.pEnd;
		commit;
    pstats('camp_pos_contracts');     
    
    AP_PUBLIC.CORE_LOG_PKG.pFinish ;
end;
