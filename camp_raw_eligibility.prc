CREATE OR REPLACE PROCEDURE "CAMP_RAW_ELIGIBILITY" AS

  PROCEDURE pStats( acTable VARCHAR2, anPerc NUMBER DEFAULT 0.01) IS
  BEGIN
    AP_PUBLIC.CORE_LOG_PKG.pStart( 'Stat:'||upper(acTable) );
    DBMS_STATS.Gather_Table_Stats( OwnName => 'AP_CRM', TabName => upper(acTable),Estimate_Percent => anPerc );
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
  END ;
  PROCEDURE pTruncate( acTable VARCHAR2)
    IS
  BEGIN
    AP_PUBLIC.CORE_LOG_PKG.pStart( 'Trunc:'||upper(acTable) );
    EXECUTE IMMEDIATE 'TRUNCATE TABLE AP_CRM.'||upper(acTable) ;
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
  END ;

BEGIN
  AP_PUBLIC.CORE_LOG_PKG.pInit( 'AP_CRM', 'CAMP_RAW_ELIGIBILITY') ;
if trunc(sysdate) <= to_date('08/25/2018','mm/dd/yyyy') then
  goto finish_line;
end if;
    
    pTruncate('CAMP_OFFER_CALL_LIST_FINAL');
    ptruncate('camp_compiled_list');
    pTruncate('CAMP_TDY_CALL_LIST');
    ptruncate('camp_alt_offer_call_list');
    ptruncate('camp_offer_cl_spc_final');
    
    AP_PUBLIC.CORE_LOG_PKG.pStart('Chk:IS_DWH_LOAD_FINISHED');
    ap_ops.p_is_dwh_load_finished('AP_CRM','CHAIN_FF_CL',null);
    AP_PUBLIC.CORE_LOG_PKG.pEnd;

    pTruncate('gtt_camp_client_at');
		AP_PUBLIC.CORE_LOG_PKG.pStart('INS:gtt_camp_client_at');
		insert /*+ APPEND */ into gtt_camp_client_at
		select /*+ */
					SKF_CAMPAIGN_CLIENT ,CODE_SOURCE_SYSTEM
					,ID_SOURCE          ,DATE_EFFECTIVE
					,SKP_PROC_INSERTED  ,SKP_PROC_UPDATED
					,FLAG_DELETED       ,SKP_CAMPAIGN
					,SKP_CLIENT         ,SKP_CREDIT_CASE
					,SKP_GOODS_TYPE     ,SKP_MARKETING_ACTION
					,ID_CAMPAIGN        ,CODE_SEGMENT
					,CODE_CAMPAIGN_TYPE ,CODE_CAMPAIGN_SUB_TYPE
					,CODE_PRODUCT_TYPE  ,NAME_OFFER
					,FLAG_OFFER         ,AMT_CREDIT_MAX
					,AMT_ANNUITY_MAX    ,AMT_DOWN_PAYMENT_MIN
					,DATE_VALID_FROM    ,DATE_VALID_TO
					,FLAG_RESPONDED     ,SKP_CAMPAIGN_SUBTYPE
					,SKP_CAMPAIGN_TYPE  ,CNT_CAMPAIGN_CLIENT
					,DTIME_EXPIRATION_OFFER, FLAG_ACTIVE
					,FLAG_RECALCULATED  ,ID_OFFER
		from owner_dwh.f_campaign_client_at cca
		where (skp_client, date_valid_from) in
		(
				select skp_client, max(date_valid_from)date_valid_from from owner_dwh.f_campaign_client_at 
				where trunc(sysdate) between date_valid_from and date_valid_to and flag_active = 'Y'
				group by skp_client
		)and cca.flag_active = 'Y';
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pStats('gtt_camp_client_at');
		
		pTruncate('camp_client_at');
		AP_PUBLIC.CORE_LOG_PKG.pStart('INS:CAMP_CLIENT_AT');
		insert /*+ APPEND */ into ap_Crm.CAMP_CLIENT_AT
    select /*+ */
        SKF_CAMPAIGN_CLIENT ,CODE_SOURCE_SYSTEM
        ,ID_SOURCE          ,DATE_EFFECTIVE
        ,SKP_PROC_INSERTED  ,SKP_PROC_UPDATED
        ,FLAG_DELETED       ,SKP_CAMPAIGN
        ,SKP_CLIENT         ,SKP_CREDIT_CASE
        ,SKP_GOODS_TYPE     ,SKP_MARKETING_ACTION
        ,ID_CAMPAIGN        ,CODE_SEGMENT
        ,CODE_CAMPAIGN_TYPE ,CODE_CAMPAIGN_SUB_TYPE
        ,CODE_PRODUCT_TYPE  ,NAME_OFFER
        ,FLAG_OFFER         ,AMT_CREDIT_MAX
        ,AMT_ANNUITY_MAX    ,AMT_DOWN_PAYMENT_MIN
        ,DATE_VALID_FROM    ,DATE_VALID_TO
        ,FLAG_RESPONDED     ,SKP_CAMPAIGN_SUBTYPE
        ,SKP_CAMPAIGN_TYPE  ,CNT_CAMPAIGN_CLIENT
        ,DTIME_EXPIRATION_OFFER, FLAG_ACTIVE
        ,FLAG_RECALCULATED  ,ID_OFFER 
    from gtt_camp_client_at where flag_recalculated = 'N';
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pStats('CAMP_CLIENT_AT');

    pTruncate('camp_orbp_offer');
		AP_PUBLIC.CORE_LOG_PKG.pStart('INS:camp_orbp_offer');
    insert /*+ APPEND */ into ap_Crm.camp_orbp_offer
    select /*+ */
        SKF_CAMPAIGN_CLIENT ,CODE_SOURCE_SYSTEM
        ,ID_SOURCE          ,DATE_EFFECTIVE
        ,SKP_PROC_INSERTED  ,SKP_PROC_UPDATED
        ,FLAG_DELETED       ,SKP_CAMPAIGN
        ,SKP_CLIENT         ,SKP_CREDIT_CASE
        ,SKP_GOODS_TYPE     ,SKP_MARKETING_ACTION
        ,ID_CAMPAIGN        ,CODE_SEGMENT
        ,CODE_CAMPAIGN_TYPE ,CODE_CAMPAIGN_SUB_TYPE
        ,CODE_PRODUCT_TYPE  ,NAME_OFFER
        ,FLAG_OFFER         ,AMT_CREDIT_MAX
        ,AMT_ANNUITY_MAX    ,AMT_DOWN_PAYMENT_MIN
        ,DATE_VALID_FROM    ,DATE_VALID_TO
        ,FLAG_RESPONDED     ,SKP_CAMPAIGN_SUBTYPE
        ,SKP_CAMPAIGN_TYPE  ,CNT_CAMPAIGN_CLIENT
        ,DTIME_EXPIRATION_OFFER, FLAG_ACTIVE
        ,FLAG_RECALCULATED  ,ID_OFFER 
    from gtt_camp_client_at where flag_recalculated = 'Y';
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pStats('camp_orbp_offer');

    pTruncate('camp_offer_recalculation');
		AP_PUBLIC.CORE_LOG_PKG.pStart('INS:camp_offer_recalculation');
		insert /*+ APPEND */ into camp_offer_recalculation
		select skf_offer_recalculation, skp_client, skf_offer_rec_main, skf_campaign_client, id_offer, flag_deactivated, id_campaign, 
					 date_valid_from, date_valid_to, date_expiration_offer, amt_credit_max, amt_instalment_max, text_pricing_category, cnt_instalment, rate_interest,
					 dtime_obod_generated, dtime_obod_submitted, num_group_position_1 
		from owner_dwh.f_offer_recalculation_tt a 
		where (skp_client, id_campaign) in
					 (
							 select skp_Client, id_campaign from camp_orbp_offer where flag_active = 'Y'
					 )
--			and a.FLAG_DEACTIVATED = 'N' /* Turn on this line after data pattern is confirmed after offer is recalculated */       
		;
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pStats('camp_offer_recalculation');
		
    pTruncate('gtt_camp_elig_base2');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS:gtt_camp_elig_base2');
    INSERT /*+ APPEND */ into gtt_camp_elig_base2
    select 
         eb.skp_client                                     ,eb.id_cuid
        ,eb.name_full                                      ,eb.name_first 
        ,eb.name_middle                                    ,eb.name_last
        ,eb.date_birth                                     ,eb.name_birth_place
        ,eb.gender gender                                  ,eb.code_employment_type code_employment_type
        ,eb.CODE_EDUCATION_TYPE                            ,eb.code_employer_industry
        ,eb.main_income                                    ,eb.OTHER_INCOME
        ,eb.AMT_EXPENSE_DEBT                               ,eb.SUM_AMT_ANNUITY_ACTIVE
        ,eb.total_paid_amount                              ,eb.total_overpaid_amount
        ,eb.dpd_ever                                       ,eb.dpd_3m
        ,eb.dpd_actual                                     ,null max_pilot_flag
        ,eb.NUMBER_OF_CL_CONTRACTS                         ,null PREV_PRICING_STRATEGY
        ,eb.pricing_strategy                               ,eb.valid_from
        ,eb.valid_to                                       ,eb.risk_band
        ,eb.rbp_segment_temp                               ,eb.INTEREST_RATE
        ,eb.max_tenor                                      ,eb.camp_type
        ,eb.x_sell_flag                                    ,eb.score
        ,eb.ANNUITY_LIMIT_FINAL_UPDATED                    ,eb.CA_LIMIT_FINAL_UPDATED
        ,eb.priority_actual                                ,eb.eligible_final_flag
        ,eb.sid_result                                     ,eb.pilot_name
        ,eb.score_pd                                       ,EB.CNT_ACTIVE_CONTRACTS 
        ,eb.fl_current_eligibility                         ,EB.REASON_NOT_ELIG
        /*,eb.camp_month_calc*/                            
				,row_number() over (partition by eb.skp_client order by eb.valid_from desc) nums
    from ap_risk.eligibility_base eb
		join gtt_camp_client_at ofr on eb.skp_client = ofr.skp_client
    where camp_month_calc between add_months(trunc(sysdate,'MM')-1,-1) and last_Day(trunc(sysdate))
		  and priority_actual is not null; 
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pStats('gtt_camp_elig_base2');
	
		pTruncate('CAMP_ELIG_BASE');
		AP_PUBLIC.CORE_LOG_PKG.pStart('INS:CAMP_ELIG_BASE');
		insert /*+ APPEND */ into camp_elig_base
    select 
         eb.skp_client                                     ,eb.id_cuid
        ,trim(eb.name_full) name_full                      ,trim(eb.name_first) name_first
        ,trim(eb.NAME_MIDDLE) name_middle                  ,trim(eb.name_last)name_last
        ,eb.date_birth                                     ,trim(eb.name_birth_place)name_birth_place
        ,trim(eb.gender) gender                            ,trim(eb.code_employment_type) code_employment_type
        ,trim(eb.CODE_EDUCATION_TYPE) CODE_EDUCATION_TYPE  ,trim(eb.code_employer_industry) code_employer_industry
        ,eb.main_income                                    ,eb.OTHER_INCOME
        ,eb.AMT_EXPENSE_DEBT                               ,eb.SUM_AMT_ANNUITY_ACTIVE
        ,eb.total_paid_amount                              ,eb.total_overpaid_amount
        ,eb.dpd_ever                                       ,eb.dpd_3m
        ,eb.dpd_actual                                     ,null max_pilot_flag
        ,eb.NUMBER_OF_CL_CONTRACTS                         ,null PREV_PRICING_STRATEGY
        ,eb.pricing_strategy                               ,eb.valid_from
        ,eb.valid_to                                       ,trim(eb.risk_band)risk_band
        ,trim(eb.rbp_segment_temp)rbp_segment_temp         ,eb.INTEREST_RATE
        ,eb.max_tenor                                      ,trim(eb.camp_type)camp_type
        ,eb.x_sell_flag                                    ,eb.score
        ,eb.ANNUITY_LIMIT_FINAL_UPDATED                    ,eb.CA_LIMIT_FINAL_UPDATED
        ,eb.priority_actual                                ,eb.eligible_final_flag
        ,trim(eb.sid_result) sid_result                    ,trim(eb.pilot_name)
        ,eb.score_pd                                       ,EB.CNT_ACTIVE_CONTRACTS 
        ,eb.fl_current_eligibility                         ,EB.REASON_NOT_ELIG
    from gtt_camp_elig_base2 eb
    where (skp_Client, valid_from, valid_to) in (select skp_Client, date_valid_from, date_valid_to from camp_client_at);
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pStats('CAMP_ELIG_BASE');

    pTruncate('camp_orbp_elig_base');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS:camp_orbp_elig_base');
    insert /*+ APPEND */ into camp_orbp_elig_base
    select 
         eb.skp_client                                     ,eb.id_cuid
        ,trim(eb.name_full) name_full                      ,trim(eb.name_first) name_first
        ,trim(eb.NAME_MIDDLE) name_middle                  ,trim(eb.name_last)name_last
        ,eb.date_birth                                     ,trim(eb.name_birth_place)name_birth_place
        ,trim(eb.gender) gender                            ,trim(eb.code_employment_type) code_employment_type
        ,trim(eb.CODE_EDUCATION_TYPE) CODE_EDUCATION_TYPE  ,trim(eb.code_employer_industry) code_employer_industry
        ,eb.main_income                                    ,eb.OTHER_INCOME
        ,eb.AMT_EXPENSE_DEBT                               ,eb.SUM_AMT_ANNUITY_ACTIVE
        ,eb.total_paid_amount                              ,eb.total_overpaid_amount
        ,eb.dpd_ever                                       ,eb.dpd_3m
        ,eb.dpd_actual                                     ,null max_pilot_flag
        ,eb.NUMBER_OF_CL_CONTRACTS                         ,null PREV_PRICING_STRATEGY
        ,eb.pricing_strategy                               ,eb.valid_from
        ,eb.valid_to                                       ,trim(eb.risk_band)risk_band
        ,trim(eb.rbp_segment_temp)rbp_segment_temp         ,eb.INTEREST_RATE
        ,eb.max_tenor                                      ,trim(eb.camp_type)camp_type
        ,eb.x_sell_flag                                    ,eb.score
        ,eb.ANNUITY_LIMIT_FINAL_UPDATED                    ,eb.CA_LIMIT_FINAL_UPDATED
        ,eb.priority_actual                                ,eb.eligible_final_flag
        ,trim(eb.sid_result) sid_result                    ,trim(eb.pilot_name)
        ,eb.score_pd                                       ,EB.CNT_ACTIVE_CONTRACTS 
        ,eb.fl_current_eligibility                         ,EB.REASON_NOT_ELIG
    from gtt_camp_elig_base2 eb
    where (skp_Client, valid_from, valid_to) in (select skp_Client, date_valid_from, valid_to from camp_orbp_offer);
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('camp_orbp_elig_base');
  
		pTruncate('CAMP_ELIG_DAILY_CHECK');
		AP_PUBLIC.CORE_LOG_PKG.pStart('INS:CAMP_ELIG_DAILY_CHECK');
    insert  /*+ APPEND */ into camp_elig_daily_check
    select tgt.CAMPAIGN_ID,   tgt.ID_CUID
          , tgt.MAX_CREDIT_AMOUNT,  tgt.MAX_ANNUITY          , tgt.VALIDITY_PERIOD,    tgt.DPD_HISTORY
          , tgt.DPD_12_MS,          tgt.DPD_3_MS             , tgt.type,               tgt.PRIORITY
          , tgt.CAMPAIGN_TYPE,      tgt.DATE_VALID_FROM      , tgt.DATE_VALID_TO,      tgt.SID_RESULT
          , tgt.score,              tgt.risk_group           , tgt.rbp_segment,        tgt.lost_elig_reason
          , tgt.date_check,         tgt.FLAG_STILL_ELIGIBLE
    from AP_RISK.ELIGIBILILITY_DAILY_CHECK tgt
    join (select id_cuid, valid_from, valid_to from camp_elig_base where eligible_final_flag = 1) ceb 
    on tgt.id_cuid = ceb.id_cuid and tgt.date_valid_from = ceb.valid_from and tgt.date_valid_to = ceb.valid_to;
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pStats('CAMP_ELIG_DAILY_CHECK');

		pTruncate('camp_orbp_daily_check');
		AP_PUBLIC.CORE_LOG_PKG.pStart('INS:camp_orbp_daily_check');
    insert  /*+ APPEND */ into camp_orbp_daily_check
    select tgt.CAMPAIGN_ID,   tgt.ID_CUID
          , tgt.MAX_CREDIT_AMOUNT,  tgt.MAX_ANNUITY          , tgt.VALIDITY_PERIOD,    tgt.DPD_HISTORY
          , tgt.DPD_12_MS,          tgt.DPD_3_MS             , tgt.type,               tgt.PRIORITY
          , tgt.CAMPAIGN_TYPE,      tgt.DATE_VALID_FROM      , tgt.DATE_VALID_TO,      tgt.SID_RESULT
          , tgt.score,              tgt.risk_group           , tgt.rbp_segment,        tgt.lost_elig_reason
          , tgt.date_check,         tgt.FLAG_STILL_ELIGIBLE
    from AP_RISK.ELIGIBILILITY_DAILY_CHECK tgt
    join (select id_cuid, valid_from, valid_to from camp_orbp_elig_base where eligible_final_flag = 1) ceb 
    on tgt.id_cuid = ceb.id_cuid and tgt.date_valid_from = ceb.valid_from and tgt.date_valid_to = ceb.valid_to;
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pStats('camp_orbp_daily_check');

		AP_PUBLIC.CORE_LOG_PKG.pStart('MRG_UPD:F_RBP_SEGMENT_PRICE');
		/* Update price not existed in current elig base */
		merge into ap_crm.f_rbp_segment_price tgt
		using
		(
				select risk_band, rbp_segment, tenor, interest_rate from ap_crm.f_rbp_segment_price
				where status = 'Y'
				minus
				select distinct risk_band, rbp_segment_temp rbp_segment, max_tenor tenor, interest_rate from camp_elig_base
				where priority_actual > 0
		)src
		on (tgt.risk_band = src.risk_band and tgt.rbp_segment = src.rbp_segment and tgt.tenor = src.tenor and tgt.interest_rate = src.interest_rate)
		when matched then
		update set tgt.status = 'N';
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pStats('F_RBP_SEGMENT_PRICE');

		AP_PUBLIC.CORE_LOG_PKG.pStart('MRG_INS:F_RBP_SEGMENT_PRICE');
		/* Insert price not existed in current elig base */
		merge into ap_crm.f_rbp_segment_price tgt
		using
		(
				select distinct risk_band, rbp_segment_temp rbp_segment, max_tenor tenor, interest_rate from camp_elig_base
				where priority_actual > 0
				minus
				select risk_band, rbp_segment, tenor, interest_rate from ap_crm.f_rbp_segment_price where status = 'Y'
		) src
		on (tgt.risk_band = src.risk_band and tgt.rbp_segment = src.rbp_segment and tgt.tenor = src.tenor and tgt.interest_rate = src.interest_rate)
		when matched then
		update
				set tgt.status = 'Y'
		when not matched then
		insert ( risk_band,  rbp_segment, tenor, interest_rate, status )
		values ( src.risk_band,  src.rbp_segment, src.tenor, src.interest_rate, 'Y');
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pStats('F_RBP_SEGMENT_PRICE');
<<finish_line>>
		AP_PUBLIC.CORE_LOG_PKG.pFinish ;
END;
