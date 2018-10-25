CREATE OR REPLACE PROCEDURE "CAMP_RAW_COMP_CL" as

  PROCEDURE pStats( acTable VARCHAR2, anPerc NUMBER DEFAULT 0.01) IS
  BEGIN
    AP_PUBLIC.CORE_LOG_PKG.pStart( 'STAT - '||upper(acTable) );
    DBMS_STATS.Gather_Table_Stats( OwnName => 'AP_CRM', TabName => upper(acTable),Estimate_Percent => anPerc );
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
  END ;
  PROCEDURE pTruncate( acTable VARCHAR2)
    IS
  BEGIN
    AP_PUBLIC.CORE_LOG_PKG.pStart( 'Trunc:'||upper(acTable) );
    EXECUTE IMMEDIATE 'TRUNCATE TABLE AP_CRM.' || upper(acTable) ;
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
  END ;

begin
    AP_PUBLIC.CORE_LOG_PKG.pInit( 'AP_CRM', 'CAMP_RAW_COMP_CL') ;
    execute immediate 'alter session set nls_date_language = ''AMERICAN''';

    AP_PUBLIC.CORE_LOG_PKG.pStart('Delete from camp_ff_comm_rec');
    delete from camp_ff_comm_rec where date_call < trunc(sysdate-95);
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pstats('camp_ff_comm_rec');
    
    AP_PUBLIC.CORE_LOG_PKG.pStart('Insert to camp_ff_comm_rec');    
    merge into camp_ff_comm_rec tgt
    using
    (
        with comms as
        (
               select /*+ MATERIALIZE FULL(FCR) */ 
               fcr.date_call, fcr.dtime_inserted, fcr.skf_communication_record, fcr.skp_client, fcr.skp_credit_case,
               fcr.skp_communication_channel, fcr.code_channel, fcr.name_communication_channel,
               fcr.skp_communication_type, fcr.code_type_code, fcr.NAME_COMMUNICATION_TYPE,
               fcr.skp_communication_subtype, fcr.code_subtype, fcr.NAME_COMMUNICATION_SUBTYPE,
               fcr.skp_comm_subtype_specif, fcr.CODE_COMM_SUBTYPE_SPECIF, fcr.NAME_COMM_SUBTYPE_SPECIF,
               fcr.skp_comm_subtype_sub_specif, fcr.CODE_COMM_SUBTYPE_SUB_SPECIF, fcr.NAME_COMM_SUBTYPE_SUB_SPECIF,
               fcr.skp_communication_status, fcr.code_status, fcr.name_communication_status,
               fcr.skp_communication_result_type, fcr.code_result_type, fcr.NAME_COMMUNICATION_RESULT_TYPE,
               fcr.text_note, fcr.text_contact, fcr.employee_number, fcr.common_name
               from ap_crm.camp_comm_rec_ob fcr where fcr.date_call >= trunc(sysdate-5)
                and fcr.skp_client in (select skp_client from camp_elig_base) 
               union all
               select
               fcr.date_call, fcr.dtime_inserted, fcr.skf_communication_record, fcr.skp_client, fcr.skp_credit_case,
               fcr.skp_communication_channel, fcr.code_channel, fcr.name_communication_channel,
               fcr.skp_communication_type, fcr.code_type_code, fcr.NAME_COMMUNICATION_TYPE,
               fcr.skp_communication_subtype, fcr.code_subtype, fcr.NAME_COMMUNICATION_SUBTYPE,
               fcr.skp_comm_subtype_specif, fcr.CODE_COMM_SUBTYPE_SPECIF, fcr.NAME_COMM_SUBTYPE_SPECIF,
               fcr.skp_comm_subtype_sub_specif, fcr.CODE_COMM_SUBTYPE_SUB_SPECIF, fcr.NAME_COMM_SUBTYPE_SUB_SPECIF,
               fcr.skp_communication_status, fcr.code_status, fcr.name_communication_status,
               fcr.skp_communication_result_type, fcr.code_result_type, fcr.NAME_COMMUNICATION_RESULT_TYPE,
               fcr.text_note, fcr.text_contact, fcr.employee_number, fcr.common_name
               from ap_crm.camp_comm_rec_ib fcr where fcr.date_call >= trunc(sysdate-5)
                and fcr.skp_client in (select skp_client from camp_elig_base) 
        )
        select /*+ FULL(FCR) USE_HASH (fcr ccl) */ distinct
               fcr.date_call, fcr.dtime_inserted, fcr.skf_communication_record, fcr.skp_client, fcr.skp_credit_case,
               fcr.skp_communication_channel, fcr.code_channel, fcr.name_communication_channel,
               fcr.skp_communication_type, fcr.code_type_code, fcr.NAME_COMMUNICATION_TYPE,
               fcr.skp_communication_subtype, fcr.code_subtype, fcr.NAME_COMMUNICATION_SUBTYPE,
               fcr.skp_comm_subtype_specif, fcr.CODE_COMM_SUBTYPE_SPECIF, fcr.NAME_COMM_SUBTYPE_SPECIF,
               fcr.skp_comm_subtype_sub_specif, fcr.CODE_COMM_SUBTYPE_SUB_SPECIF, fcr.NAME_COMM_SUBTYPE_SUB_SPECIF,
               fcr.skp_communication_status, fcr.code_status, fcr.name_communication_status,
               fcr.skp_communication_result_type, fcr.code_result_type, fcr.NAME_COMMUNICATION_RESULT_TYPE,
               fcr.text_note, fcr.text_contact, fcr.employee_number, fcr.common_name
        FROM ap_crm.comms fcr
        inner join camp_cfg_comm_list ccl on ccl.name_campaign in ('FF Regular','FF General') /* and nvl(ccl.logic_group,'-') not in ('Complaint','Exclusion') */ and ccl.active = 'Y'
           and nvl(fcr.skp_communication_channel,-1) = case when ccl.skp_communication_channel is null then nvl(fcr.skp_communication_channel,-1) else ccl.skp_communication_channel end
           and nvl(fcr.skp_communication_type,-1) = case when ccl.skp_communication_type is null then nvl(fcr.skp_communication_type,-1) else ccl.skp_communication_type end
           and nvl(fcr.skp_communication_subtype,-1) = case when ccl.skp_communication_subtype is null then nvl(fcr.skp_communication_subtype,-1) else ccl.skp_communication_subtype end
           and nvl(fcr.skp_comm_subtype_specif,-1) = case when ccl.skp_comm_subtype_specif is null then nvl(fcr.skp_comm_subtype_specif,-1) else  ccl.skp_comm_subtype_specif end
           and nvl(fcr.skp_comm_subtype_sub_specif,-1) = case when ccl.skp_comm_subtype_sub_specif is null then nvl(fcr.skp_comm_subtype_sub_specif,-1) else ccl.skp_comm_subtype_sub_specif end
           and nvl(fcr.skp_communication_status,-1) = case when ccl.skp_communication_status is null then nvl(fcr.skp_communication_status,-1) else ccl.skp_communication_status end
           and nvl(fcr.skp_communication_result_type,-1) = case when ccl.skp_communication_result_type is null then nvl(fcr.skp_communication_result_type,-1) else ccl.skp_communication_result_type end
    )src on (tgt.skp_client = src.skp_client and tgt.skp_credit_case = src.skp_credit_case and src.skf_communication_record = tgt.skf_communication_record )
    when not matched then insert 
    (
         TGT.date_call, TGT.dtime_inserted, TGT.skf_communication_record, TGT.skp_client, TGT.skp_credit_case,
         TGT.skp_communication_channel, TGT.code_channel, TGT.name_communication_channel,
         TGT.skp_communication_type, TGT.code_type_code, TGT.NAME_COMMUNICATION_TYPE,
         TGT.skp_communication_subtype, TGT.code_subtype, TGT.NAME_COMMUNICATION_SUBTYPE,
         TGT.skp_comm_subtype_specif, TGT.CODE_COMM_SUBTYPE_SPECIF, TGT.NAME_COMM_SUBTYPE_SPECIF,
         TGT.skp_comm_subtype_sub_specif, TGT.CODE_COMM_SUBTYPE_SUB_SPECIF, TGT.NAME_COMM_SUBTYPE_SUB_SPECIF,
         TGT.skp_communication_status, TGT.code_status, TGT.name_communication_status,
         TGT.skp_communication_result_type, TGT.code_result_type, TGT.NAME_COMMUNICATION_RESULT_TYPE,
         TGT.text_note, TGT.text_contact, TGT.employee_number, TGT.common_name
    )
    values 
    (
         SRC.date_call, SRC.dtime_inserted, SRC.skf_communication_record, SRC.skp_client, SRC.skp_credit_case,
         SRC.skp_communication_channel, SRC.code_channel, SRC.name_communication_channel,
         SRC.skp_communication_type, SRC.code_type_code, SRC.NAME_COMMUNICATION_TYPE,
         SRC.skp_communication_subtype, SRC.code_subtype, SRC.NAME_COMMUNICATION_SUBTYPE,
         SRC.skp_comm_subtype_specif, SRC.CODE_COMM_SUBTYPE_SPECIF, SRC.NAME_COMM_SUBTYPE_SPECIF,
         SRC.skp_comm_subtype_sub_specif, SRC.CODE_COMM_SUBTYPE_SUB_SPECIF, SRC.NAME_COMM_SUBTYPE_SUB_SPECIF,
         SRC.skp_communication_status, SRC.code_status, SRC.name_communication_status,
         SRC.skp_communication_result_type, SRC.code_result_type, SRC.NAME_COMMUNICATION_RESULT_TYPE,
         SRC.text_note, SRC.text_contact, SRC.employee_number, SRC.common_name
    );
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pstats('camp_ff_comm_rec');

    ptruncate('gtt_cmp_rtn_comm_parts');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS - GTT_cmp_rtn_comm_parts');
    insert /*+ APPEND */ into gtt_cmp_rtn_comm_parts
    select skf_communication_record,
          coalesce
          (
             max(decode(code_comm_result_part, 'CALL_ON', text_value)), 
             max(decode(code_comm_result_part, 'PRMS_DT_MPF', text_value)), 
             max(decode(code_comm_result_part, 'DATETIME', text_value)), 
             max(decode(code_comm_result_part, 'PUSH_TOSIGN', text_value))
          )DATE_PROMISE,
          max(decode(code_comm_result_part, 'PHONE', text_value)) CB_PHONE
    from camp_comm_res_part
    where skf_communication_record in (select skf_communication_record from ap_crm.camp_ff_comm_rec)
    group by skf_communication_record;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pstats('gtt_cmp_rtn_comm_parts');
    
    pTruncate('gtt_cmp_rtn_03_comp_comms');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS - GTT_cmp_rtn_03_comp_comms');
    insert /*+ APPEND */ into gtt_cmp_rtn_03_comp_comms
    select /*+ USE_HASH(COMM CRP CCL) */  comm.skp_client, comm.skf_communication_record, comm.date_call,
           coalesce(comm.name_communication_channel, ccl.name_channel)name_communication_channel, 
           coalesce(comm.name_communication_type, ccl.name_communication_type)name_communication_type,
           coalesce(comm.name_communication_subtype,ccl.name_communication_subtype)name_communication_subtype,
           coalesce(comm.name_comm_subtype_specif, ccl.name_comm_subtype_specif)name_comm_subtype_specif, 
           coalesce(comm.name_comm_subtype_sub_specif, ccl.name_comm_subtye_sub_specif)name_comm_subtye_sub_specif,
           coalesce(comm.name_communication_status, ccl.name_communication_status)name_communication_status, 
           coalesce(comm.name_communication_result_type, ccl.name_communication_result_type)name_communication_result_type,
           ccl.name_sub_campaign, ccl.action, ccl.delay_days, 
           (trunc(sysdate)-trunc(comm.date_call)) detention_days
           , to_date(substr(crp.date_promise, 0, 10), 'dd/mm/yyyy')date_promise, crp.cb_phone
           , ccl.logic_group
    from camp_ff_comm_rec comm
    join camp_cfg_comm_list ccl on ccl.name_campaign in ('FF Regular','FF General') and ccl.active = 'Y'
       and nvl(comm.skp_communication_channel,-1) = case when ccl.skp_communication_channel is null then nvl(comm.skp_communication_channel,-1) else ccl.skp_communication_channel end
       and nvl(comm.skp_communication_type,-1) = case when ccl.skp_communication_type is null then nvl(comm.skp_communication_type,-1) else ccl.skp_communication_type end
       and nvl(comm.skp_communication_subtype,-1) = case when ccl.skp_communication_subtype is null then nvl(comm.skp_communication_subtype,-1) else ccl.skp_communication_subtype end
       and nvl(comm.skp_comm_subtype_specif,-1) = case when ccl.skp_comm_subtype_specif is null then nvl(comm.skp_comm_subtype_specif,-1) else  ccl.skp_comm_subtype_specif end
       and nvl(comm.skp_comm_subtype_sub_specif,-1) = case when ccl.skp_comm_subtype_sub_specif is null then nvl(comm.skp_comm_subtype_sub_specif,-1) else ccl.skp_comm_subtype_sub_specif end
       and nvl(comm.skp_communication_status,-1) = case when ccl.skp_communication_status is null then nvl(comm.skp_communication_status,-1) else ccl.skp_communication_status end
       and nvl(comm.skp_communication_result_type,-1) = case when ccl.skp_communication_result_type is null then nvl(comm.skp_communication_result_type,-1) else ccl.skp_communication_result_type end
    left join gtt_cmp_rtn_comm_parts crp on comm.skf_communication_record = crp.skf_communication_record;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('gtt_cmp_rtn_03_comp_comms');
    
    ptruncate('GTT_CAMP_IB_INTRST');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS - GTT_CAMP_IB_INTRST');
		insert /*+ APPEND */ into AP_CRM.GTT_CAMP_IB_INTRST (skp_client, date_call, name_communication_subtype, cnt_delay_days)
    select skp_client, date_call, name_communication_subtype, DELAY_DAYS 
    from gtt_cmp_rtn_03_comp_comms where LOGIC_GROUP = 'Inb. Interest' and  (trunc(sysdate) - trunc(date_call)) < 20;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('GTT_CAMP_IB_INTRST');

    ptruncate('gtt_camp_ib_info');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS - GTT_CAMP_IB_INfo');
    insert /*+ APPEND */ into AP_CRM.gtt_camp_ib_info (skp_client, date_call, name_communication_subtype, cnt_delay_days)
    select skp_client, date_call, name_communication_subtype, DELAY_DAYS 
    from gtt_cmp_rtn_03_comp_comms where LOGIC_GROUP = 'Inb. Info' and  (trunc(sysdate) - trunc(date_call)) < 20;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('gtt_camp_ib_info');
    
    ptruncate('GTT_CAMP_MPF_CANCEL');
		AP_PUBLIC.CORE_LOG_PKG.pStart('INS - GTT_CAMP_MPF_CANCEL');
    insert /*+ APPEND */ into GTT_CAMP_MPF_CANCEL
    select * from v_camp_mpf_cancel;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('GTT_CAMP_MPF_CANCEL');
    
    ptruncate('GTT_CAMP_IB_COMPLAINT');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS - GTT_CAMP_IB_complaint');
    insert /*+ APPEND */ into GTT_CAMP_IB_COMPLAINT (skp_client, date_call, name_communication_subtype, cnt_delay_days, detention_days)
    select skp_client, date_call, name_communication_subtype, DELAY_DAYS, DETENTION_DAYS 
    from gtt_cmp_rtn_03_comp_comms where LOGIC_GROUP = 'Complaint' and  detention_days < delay_days;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('GTT_CAMP_IB_COMPLAINT');

    ptruncate('GTT_CAMP_NOTINTEREST_OFFR');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS - GTT_CAMP_notinterest_offr');
    insert /*+ APPEND */ into GTT_CAMP_NOTINTEREST_OFFR (skp_client, date_call, NAME_COMMUNICATION_RESULT_TYPE, cnt_delay_days, detention_days)
    select comm.skp_client, date_call, NAME_COMMUNICATION_RESULT_TYPE, 
           case when ptb.decile = 8 and eli.priority_actual >= 10 and comm.delay_days < 21 then 21
                when ptb.decile = 9 and comm.delay_days < 21 then 21
                when ptb.decile = 10 and comm.delay_days < 30 then 30
                else comm.delay_days
           end DELAY_DAYS,
           DETENTION_DAYS 
    from gtt_cmp_rtn_03_comp_comms comm
    left join ptb_population ptb on comm.skp_client = ptb.skp_client and ptb.campaign_id = to_char(sysdate,'yymm')
    left join camp_elig_base eli on comm.skp_client = eli.skp_client
    where LOGIC_GROUP = 'Not Interested'
      and detention_days < 
                case when ptb.decile = 8 and eli.priority_actual >= 10 and comm.delay_days < 21 then 21
                when ptb.decile = 9 and comm.delay_days < 21 then 21
                when ptb.decile = 10 and comm.delay_days < 30 then 30
                else comm.delay_days end;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('GTT_CAMP_NOTINTEREST_OFFR');

    ptruncate('GTT_CAMP_DWTO_OFFR');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS - GTT_camp_dwto_offr');
    insert /*+ APPEND */ into GTT_CAMP_DWTO_OFFR (skp_client, date_call, name_communication_result_type, cnt_delay_days, detention_days)
    select skp_client, trunc(sysdate) date_call, name_communication_result_type, DELAY_DAYS, DETENTION_DAYS 
    from gtt_cmp_rtn_03_comp_comms where LOGIC_GROUP = 'Exclusion'; --and  detention_days < delay_days;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('GTT_CAMP_DWTO_OFFR');

    ptruncate('GTT_CAMP_CALLBACK');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS - GTT_CAMP_CALLBACK');  
    insert /*+ APPEND */ into GTT_CAMP_CALLBACK
    select skp_Client, max(Dt_Callback)dt_callback from gtt_cmp_rtn_03_comp_comms 
    where name_sub_campaign = '3.Call Back' and dt_callback >= trunc(sysdate)
    group by skp_client;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('GTT_CAMP_CALLBACK');

    ptruncate('GTT_CAMP_ATMPT_L30D');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS - GTT_CAMP_ATMPT_L30D');
/*		INSERT \*+ APPEND *\ INTO GTT_CAMP_ATMPT_L30D
		SELECT * FROM CAMP_V_ATTMPT_LAST_30D;*/
    if extract(day from sysdate) >= 1 then
       INSERT /*+ APPEND */ INTO GTT_CAMP_ATMPT_L30D
       SELECT * FROM CAMP_V_ATTMPT_LAST_30D_RING;
    else
       INSERT /*+ APPEND */ INTO GTT_CAMP_ATMPT_L30D
       SELECT * FROM CAMP_V_ATTMPT_LAST_30D;
    end if;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    COMMIT;
    pStats('GTT_CAMP_ATMPT_L30D');

    ptruncate('GTT_CAMP_ATMPT_OCS_L30D');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS - GTT_CAMP_ATMPT_OCS_L30D');
/*		insert \*+ APPEND *\ into GTT_CAMP_ATMPT_OCS_L30D
		select * from camp_v_attmpt_ocs_last_30d;*/
    if extract(day from sysdate) >= 1 then
        insert /*+ APPEND */ into GTT_CAMP_ATMPT_OCS_L30D
        select * from camp_v_attmpt_ocs_30d_ring;
    else
        insert /*+ APPEND */ into GTT_CAMP_ATMPT_OCS_L30D
        select * from camp_v_attmpt_ocs_last_30d;
    end if;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    COMMIT;
    pStats('GTT_CAMP_ATMPT_OCS_L30D');

    ptruncate('GTT_CAMP_ATMPT_CM');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS - GTT_CAMP_ATMPT_CM');
/*		INSERT \*+ APPEND *\ INTO GTT_CAMP_ATMPT_CM
    SELECT * FROM CAMP_V_ATTMPT_cm;*/
    if extract(day from sysdate) >= 1 then
        INSERT /*+ APPEND */ INTO GTT_CAMP_ATMPT_CM
        SELECT * FROM CAMP_V_ATTMPT_cm_ring;
    else
        INSERT /*+ APPEND */ INTO GTT_CAMP_ATMPT_CM
        SELECT * FROM CAMP_V_ATTMPT_cm;
    end if;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    COMMIT;
    pStats('GTT_CAMP_ATMPT_CM');

    ptruncate('GTT_CAMP_ATMPT_OCS_CM');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS - GTT_CAMP_ATMPT_OCS_CM');
/*	  INSERT \*+ APPEND *\ INTO GTT_CAMP_ATMPT_OCS_CM
	  SELECT * FROM CAMP_V_ATTMPT_OCS_CM;*/
    if extract(day from sysdate) >= 1 then
       INSERT /*+ APPEND */ INTO GTT_CAMP_ATMPT_OCS_CM
       SELECT * FROM CAMP_V_ATTMPT_OCS_CM_RING;
    else
       INSERT /*+ APPEND */ INTO GTT_CAMP_ATMPT_OCS_CM
       SELECT * FROM CAMP_V_ATTMPT_OCS_CM;
    end if;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    COMMIT;
    pStats('GTT_CAMP_ATMPT_CM');

    ptruncate('GTT_CAMP_ATTMPT_L3D');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS - GTT_CAMP_ATTMPT_L3D');
    INSERT /*+ APPEND */ INTO GTT_CAMP_ATTMPT_L3D
    SELECT * FROM CAMP_V_ATTMPT_LAST_3D;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    COMMIT;
    pStats('GTT_CAMP_ATTMPT_L3D');

    ptruncate('GTT_CAMP_MOBILE_APP');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS - GTT_CAMP_MOBILE_APP');
    insert /*+ APPEND */ into GTT_CAMP_MOBILE_APP
    select * from AP_CRM.V_CAMP_MOBILE_APP;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('GTT_CAMP_MOBILE_APP');

    ptruncate('GTT_CAMP_landing_page');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS - GTT_CAMP_LANDING_PAGE');
    insert /*+ APPEND */ into GTT_CAMP_landing_page
    select * from AP_CRM.v_camp_landing_page;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('GTT_CAMP_LANDING_PAGE');

    ptruncate('GTT_CAMP_abandon');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS - GTT_CAMP_ABANDON');
    insert /*+ APPEND */ into GTT_CAMP_abandon
    select * from AP_CRM.v_camp_abandoned_drop;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('GTT_CAMP_ABANDON');

    ptruncate('GTT_CAMP_MPF_REJECT');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS - GTT_CAMP_MPF_REJECT');
    insert /*+ APPEND */ into GTT_CAMP_MPF_REJECT
    select * from AP_CRM.V_CAMP_MPF_REJECT;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('GTT_CAMP_MPF_REJECT');

    pTruncate('CAMP_COMPILED_LIST');
    AP_PUBLIC.CORE_LOG_PKG.pStart('INS - CAMP_COMPILED_LIST');
    insert /*+ APPEND */ into ap_crm.camp_compiled_list
    with Interest_Inb as
    (
       select /*+ MATERIALIZE */ SKP_CLIENT, max(DATE_CALL) DATE_CALL
       from AP_CRM.gtt_camp_ib_intrst
       group by SKP_CLIENT
    ),
    INFO_INB as
    (
        select /*+ MATERIALIZE */ SKP_CLIENT, max(DATE_CALL) DATE_CALL
        from AP_CRM.gtt_camp_ib_info
        group by SKP_CLIENT
    ),
    Cancelled as
    (
        select /*+ MATERIALIZE */ SKP_CLIENT, MAX(cancel_date) cancel_date
        from AP_CRM.GTT_CAMP_MPF_CANCEL
        group by SKP_CLIENT
    ),
    complaint as
    (
        select /*+ MATERIALIZE */ compl.SKP_CLIENT, max(Compl.DATE_CALL) Date_Complaint
        from AP_CRM.GTT_CAMP_IB_COMPLAINT Compl
        where Compl.DETENTION_DAYS < Compl.CNT_DELAY_DAYS
        group by compl.SKP_CLIENT
    ),
    out_fr_dwto as
    (
        select /*+ MATERIALIZE */ DWTO.SKP_CLIENT, max(DWTO.DATE_CALL) Date_DWTO
        from AP_CRM.GTT_CAMP_DWTO_OFFR DWTO
        --where DWTO.DETENTION_DAYS < DWTO.CNT_DELAY_DAYS
        group by DWTO.SKP_CLIENT
    ),
    out_fr_dwto2 as
    (
        select /*+ MATERIALIZE */ DWTO2.SKP_CLIENT, max(DWTO2.DATE_CALL) Date_DWTO2
        from AP_CRM.gtt_CAMP_DWTO_COMM_STATUS DWTO2
        where DWTO2.DETENTION_DAYS < DWTO2.CNT_DELAY_DAYS
        group by DWTO2.SKP_CLIENT
    ),
    not_int as
    (
        select /*+ MATERIALIZE */ NOT_INT.SKP_CLIENT, max(NOT_INT.DATE_CALL) Date_NOT_INT
        from AP_CRM.GTT_CAMP_NOTINTEREST_OFFR NOT_INT
        where NOT_INT.DETENTION_DAYS < NOT_INT.CNT_DELAY_DAYS
        group by NOT_INT.SKP_CLIENT
    ),
    call_back as
    (
        select /*+ MATERIALIZE */ SKP_CLIENT, CALL_BACK_DT
        from AP_CRM.GTT_CAMP_CALLBACK
        where trunc(CALL_BACK_DT)>= trunc(sysdate)
    ),
    ATTEMPT_30D AS
    (
        select /*+ MATERIALIZE */ cuid, max(ATTEMPT_LAST30D)ATTEMPT_Last30D from
        (
            select CUID, sum(ATTEMPT) ATTEMPT_Last30D
            from ap_crm.GTT_CAMP_ATMPT_L30D
            group by CUID
            union all
            select CUID, sum(ATTEMPT) ATTEMPT_Last30D
            from ap_crm.GTT_CAMP_ATMPT_OCS_L30D
            group by CUID
        )group by cuid
    ),
    ATTEMPT_CM AS
    (
        select /*+ MATERIALIZE */ cuid, max(ATTEMPT_CURRENT)ATTEMPT_CURRENT from
        (
            select CUID, sum(ATTEMPT) ATTEMPT_CURRENT
            from ap_crm.GTT_CAMP_ATMPT_CM
            group by CUID
            union all
            select CUID, sum(ATTEMPT) ATTEMPT_CURRENT
            from ap_crm.GTT_CAMP_ATMPT_OCS_CM
            group by CUID
        )group by cuid
    ),
    ATTEMPT_3D AS
    (
      select distinct CUID --, sum(ATTEMPT) ATTEMPT_Last3D
      from ap_crm.GTT_CAMP_ATTMPT_L3D
    ),
    mob as
    (
      select /*+ MATERIALIZE */ CLIENT_ID,CUID,CREATED_DATE MOB_APPLY,Email,PHONE
      from AP_CRM.GTT_CAMP_MOBILE_APP
    ),
    land as
    (
        select /*+ MATERIALIZE */ DATE_CREATED LAND_APPLY,SKP_CLIENT,EMAIL,Mobile1
        from AP_CRM.GTT_CAMP_landing_page
        where (SKP_CLIENT, date_created)
        in
        (
           select skp_client, max(date_created) from AP_CRM.GTT_CAMP_landing_page where skp_client is not null
           group by skp_client
        )
    ),
    dropped as
    (
        select /*+ MATERIALIZE */ distinct A.*,B.PHONE_NUMBER from
         (
            select ID_CUID,SKP_CLIENT,MAX(TIME_CALL) TIME_CALL
            from AP_CRM.GTT_CAMP_abandon
            group by ID_CUID,SKP_CLIENT
         ) A
        left join
        (
            select SKP_CLIENT,TIME_CALL,PHONE_NUMBER
            from AP_CRM.GTT_CAMP_abandon
        ) B on A.SKP_CLIENT=B.SKP_CLIENT and  A.TIME_CALL=B.TIME_CALL
        where A.SKP_CLIENT is not null
    ),
    rejected as
    (
        select /*+ MATERIALIZE */ SKP_CLIENT,MAX(trunc(Reject_date)) Dt_Reject
        from AP_CRM.GTT_CAMP_MPF_REJECT
        group by SKP_CLIENT
    )
    select distinct
           PRE.period, pre.valid_from, pre.id_cuid, pre.skp_client, pre.contract,
           pre.name_salesroom, pre.name_full, pre.name_first, pre.name_last, pre.name_birth_place,
           pre.date_birth, pre.code_employment_type, pre.code_employer_industry, pre.main_income,
           pre.code_education_type, pre.max_tenor, pre.max_credit_amount, pre.max_instalment, pre.rbp_segment,
           pre.risk_group, pre.risk_score, pre.type, pre.priority, pre.tdy_priority, pre.first_eligibility, pre.tdy_eligibility,
           pre.name_mother, pre.id_ktp, pre.expiry_date_ktp, pre.primarym_1, pre.primarym_2, pre.primarym_3, pre.primarym_4, pre.primarym_5, pre.client_email,
           pre.full_address, pre.name_town, pre.name_subdistrict, pre.code_zip_code, pre.name_district, pre.dead_customer
           /*,Case when LOWER(PRE.PILOT_NAME) = '2nd mpf' then
                      case when act_mpf.SKP_CLIENT is not null and act_mpf.create_date >= trunc(add_months(sysdate,-12)) then 'Y' else 'N' end
                 when LOWER(PRE.PILOT_NAME) <> '2nd mpf' then
                      case when Act_MPF.SKP_CLIENT is null then 'N' else 'Y' end
                 else 'Y'*/
            ,Case when LOWER(nvl(PRE.PILOT_NAME,'-')) = '2nd mpf' then
                       case when act_mpf.SKP_CLIENT is not null and act_mpf.create_date >= trunc(add_months(sysdate,-12)) then 'Y' else 'N' end
                 when lower(nvl(PRE.PILOT_NAME,'-')) = 'premium offer' then 'N'
								 when LOWER(nvl(PRE.PILOT_NAME,'-')) not in ('2nd mpf', 'premium offer') and Act_MPF.SKP_CLIENT is null then 'N'
            else 'Y' 
            end as Has_Active_FF
           ,Interest_Inb.Date_call Dt_Intrst_Inb
           ,INFO_INB.Date_call Dt_INFO_inb
           ,cancel_date Dt_Cancel
           ,Rejected.Dt_Reject
           ,CALL_BACK_DT Dt_Call_Back
           ,MOB.MOB_APPLY Dt_MobApp,MOB.Email Mob_EMAIL,MOB.PHONE MOB_Phone
           ,Land.LAND_APPLY Dt_Land,Land.Email Land_EMAIL,Land.Mobile1 Land_Phone
           ,Dropped.Time_call Dt_Drop,Dropped.PHONE_NUMBER Drop_Phone
           ,Date_Complaint Dt_complaint
           ,Date_DWTO Dt_DWTO
           ,Date_DWTO2 Dt_DWTO2
           ,NOT_INT.Date_NOT_INT Dt_Nintrst
           ,case when Attempt_3D.CUID is null then 'Y' else 'N' end as Avail_to_Call3D
           ,ATTEMPT_Last30D
           ,ATTEMPT_CURRENT
           ,lower(pre.pilot_name),
					 ceil((((ceb.interest_rate/100) * pre.max_credit_amount * pre.max_tenor) + pre.max_credit_amount)/pre.max_tenor) + 5000 min_instalment
    from AP_CRM.CAMP_OFFER_CALL_PRE PRE /*Flexifast customer basic info: eligibility tdy, customer identity,Communication address */
		left join ap_crm.camp_elig_base ceb on pre.skp_client = ceb.skp_client
    /* Find Active FlexiFast Contract */
    left join AP_CRM.V_CAMP_MPF_ACTIVE Act_MPF on PRE.SKP_CLIENT=Act_MPF.SKP_CLIENT
    /* Find interested communication from inbound Ops */
    left join Interest_Inb On PRE.SKP_CLIENT=Interest_Inb.SKP_CLIENT
    /* Find Flexifast info communication from inbound Ops */
    left join INFO_INB On PRE.SKP_CLIENT=INFO_INB.SKP_CLIENT
    /* Find Cancelled Flexifast  Customer*/
    left join Cancelled On PRE.SKP_CLIENT=Cancelled.SKP_CLIENT
    /*Find Any COmplaint from Customer */
    left join Complaint On PRE.SKP_CLIENT=Complaint.SKP_CLIENT
    /* Find Don't want to be offered from call result */
    left join OUT_FR_DWTO On PRE.SKP_CLIENT = OUT_FR_DWTO.SKP_CLIENT
    /* Find Don't want to be offered from Communication Status */
    left join OUT_FR_DWTO2 On PRE.SKP_CLIENT = OUT_FR_DWTO2.SKP_CLIENT
    /* Find Not Interested Result from call offer */
    left join NOT_INT On PRE.SKP_CLIENT = NOT_INT.SKP_CLIENT
    /* Find Call Back Request from call result */
    left join CALL_BACK On PRE.SKP_CLIENT = CALL_BACK.SKP_CLIENT
    /* Count Call Attempt from last 30 days */
    left join Attempt_30D on PRE.id_cuid = Attempt_30D.CUID
    /* Count Call Attempt from current month */
    LEFT JOIN attempt_cm on pre.id_cuid = attempt_cm.cuid
    /* Count Call Attempt from last 3 days */
    left join Attempt_3D on PRE.id_cuid = Attempt_3D.CUID
    /* find From Mobile App Form */
    left join MOB on  PRE.id_cuid = MOB.CUID
    /* find from Landing Page Form*/
    left join Land on  PRE.SKP_CLIENT = LAND.SKP_CLIENT
    /* find from Abandoned and drop call from IVR*/
    left join Dropped on  PRE.SKP_CLIENT = Dropped.SKP_CLIENT
    /* Find Reject FlexiFast Customer*/
    left join Rejected on  PRE.SKP_CLIENT = Rejected.SKP_CLIENT;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pStats('CAMP_COMPILED_LIST');

    AP_PUBLIC.CORE_LOG_PKG.pFinish ;
end;
/

