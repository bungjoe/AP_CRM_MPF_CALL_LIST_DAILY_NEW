CREATE OR REPLACE PROCEDURE CAMP_RAW_COMM_RECS AS
      SKP_COMM_CHANNEL number;
      SKP_COMM_TYPE varchar2(150);
      SKP_COMM_SUBTYPE varchar2(150);
      SKP_COMM_SUBTYP_SPEC varchar2(150);
      SKP_COMM_SUBTYP_SUB_SPEC varchar2(150);
      SKP_COMM_STATUS varchar2(150);
      SKP_COMM_RESULT_TYPE varchar2(150);
      sql_string varchar2(32000);

      cursor cfg_commlist is
        SELECT skp_communication_channel, SKP_communication_type, SKP_communication_subtype, SKP_comm_subtype_specif,
                 SKP_comm_subtype_sub_specif, SKP_communication_status, SKP_communication_result_type
          from CAMP_CFG_COMM_LIST csm where name_campaign in ('FF Regular','FF General') and active = 'Y';
					
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
    AP_PUBLIC.CORE_LOG_PKG.pInit( 'AP_CRM', 'CAMP_RAW_COMM_RECS');
if trunc(sysdate) <= to_date('08/25/2018','mm/dd/yyyy') then
  goto finish_line;
end if;    
		/* Communication Type */
		AP_PUBLIC.CORE_LOG_PKG.pStart('Merge New Communication Type');
		merge into camp_cfg_comm_list tgt
		using
		(
				with como as
				(
						select CODE_COMMUNICATION_TYPE from camp_cfg_comm_list where CODE_COMMUNICATION_TYPE is not null and skp_communication_type is null
				)
				select skp_communication_type, CODE_COMMUNICATION_TYPE from owner_dwh.cl_communication_type where code_communication_type in 
				(
						select code_communication_type from como
				)
		)src on (src.code_communication_type = tgt.code_communication_type)
		when matched then update set tgt.skp_communication_type = src.skp_communication_type;
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;

		/* Communication Sub Type */
		AP_PUBLIC.CORE_LOG_PKG.pStart('Merge New Communication Sub Type');
		merge into camp_cfg_comm_list tgt
		using
		(
				with como as
				(
						select CODE_COMMUNICATION_SUBTYPE from camp_cfg_comm_list where CODE_COMMUNICATION_SUBTYPE is not null and skp_communication_subtype is null
				)
				select skp_communication_subtype, CODE_COMMUNICATION_subTYPE from owner_dwh.cl_communication_subtype where code_communication_subtype in 
				(
						select code_communication_subtype from como
				)
		)src on (src.code_communication_subtype = tgt.code_communication_subtype)
		when matched then update set tgt.skp_communication_subtype = src.skp_communication_subtype, tgt.active = 'Y';
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;

		/* Communication Subtype Specification */
		AP_PUBLIC.CORE_LOG_PKG.pStart('Merge New Communication Subtype Specification');
		merge into camp_cfg_comm_list tgt
		using
		(
				with como as
				(
						select te.CODE_COMM_SUBTYPE_SPECIF from camp_cfg_comm_list te where te.code_comm_subtype_specif is not null and te.skp_comm_subtype_specif is null
				)
				select skp_comm_subtype_specif, CODE_COMM_SUBTYPE_SPECIF from owner_dwh.cl_comm_subtype_specif where CODE_COMM_SUBTYPE_SPECIF in 
				(
						select CODE_COMM_SUBTYPE_SPECIF from como
				)
		)src on (src.CODE_COMM_SUBTYPE_SPECIF = tgt.CODE_COMM_SUBTYPE_SPECIF)
		when matched then update set tgt.SKP_COMM_SUBTYPE_SPECIF = src.SKP_COMM_SUBTYPE_SPECIF, tgt.active = 'Y';
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;

		/* Communication Subtype Sub Specification */
		AP_PUBLIC.CORE_LOG_PKG.pStart('Merge New Communication Subtype Sub Specification');
		merge into camp_cfg_comm_list tgt
		using
		(
				with como as
				(
						select te.code_comm_subtype_sub_specif from camp_cfg_comm_list te where te.code_comm_subtype_sub_specif is not null and te.skp_comm_subtype_sub_specif is null
				)
				select skp_comm_subtype_sub_specif, code_comm_subtype_sub_specif from owner_dwh.cl_comm_subtype_sub_specif where code_comm_subtype_sub_specif in 
				(
						select code_comm_subtype_sub_specif from como
				)
		)src on (src.code_comm_subtype_sub_specif = tgt.code_comm_subtype_sub_specif)
		when matched then update set tgt.skp_comm_subtype_sub_specif = src.skp_comm_subtype_sub_specif, tgt.active = 'Y';
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;

/*		\* Communication Status *\
		AP_PUBLIC.CORE_LOG_PKG.pStart('Merge New Communication Status');
		merge into camp_cfg_comm_list tgt
		using
		(
				with como as
				(
						select distinct te.code_communication_status from camp_cfg_comm_list te where te.code_communication_status is not null and te.skp_communication_status is null
				)
				select skp_communication_status, code_communication_status from owner_dwh.cl_communication_status where code_communication_status in 
				(
						select code_communication_status from como
				)
		)src on (src.code_communication_status = tgt.code_communication_status)
		when matched then update set tgt.skp_communication_status = src.skp_communication_status;
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;*/

		/* Communication Result Type */
		AP_PUBLIC.CORE_LOG_PKG.pStart('Merge New Communication Result Type');
		merge into camp_cfg_comm_list tgt
		using
		(
				with como as
				(
						select te.code_communication_result_type from camp_cfg_comm_list te where te.code_communication_result_type is not null and te.skp_communication_result_type is null
				)
				select skp_communication_result_type, code_communication_result_type from owner_dwh.cl_communication_result_type where code_communication_result_type in 
				(
						select code_communication_result_type from como
				)
		)src on (src.code_communication_result_type = tgt.code_communication_result_type)
		when matched then update set tgt.skp_communication_result_type = src.skp_communication_result_type, tgt.active = 'Y';
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;

		sql_string :=
'begin insert /*+ APPEND */ into ap_crm.gtt_cmp_rtn_02_commlist
 with W$PREPARE AS
 ( SELECT --+ MATERIALIZE
           FCR.DATE_CALL, FCR.DTIME_INSERTED, FCR.SKF_COMMUNICATION_RECORD
         , FCR.SKP_CLIENT, FCR.SKP_CREDIT_CASE, FCR.SKP_CREDIT_TYPE, FCR.DATE_DECISION
         , FCR.SKP_COMMUNICATION_CHANNEL, FCR.SKP_COMMUNICATION_TYPE
         , FCR.SKP_COMMUNICATION_SUBTYPE, FCR.SKP_COMM_SUBTYPE_SPECIF
         , FCR.SKP_COMM_SUBTYPE_SUB_SPECIF, FCR.SKP_COMMUNICATION_STATUS
        , FCR.SKP_COMMUNICATION_RESULT_TYPE
        , FCR.TEXT_NOTE, FCR.TEXT_CONTACT, FCR.EMPLOYEE_NUMBER, FCR.COMMON_NAME
        , FCR.CODE_RESULT_TYPE, FCR.CODE_STATUS, FCR.CODE_SUBTYPE
        , FCR.CODE_TYPE_CODE, FCR.CODE_CHANNEL
    FROM OWNER_DWH.F_COMMUNICATION_RECORD_TT FCR
    WHERE fcr.dtime_inserted >= trunc(sysdate-7)
      and FCR.SKP_CLIENT IN
          ( select skp_client from ap_crm.camp_elig_base where eligible_final_flag = 1 and priority_actual > 0 )
      And FCR.SKF_COMMUNICATION_RECORD NOT IN
          ( 
            SELECT SKF_COMMUNICATION_RECORD FROM AP_CRM.CAMP_COMM_REC_OB
            UNION ALL
            SELECT SKF_COMMUNICATION_RECORD FROM AP_CRM.CAMP_COMM_REC_IB
          )
)
select /*+ FULL( FCR ) USE_HASH (fcr ccl cct ccs css csf cst crt) */
       fcr.date_call, fcr.dtime_inserted, fcr.skf_communication_record, fcr.skp_client, fcr.skp_credit_case,
       fcr.skp_communication_channel, fcr.code_channel, ccl.name_communication_channel,
       fcr.skp_communication_type, fcr.code_type_code, cct.NAME_COMMUNICATION_TYPE,
       fcr.skp_communication_subtype, fcr.code_subtype, ccs.NAME_COMMUNICATION_SUBTYPE,
       fcr.skp_comm_subtype_specif, css.CODE_COMM_SUBTYPE_SPECIF, css.NAME_COMM_SUBTYPE_SPECIF,
       fcr.skp_comm_subtype_sub_specif, csf.CODE_COMM_SUBTYPE_SUB_SPECIF, csf.NAME_COMM_SUBTYPE_SUB_SPECIF,
       fcr.skp_communication_status, fcr.code_status, cst.name_communication_status,
       fcr.skp_communication_result_type, fcr.code_result_type, crt.NAME_COMMUNICATION_RESULT_TYPE,
       fcr.text_note, fcr.text_contact, fcr.employee_number, fcr.common_name
FROM W$PREPARE fcr
left join owner_dwh.cl_communication_channel ccl on fcr.skp_communication_channel = ccl.skp_communication_channel
left join owner_dwh.cl_communication_type cct on fcr.skp_communication_type = cct.SKP_COMMUNICATION_TYPE
left join owner_dwh.cl_communication_subtype ccs on fcr.skp_communication_subtype = ccs.skp_communication_subtype
left join owner_dwh.cl_comm_subtype_specif css on fcr.skp_comm_subtype_specif = css.SKP_COMM_SUBTYPE_SPECIF
left join owner_dwh.cl_comm_subtype_sub_specif csf on fcr.skp_comm_subtype_sub_specif = csf.SKP_COMM_SUBTYPE_SUB_SPECIF
left join owner_dwh.cl_communication_status cst on fcr.skp_communication_status = cst.skp_communication_status
left join owner_Dwh.cl_communication_result_type crt on fcr.skp_communication_result_type = crt.skp_communication_result_type
where (';
		if not (cfg_commlist%ISOPEN) then
					open cfg_commlist;
		end if;
		loop
		fetch cfg_commlist into SKP_COMM_CHANNEL, SKP_COMM_TYPE, SKP_COMM_SUBTYPE, SKP_COMM_SUBTYP_SPEC, SKP_COMM_SUBTYP_SUB_SPEC, SKP_COMM_STATUS, SKP_COMM_RESULT_TYPE;
		exit when cfg_commlist%NOTFOUND;
				 sql_string :=  sql_string || case when substr(sql_string, -1, 1) = '(' then '(' else ' or(' end;
				 if(SKP_COMM_CHANNEL IS NOT null) then
							sql_string := sql_string || case when substr(sql_string,-1,1) <> '(' then ' AND ' else '' end || 'fcr.SKP_COMMUNICATION_CHANNEL = ' || SKP_COMM_CHANNEL;
				 END IF;
				 if(SKP_COMM_TYPE IS NOT null) then
							sql_string := sql_string || case when substr(sql_string,-1,1) <> '(' then ' AND ' else '' end || 'fcr.SKP_COMMUNICATION_TYPE = ' || SKP_COMM_TYPE;
				 END IF;
				 if(SKP_COMM_SUBTYPE IS NOT null) then
							sql_string := sql_string || case when substr(sql_string,-1,1) <> '(' then ' AND ' else '' end || 'fcr.SKP_COMMUNICATION_SUBTYPE = ' || SKP_COMM_SUBTYPE;
				 END IF;
				 if(SKP_COMM_SUBTYP_SPEC IS NOT null) then
							sql_string := sql_string || case when substr(sql_string,-1,1) <> '(' then ' AND ' else '' end || 'fcr.SKP_COMM_SUBTYPE_SPECIF = ' || SKP_COMM_SUBTYP_SPEC;
				 END IF;
				 if(SKP_COMM_SUBTYP_SUB_SPEC IS NOT null) then
							sql_string := sql_string || case when substr(sql_string,-1,1) <> '(' then ' AND ' else '' end || 'fcr.SKP_COMM_SUBTYPE_SUB_SPECIF = ' || SKP_COMM_SUBTYP_SUB_SPEC;
				 END IF;
				 if(SKP_COMM_STATUS IS NOT null) then
							sql_string := sql_string || case when substr(sql_string,-1,1) <> '(' then ' AND ' else '' end || 'fcr.SKP_COMMUNICATION_STATUS = ' || SKP_COMM_STATUS;
				 END IF;
				 if(SKP_COMM_RESULT_TYPE IS NOT null) then
							sql_string := sql_string || case when substr(sql_string,-1,1) <> '(' then ' AND ' else '' end || 'fcr.SKP_COMMUNICATION_RESULT_TYPE = ' || SKP_COMM_RESULT_TYPE;
				 END IF;
				 sql_string := sql_string || ')' || chr(10);
		end loop;
		close cfg_commlist;

		sql_string := sql_string || ');' || chr(10) || 'AP_PUBLIC.CORE_LOG_PKG.pEnd; commit; end;';
    ptruncate('gtt_cmp_rtn_02_commlist');
		AP_PUBLIC.CORE_LOG_PKG.pStart('INS:gtt_cmp_rtn_02_commlist');
    execute immediate sql_string;
		pStats('gtt_cmp_rtn_02_commlist');
		
    AP_PUBLIC.CORE_LOG_PKG.pStart('MERGE:CAMP_COMM_REC_OB');
		merge /*+ PARALLEL(4) */ into camp_comm_rec_ob tgt
		using
		(
				select date_call, dtime_inserted, skf_communication_record, skp_client, skp_credit_case,
							 skp_communication_channel, code_channel, name_communication_channel,
							 skp_communication_type, code_type_code, name_communication_type,
							 skp_communication_subtype, code_subtype, name_communication_subtype,
							 skp_Comm_subtype_specif, code_comm_subtype_specif, name_comm_subtype_specif,
							 skp_Comm_subtype_sub_specif, code_comm_subtype_sub_specif, name_comm_subtype_sub_specif,
							 skp_communication_status, code_status, name_communication_status,
							 skp_communication_result_type, code_result_type, name_communication_result_type,
							 text_note, text_contact, employee_number, common_name
				from gtt_cmp_rtn_02_commlist where skp_communication_channel in (1, 3, 4, 9, 3501)
		)src on (tgt.skf_communication_record = src.skf_communication_record and tgt.skp_client = src.skp_client)
		when not matched then insert
		(
				 date_call, dtime_inserted, skf_communication_record, skp_client, skp_credit_case,
				 skp_communication_channel, code_channel, name_communication_channel,
				 skp_communication_type, code_type_code, name_communication_type,
				 skp_communication_subtype, code_subtype, name_communication_subtype,
				 skp_Comm_subtype_specif, code_comm_subtype_specif, name_comm_subtype_specif,
				 skp_Comm_subtype_sub_specif, code_comm_subtype_sub_specif, name_comm_subtype_sub_specif,
				 skp_communication_status, code_status, name_communication_status,
				 skp_communication_result_type, code_result_type, name_communication_result_type,
				 text_note, text_contact, employee_number, common_name
		)
		values
		(
				 src.date_call, src.dtime_inserted, src.skf_communication_record, src.skp_client, src.skp_credit_case,
				 src.skp_communication_channel, src.code_channel, src.name_communication_channel,
				 src.skp_communication_type, src.code_type_code, src.name_communication_type,
				 src.skp_communication_subtype, src.code_subtype, src.name_communication_subtype,
				 src.skp_Comm_subtype_specif, src.code_comm_subtype_specif, src.name_comm_subtype_specif,
				 src.skp_Comm_subtype_sub_specif, src.code_comm_subtype_sub_specif, src.name_comm_subtype_sub_specif,
				 src.skp_communication_status, src.code_status, src.name_communication_status,
				 src.skp_communication_result_type, src.code_result_type, src.name_communication_result_type,
				 src.text_note, src.text_contact, src.employee_number, src.common_name
		);
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pStats('CAMP_COMM_REC_OB');  
	 
		AP_PUBLIC.CORE_LOG_PKG.pStart('MERGE:CAMP_COMM_REC_IB');
		merge /*+ PARALLEL(4) */ into camp_comm_rec_ib tgt
		using
		(
				select date_call, dtime_inserted, skf_communication_record, skp_client, skp_credit_case,
							 skp_communication_channel, code_channel, name_communication_channel,
							 skp_communication_type, code_type_code, name_communication_type,
							 skp_communication_subtype, code_subtype, name_communication_subtype,
							 skp_Comm_subtype_specif, code_comm_subtype_specif, name_comm_subtype_specif,
							 skp_Comm_subtype_sub_specif, code_comm_subtype_sub_specif, name_comm_subtype_sub_specif,
							 skp_communication_status, code_status, name_communication_status,
							 skp_communication_result_type, code_result_type, name_communication_result_type,
							 text_note, text_contact, employee_number, common_name
				from gtt_cmp_rtn_02_commlist where skp_communication_channel in (5, 7, 8, 6, 107, 13501, 108, 301, 2)
		)src on (tgt.skf_communication_record = src.skf_communication_record and tgt.skp_client = src.skp_client)
		when not matched then insert
		(
				 date_call, dtime_inserted, skf_communication_record, skp_client, skp_credit_case,
				 skp_communication_channel, code_channel, name_communication_channel,
				 skp_communication_type, code_type_code, name_communication_type,
				 skp_communication_subtype, code_subtype, name_communication_subtype,
				 skp_Comm_subtype_specif, code_comm_subtype_specif, name_comm_subtype_specif,
				 skp_Comm_subtype_sub_specif, code_comm_subtype_sub_specif, name_comm_subtype_sub_specif,
				 skp_communication_status, code_status, name_communication_status,
				 skp_communication_result_type, code_result_type, name_communication_result_type,
				 text_note, text_contact, employee_number, common_name
		)
		values
		(
				 src.date_call, src.dtime_inserted, src.skf_communication_record, src.skp_client, src.skp_credit_case,
				 src.skp_communication_channel, src.code_channel, src.name_communication_channel,
				 src.skp_communication_type, src.code_type_code, src.name_communication_type,
				 src.skp_communication_subtype, src.code_subtype, src.name_communication_subtype,
				 src.skp_Comm_subtype_specif, src.code_comm_subtype_specif, src.name_comm_subtype_specif,
				 src.skp_Comm_subtype_sub_specif, src.code_comm_subtype_sub_specif, src.name_comm_subtype_sub_specif,
				 src.skp_communication_status, src.code_status, src.name_communication_status,
				 src.skp_communication_result_type, src.code_result_type, src.name_communication_result_type,
				 src.text_note, src.text_contact, src.employee_number, src.common_name
		);
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pStats('CAMP_COMM_REC_IB');
	  
		AP_PUBLIC.CORE_LOG_PKG.pStart('INS:CAMP_COMM_RES_PART');
		merge /*+ PARALLEL(4) */ into camp_comm_res_part tgt
		using
		(
					select skf_comm_result_part, skf_communication_record, code_comm_result_part, text_value, dtime_inserted from owner_Dwh.f_Comm_Result_Part_Tt
					where skf_communication_record in (select skf_communication_record from gtt_cmp_rtn_02_commlist) and flag_archived = 'N' and flag_deleted = 'N'
		)src on (tgt.skf_comm_result_part = src.skf_comm_result_part)
		when not matched then insert
		(
			 skf_comm_result_part, skf_communication_record, code_comm_result_part, text_value, dtime_inserted
		)
		values
		(
			 src.skf_comm_result_part, src.skf_communication_record, src.code_comm_result_part, src.text_value, src.dtime_inserted
		);
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pStats('CAMP_COMM_RES_PART');
<<finish_line>>    
		AP_PUBLIC.CORE_LOG_PKG.pFinish ;
END;
/

