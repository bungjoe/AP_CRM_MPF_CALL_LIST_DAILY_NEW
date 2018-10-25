create or replace procedure CAMP_UPD_BLACK_LIST is
    PROCEDURE pStats( acTable VARCHAR2, anPerc NUMBER DEFAULT 0.01) IS
    BEGIN
        AP_PUBLIC.CORE_LOG_PKG.pStart( 'Stat:'||upper(acTable) );
        DBMS_STATS.Gather_Table_Stats( OwnName => 'AP_CRM', TabName => upper(acTable),Estimate_Percent => anPerc );
        AP_PUBLIC.CORE_LOG_PKG.pEnd;
    END;
    PROCEDURE pTruncate( acTable VARCHAR2) IS
    BEGIN
        AP_PUBLIC.CORE_LOG_PKG.pStart( 'Trunc:'||upper(acTable) );
        EXECUTE IMMEDIATE 'TRUNCATE TABLE AP_CRM.'||upper(acTable) ;
        AP_PUBLIC.CORE_LOG_PKG.pEnd ;
    END;
begin
    AP_PUBLIC.CORE_LOG_PKG.pInit( 'AP_CRM', 'CAMP_BLACK_LIST_UPD');
		/************************* Update black list table both temporary and permanent based on ticket created in BSL *******************************/
    ptruncate('gtt_camp_complaints');
    pstats('gtt_camp_complaints');
    AP_PUBLIC.CORE_LOG_PKG.pStart('Get Raw Regular Complaints');
    insert /*+ APPEND */ into gtt_camp_complaints
    select distinct task.SKF_EMPLOYEE_TASK, task.DATE_EFFECTIVE, tt.skp_employee_task_type, st.skp_employee_task_subtype,
           comm.skp_client, comm.skp_credit_case, tt.code_employee_task_type, st.code_employee_task_subtype,
           rol.NAME_EMPLOYEE_TASK_ACCESS_ROLE, tt.name_employee_task_type, st.name_employee_task_subtype,
           task.CODE_TASK, task.TEXT_TASK_NOTE,
           emp.NAME_COMMON name_employee, emp.CODE_EMPLOYEE,
           dcc.text_contract_number, dcl.id_cuid
    from owner_dwh.f_employee_task_at task
    left join owner_dwh.f_employee_tt emp on task.SKP_EMPLOYEE_SUBMITTER = emp.SKP_EMPLOYEE and emp.CODE_STATUS = 'a' and emp.flag_deleted = 'N' --and emp.flag_current = 'Y'
    left join owner_dwh.F_EMPLOYEE_TASK_REL_SUBJ_TT coms on task.SKF_EMPLOYEE_TASK = coms.SKF_EMPLOYEE_TASK
    left join owner_Dwh.f_communication_record_tt comm on coms.SKF_COMMUNICATION_RECORD = comm.skf_communication_record
    left join owner_dwh.dc_credit_case dcc on dcc.skp_credit_case = comm.skp_credit_case
    left join owner_Dwh.dc_client dcl on comm.skp_client = dcl.skp_client
    left join owner_dwh.cl_employee_task_subtype st on task.SKP_EMPLOYEE_TASK_SUBTYPE = st.skp_employee_task_subtype and st.code_status = 'a'
    left join owner_dwh.cl_employee_task_type tt on st.skp_employee_task_type = tt.skp_employee_task_type and tt.code_status = 'a'
    left join owner_Dwh.cl_employee_task_access_role rol on task.SKP_EMPLOYEE_TASK_ACCESS_ROLE = rol.SKP_EMPLOYEE_TASK_ACCESS_ROLE
    where 1=1
      and task.DATE_EFFECTIVE >= trunc(sysdate-10)
      and st.skp_employee_task_type in (1401, 5)
      and task.SKP_EMPLOYEE_TASK_SUBTYPE in (240, 238, 3503)
      and task.FLAG_DELETED = 'N'
      and comm.skp_client is not null;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pstats('gtt_camp_complaints');

    AP_PUBLIC.CORE_LOG_PKG.pStart('Merges Regular Complaints');
    merge into camp_compl_reg tgt
    using
    (
        select SKF_EMPLOYEE_TASK, DATE_EFFECTIVE, skp_employee_task_type, skp_employee_task_subtype,
               skp_client, skp_credit_case, code_employee_task_type, code_employee_task_subtype,
               NAME_EMPLOYEE_TASK_ACCESS_ROLE, name_employee_task_type, name_employee_task_subtype,
               CODE_TASK, TEXT_TASK_NOTE,
               name_employee, CODE_EMPLOYEE,
               text_contract_number, id_cuid
        from gtt_camp_complaints
    )src on (tgt.SKF_EMPLOYEE_TASK = src.SKF_EMPLOYEE_TASK)
    when not matched then insert
    (
         tgt.SKF_EMPLOYEE_TASK, tgt.DATE_EFFECTIVE, tgt.skp_employee_task_type, tgt.skp_employee_task_subtype,
         tgt.skp_client, tgt.skp_credit_case, tgt.code_employee_task_type, tgt.code_employee_task_subtype,
         tgt.NAME_EMPLOYEE_TASK_ACCESS_ROLE, tgt.name_employee_task_type, tgt.name_employee_task_subtype,
         tgt.CODE_TASK, tgt.TEXT_TASK_NOTE,
         tgt.name_employee, tgt.CODE_EMPLOYEE,
         tgt.text_contract_number, tgt.id_cuid
    )
    values
    (
         src.SKF_EMPLOYEE_TASK, src.DATE_EFFECTIVE, src.skp_employee_task_type, src.skp_employee_task_subtype,
         src.skp_client, src.skp_credit_case, src.code_employee_task_type, src.code_employee_task_subtype,
         src.NAME_EMPLOYEE_TASK_ACCESS_ROLE, src.name_employee_task_type, src.name_employee_task_subtype,
         src.CODE_TASK, src.TEXT_TASK_NOTE,
         src.name_employee, src.CODE_EMPLOYEE,
         src.text_contract_number, src.id_cuid
    );
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pstats('camp_compl_reg');

    ptruncate('gtt_camp_complaints');
    pstats('gtt_camp_complaints');
    AP_PUBLIC.CORE_LOG_PKG.pStart('Get Raw VIP Complaints');
    insert /*+ APPEND */ into gtt_camp_complaints
    select distinct task.SKF_EMPLOYEE_TASK, task.DATE_EFFECTIVE, tt.skp_employee_task_type, st.skp_employee_task_subtype,
           comm.skp_client, comm.skp_credit_case, tt.code_employee_task_type, st.code_employee_task_subtype,
           rol.NAME_EMPLOYEE_TASK_ACCESS_ROLE, tt.name_employee_task_type, st.name_employee_task_subtype,
           task.CODE_TASK, task.TEXT_TASK_NOTE,
           emp.NAME_COMMON name_employee, emp.CODE_EMPLOYEE,
           dcc.text_contract_number, dcl.id_cuid
    from owner_dwh.f_employee_task_at task
    left join owner_dwh.f_employee_tt emp on task.SKP_EMPLOYEE_SUBMITTER = emp.SKP_EMPLOYEE and emp.CODE_STATUS = 'a' and emp.flag_deleted = 'N' --and emp.flag_current = 'Y'
    left join owner_dwh.F_EMPLOYEE_TASK_REL_SUBJ_TT coms on task.SKF_EMPLOYEE_TASK = coms.SKF_EMPLOYEE_TASK
    left join owner_Dwh.f_communication_record_tt comm on coms.SKF_COMMUNICATION_RECORD = comm.skf_communication_record
    left join owner_dwh.dc_credit_case dcc on dcc.skp_credit_case = comm.skp_credit_case
    left join owner_Dwh.dc_client dcl on comm.skp_client = dcl.skp_client
    left join owner_dwh.cl_employee_task_subtype st on task.SKP_EMPLOYEE_TASK_SUBTYPE = st.skp_employee_task_subtype and st.code_status = 'a'
    left join owner_dwh.cl_employee_task_type tt on st.skp_employee_task_type = tt.skp_employee_task_type and tt.code_status = 'a'
    left join owner_Dwh.cl_employee_task_access_role rol on task.SKP_EMPLOYEE_TASK_ACCESS_ROLE = rol.SKP_EMPLOYEE_TASK_ACCESS_ROLE
    where 1=1
      and task.DATE_EFFECTIVE >= trunc(sysdate-10)
      and st.skp_employee_task_type = 2402
      and task.FLAG_DELETED = 'N'
      and comm.skp_client in (select skp_client from camp_elig_base eb where eb.eligible_final_flag = 1);
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pstats('gtt_camp_complaints');

    AP_PUBLIC.CORE_LOG_PKG.pStart('Merges VIP Complaints');
    merge into camp_compl_vip tgt
    using
    (
        select SKF_EMPLOYEE_TASK, DATE_EFFECTIVE, skp_employee_task_type, skp_employee_task_subtype,
               skp_client, skp_credit_case, code_employee_task_type, code_employee_task_subtype,
               NAME_EMPLOYEE_TASK_ACCESS_ROLE, name_employee_task_type, name_employee_task_subtype,
               CODE_TASK, TEXT_TASK_NOTE,
               name_employee, CODE_EMPLOYEE,
               text_contract_number, id_cuid
        from gtt_camp_complaints
    )src on (tgt.SKF_EMPLOYEE_TASK = src.SKF_EMPLOYEE_TASK)
    when not matched then insert
    (
         tgt.SKF_EMPLOYEE_TASK, tgt.DATE_EFFECTIVE, tgt.skp_employee_task_type, tgt.skp_employee_task_subtype,
         tgt.skp_client, tgt.skp_credit_case, tgt.code_employee_task_type, tgt.code_employee_task_subtype,
         tgt.NAME_EMPLOYEE_TASK_ACCESS_ROLE, tgt.name_employee_task_type, tgt.name_employee_task_subtype,
         tgt.CODE_TASK, tgt.TEXT_TASK_NOTE,
         tgt.name_employee, tgt.CODE_EMPLOYEE,
         tgt.text_contract_number, tgt.id_cuid
    )
    values
    (
         src.SKF_EMPLOYEE_TASK, src.DATE_EFFECTIVE, src.skp_employee_task_type, src.skp_employee_task_subtype,
         src.skp_client, src.skp_credit_case, src.code_employee_task_type, src.code_employee_task_subtype,
         src.NAME_EMPLOYEE_TASK_ACCESS_ROLE, src.name_employee_task_type, src.name_employee_task_subtype,
         src.CODE_TASK, src.TEXT_TASK_NOTE,
         src.name_employee, src.CODE_EMPLOYEE,
         src.text_contract_number, src.id_cuid
    );
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pstats('camp_compl_vip');

    AP_PUBLIC.CORE_LOG_PKG.pStart('Merges Camp Black List');
    merge into camp_black_list tgt
    using
    (
        select distinct id_cuid, trunc(nvl(dtime_inserted, date_effective))date_block, 'COMPLAINT' Source, code_task source_identification,
               t.name_employee_task_subtype block_reason, null for_num_days, 'Y' block_forever, 'A' status
        from camp_compl_vip t where (id_cuid, skf_employee_task) in
        (
            select id_cuid, max(skf_employee_task)skf_employee_task
            from camp_compl_vip
            where(id_cuid, nvl(dtime_inserted,to_Date('01/01/2000','mm/dd/yyyy'))) in
            (
               select id_cuid, max(nvl(dtime_inserted, nvl(dtime_inserted,to_Date('01/01/2000','mm/dd/yyyy')))) from camp_compl_vip group by id_cuid
            )
            group by id_cuid
        )
    )src on (tgt.cuid = src.id_cuid)
    when not matched then insert
    (
         tgt.cuid, tgt.date_block, tgt.source, tgt.source_identification, tgt.block_reason, tgt.for_num_days, tgt.block_forever, tgt.status
    )
    values
    (
         src.id_cuid, src.date_block, src.source, src.source_identification, src.block_reason, src.for_num_days, src.block_forever, src.status
    );
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pstats('camp_black_list');

    AP_PUBLIC.CORE_LOG_PKG.pStart('Merges Camp Black List 90 Days');
    merge into camp_black_list_temporary tgt
    using
    (
        select distinct id_cuid, trunc(nvl(dtime_inserted, date_effective))date_block, 'COMPLAINT' Source, code_task source_identification, t.name_employee_task_subtype block_reason,
               90 for_num_days, 'N' block_forever, 'A' status
        from camp_compl_reg t where (id_cuid, skf_employee_task) in
        (
            select id_cuid, max(skf_employee_task)skf_employee_task
            from camp_compl_reg
            where(id_cuid, nvl(dtime_inserted,to_Date('01/01/2000','mm/dd/yyyy'))) in
            (
               select id_cuid, max(nvl(dtime_inserted, nvl(dtime_inserted,to_Date('01/01/2000','mm/dd/yyyy')))) from camp_compl_reg group by id_cuid
            )
            group by id_cuid
        )
    )src on (tgt.cuid = src.id_cuid)
    when matched then update
         set tgt.date_block = src.date_block,
             tgt.source = src.source,
             tgt.source_identification = src.source_identification,
             tgt.block_reason = src.block_reason,
             tgt.status = 'A', tgt.block_forever = 'N', tgt.for_num_days = 90
    when not matched then insert
    (
         tgt.cuid, tgt.date_block, tgt.source, tgt.source_identification, tgt.block_reason, tgt.for_num_days, tgt.block_forever, tgt.status
    )
    values
    (
         src.id_cuid, src.date_block, src.source, src.source_identification, src.block_reason, src.for_num_days, src.block_forever, src.status
    );
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
    commit;
    pstats('camp_black_list_temporary');
		/********************************************************* end of line ***********************************************************************/

		/************************* Update pernament black list table base on communication record created in BSL *************************************/
    if (trunc(sysdate) < to_date('08/10/2018','mm/dd/yyyy')) then
			 goto finish_line;
	  end if;
		ptruncate('gtt_cmp_rtn_02_commlist');
    AP_PUBLIC.CORE_LOG_PKG.pStart('Compile all comm marked as exclusion');
		insert /*+ APPEND */ into gtt_cmp_rtn_02_commlist
		select /*+ PARALLEL(5) USE_HASH (fcr ccl ccls cct ccs css csf cst crt) */
					 fcr.date_call, fcr.dtime_inserted, fcr.skf_communication_record, fcr.skp_client, fcr.skp_credit_case,
					 fcr.skp_communication_channel, fcr.code_channel, ccls.name_communication_channel,
					 fcr.skp_communication_type, fcr.code_type_code, cct.NAME_COMMUNICATION_TYPE,
					 fcr.skp_communication_subtype, fcr.code_subtype, ccs.NAME_COMMUNICATION_SUBTYPE,
					 fcr.skp_comm_subtype_specif, css.CODE_COMM_SUBTYPE_SPECIF, css.NAME_COMM_SUBTYPE_SPECIF,
					 fcr.skp_comm_subtype_sub_specif, csf.CODE_COMM_SUBTYPE_SUB_SPECIF, csf.NAME_COMM_SUBTYPE_SUB_SPECIF,
					 fcr.skp_communication_status, fcr.code_status, cst.name_communication_status,
					 fcr.skp_communication_result_type, fcr.code_result_type, crt.NAME_COMMUNICATION_RESULT_TYPE,
					 fcr.text_note, fcr.text_contact, fcr.employee_number, fcr.common_name
		FROM ap_crm.camp_comm_rec_ob fcr
		inner join camp_cfg_comm_list ccl on ccl.name_campaign in ('FF Regular','FF General') and nvl(ccl.logic_group,'-') in ('Exclusion') and ccl.active = 'Y'
			 and nvl(fcr.skp_communication_channel,-1) = case when ccl.skp_communication_channel is null then nvl(fcr.skp_communication_channel,-1) else ccl.skp_communication_channel end
			 and nvl(fcr.skp_communication_type,-1) = case when ccl.skp_communication_type is null then nvl(fcr.skp_communication_type,-1) else ccl.skp_communication_type end
			 and nvl(fcr.skp_communication_subtype,-1) = case when ccl.skp_communication_subtype is null then nvl(fcr.skp_communication_subtype,-1) else ccl.skp_communication_subtype end
			 and nvl(fcr.skp_comm_subtype_specif,-1) = case when ccl.skp_comm_subtype_specif is null then nvl(fcr.skp_comm_subtype_specif,-1) else  ccl.skp_comm_subtype_specif end
			 and nvl(fcr.skp_comm_subtype_sub_specif,-1) = case when ccl.skp_comm_subtype_sub_specif is null then nvl(fcr.skp_comm_subtype_sub_specif,-1) else ccl.skp_comm_subtype_sub_specif end
			 and nvl(fcr.skp_communication_status,-1) = case when ccl.skp_communication_status is null then nvl(fcr.skp_communication_status,-1) else ccl.skp_communication_status end
			 and nvl(fcr.skp_communication_result_type,-1) = case when ccl.skp_communication_result_type is null then nvl(fcr.skp_communication_result_type,-1) else ccl.skp_communication_result_type end
		left join owner_dwh.cl_communication_channel ccls on fcr.skp_communication_channel = ccls.skp_communication_channel
		left join owner_dwh.cl_communication_type cct on fcr.skp_communication_type = cct.SKP_COMMUNICATION_TYPE
		left join owner_dwh.cl_communication_subtype ccs on fcr.skp_communication_subtype = ccs.skp_communication_subtype
		left join owner_dwh.cl_comm_subtype_specif css on fcr.skp_comm_subtype_specif = css.SKP_COMM_SUBTYPE_SPECIF
		left join owner_dwh.cl_comm_subtype_sub_specif csf on fcr.skp_comm_subtype_sub_specif = csf.SKP_COMM_SUBTYPE_SUB_SPECIF
		left join owner_dwh.cl_communication_status cst on fcr.skp_communication_status = cst.skp_communication_status
		left join owner_Dwh.cl_communication_result_type crt on fcr.skp_communication_result_type = crt.skp_communication_result_type
		where fcr.date_call < trunc(sysdate-95)
			and fcr.skp_client in (select skp_client from camp_elig_base where eligible_final_flag = 1 and priority_actual > 0)
			and fcr.skp_client not in (select skp_client from camp_black_list cbl inner join owner_Dwh.dc_Client dcl on cbl.cuid = dcl.id_cuid where lower(cbl.status) = 'a')
		union
		select /*+ PARALLEL(5) USE_HASH (fcr ccl ccls cct ccs css csf cst crt) */
					 fcr.date_call, fcr.dtime_inserted, fcr.skf_communication_record, fcr.skp_client, fcr.skp_credit_case,
					 fcr.skp_communication_channel, fcr.code_channel, ccls.name_communication_channel,
					 fcr.skp_communication_type, fcr.code_type_code, cct.NAME_COMMUNICATION_TYPE,
					 fcr.skp_communication_subtype, fcr.code_subtype, ccs.NAME_COMMUNICATION_SUBTYPE,
					 fcr.skp_comm_subtype_specif, css.CODE_COMM_SUBTYPE_SPECIF, css.NAME_COMM_SUBTYPE_SPECIF,
					 fcr.skp_comm_subtype_sub_specif, csf.CODE_COMM_SUBTYPE_SUB_SPECIF, csf.NAME_COMM_SUBTYPE_SUB_SPECIF,
					 fcr.skp_communication_status, fcr.code_status, cst.name_communication_status,
					 fcr.skp_communication_result_type, fcr.code_result_type, crt.NAME_COMMUNICATION_RESULT_TYPE,
					 fcr.text_note, fcr.text_contact, fcr.employee_number, fcr.common_name
		FROM ap_crm.camp_comm_rec_ib fcr
		inner join camp_cfg_comm_list ccl on ccl.name_campaign in ('FF Regular','FF General') and nvl(ccl.logic_group,'-') in ('Exclusion') and ccl.active = 'Y'
			 and nvl(fcr.skp_communication_channel,-1) = case when ccl.skp_communication_channel is null then nvl(fcr.skp_communication_channel,-1) else ccl.skp_communication_channel end
			 and nvl(fcr.skp_communication_type,-1) = case when ccl.skp_communication_type is null then nvl(fcr.skp_communication_type,-1) else ccl.skp_communication_type end
			 and nvl(fcr.skp_communication_subtype,-1) = case when ccl.skp_communication_subtype is null then nvl(fcr.skp_communication_subtype,-1) else ccl.skp_communication_subtype end
			 and nvl(fcr.skp_comm_subtype_specif,-1) = case when ccl.skp_comm_subtype_specif is null then nvl(fcr.skp_comm_subtype_specif,-1) else  ccl.skp_comm_subtype_specif end
			 and nvl(fcr.skp_comm_subtype_sub_specif,-1) = case when ccl.skp_comm_subtype_sub_specif is null then nvl(fcr.skp_comm_subtype_sub_specif,-1) else ccl.skp_comm_subtype_sub_specif end
			 and nvl(fcr.skp_communication_status,-1) = case when ccl.skp_communication_status is null then nvl(fcr.skp_communication_status,-1) else ccl.skp_communication_status end
			 and nvl(fcr.skp_communication_result_type,-1) = case when ccl.skp_communication_result_type is null then nvl(fcr.skp_communication_result_type,-1) else ccl.skp_communication_result_type end
		left join owner_dwh.cl_communication_channel ccls on fcr.skp_communication_channel = ccls.skp_communication_channel
		left join owner_dwh.cl_communication_type cct on fcr.skp_communication_type = cct.SKP_COMMUNICATION_TYPE
		left join owner_dwh.cl_communication_subtype ccs on fcr.skp_communication_subtype = ccs.skp_communication_subtype
		left join owner_dwh.cl_comm_subtype_specif css on fcr.skp_comm_subtype_specif = css.SKP_COMM_SUBTYPE_SPECIF
		left join owner_dwh.cl_comm_subtype_sub_specif csf on fcr.skp_comm_subtype_sub_specif = csf.SKP_COMM_SUBTYPE_SUB_SPECIF
		left join owner_dwh.cl_communication_status cst on fcr.skp_communication_status = cst.skp_communication_status
		left join owner_Dwh.cl_communication_result_type crt on fcr.skp_communication_result_type = crt.skp_communication_result_type
		where fcr.date_call < trunc(sysdate-95)
			and fcr.skp_client in (select skp_client from camp_elig_base where eligible_final_flag = 1 and priority_actual > 0)
			and fcr.skp_client not in (select skp_client from camp_black_list cbl inner join owner_Dwh.dc_Client dcl on cbl.cuid = dcl.id_cuid where lower(cbl.status) = 'a');
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
    pstats('gtt_cmp_rtn_02_commlist');

		AP_PUBLIC.CORE_LOG_PKG.pStart('Merge into camp_black_list from listcomm');
		merge into camp_black_list tgt
		using
		(
				select dcl.id_cuid, ccl.date_call date_block, 'LISTCOMM' source,
							 'SKF_COMMUNICATION_RECORD = ' || ccl.skf_communication_record source_identification,
							 ccl.text_note block_reason, null for_num_days, 'Y' block_forever, 'A' status
				from gtt_cmp_rtn_02_commlist ccl
				left join owner_Dwh.dc_Client dcl on ccl.skp_client = dcl.skp_client
				where (ccl.skp_Client, ccl.skp_credit_case, ccl.dtime_inserted) in
				(
						select skp_client, skp_Credit_case, min(dtime_inserted) from  gtt_cmp_rtn_02_commlist
						group by skp_Client, skp_Credit_case
				)
		)src on (tgt.cuid = src.id_cuid)
		when not matched then insert
		(
				 tgt.cuid, tgt.date_block, tgt.source, tgt.source_identification, tgt.block_reason, tgt.for_num_days, tgt.block_forever, tgt.status
		)
		values
		(
				 src.id_cuid, src.date_block, src.source, src.source_identification, src.block_reason, src.for_num_days, src.block_forever, src.status
		);
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pstats('camp_black_list');
		/********************************************************* end of line ***********************************************************************/
<<finish_line>>
AP_PUBLIC.CORE_LOG_PKG.pFinish;
end;
/

