create or replace procedure CAMP_OFFER_CL_RANDOMIZE is
  decile_mark varchar2(10);
  mark1_size decimal(18,2);
  mark2_size decimal(18,2);
  mark3_size decimal(18,2);
  mark4_size decimal(18,2);
  mark1_low integer;
  mark1_high integer;
  mark2_low integer;
  mark2_high integer;
  mark3_low integer;
  mark3_high integer;
  mark4_low integer;
  mark4_high integer;
  pop_size number;
  
  cursor cur_decile is
         SELECT dcl.decile, dcl.mark1/100, dcl.mark2/100, dcl.mark3/100, dcl.mark4/100 from CAMP_CFG_DECILE_SPLIT dcl;
  
pROCEDURE pStats( acTable VARCHAR2, anPerc NUMBER DEFAULT 0.01) IS
BEGIN
    AP_PUBLIC.CORE_LOG_PKG.pStart( 'Stat:'||acTable );
    DBMS_STATS.Gather_Table_Stats( OwnName => 'AP_CRM', TabName => acTable,Estimate_Percent => anPerc );
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
END;
PROCEDURE pTruncate( acTable VARCHAR2)  IS
BEGIN
    AP_PUBLIC.CORE_LOG_PKG.pStart( 'Trunc:'||acTable );
    EXECUTE IMMEDIATE 'TRUNCATE TABLE AP_CRM.'||acTable ;
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
END ;
PROCEDURE print(strings varchar2) IS
BEGIN
   dbms_output.put_line(strings);
END;
          
begin
      if trunc(sysdate) < to_Date('05/01/2018','mm/dd/yyyy') then
         goto finish_line;
      end if;
      AP_PUBLIC.CORE_LOG_PKG.pInit( 'AP_CRM', 'CAMP_RANDOM_SPLIT_OFFER_CL');
      ptruncate('gtt_camp_ptb_score');
      AP_PUBLIC.CORE_LOG_PKG.pStart('Insert gtt_camp_ptb_score');
			insert /*+ APPEND */ into gtt_camp_ptb_score
			with ptb as
			(
					select distinct * from ptb_population where (skp_client, id_cuid, campaign_id) in
					(
							select skp_Client, id_cuid, max(campaign_id)campaign_id from ptb_population where campaign_id between  to_char(sysdate,'yymm')-1 and to_char(sysdate,'yymm')
							group by skp_Client, id_cuid
					)
			)
			/*      select eli.skp_client, ptb.ID_CUID, ptb.PRIORITY, 
						 case when ptb.DECILE > 10 then '10+' else trim(to_char(decile,'09')) end decile, 
						 ptb.PRED score, ptb.MAX_CREDIT_AMOUNT from V_PTB_SCORE_2 ptb
			left join camp_elig_base eli on ptb.ID_CUID = eli.id_cuid
			where campaign_id = to_char(sysdate,'yymm');*/
			select distinct eli.skp_client, ptb.ID_CUID, eli.priority_actual priority, 
			 case when ptb.DECILE > 10 then '10+' else trim(to_char(decile,'09')) end decile, 
			 ptb.score score, eli.ca_limit_final_updated max_credit_amount
			 from ptb
			left join camp_elig_base eli on ptb.ID_CUID = eli.id_cuid;
      AP_PUBLIC.CORE_LOG_PKG.pEnd;
      commit;
      pStats('gtt_camp_ptb_score');
      
      ptruncate('gtt_camp_ff_decile_num');
      AP_PUBLIC.CORE_LOG_PKG.pStart('Insert gtt_camp_ff_decile_num');
      insert /*+ APPEND */ into gtt_camp_ff_decile_num
      with base as
      (
          SELECT distinct cuid, decile, nums orig_nums, trunc(dbms_random.random)rand_nums from camp_offer_call_list_final tdy
                   left join gtt_camp_ptb_score ptb on tdy.cuid = ptb.id_cuid
                   where info2 not like '6.Follow Up - In 1stBOD%'
      ),
      inter_nums as
      (
          select cuid, decile, orig_nums, row_number() over (partition by decile, rand_nums order by rand_nums)int_num from base       
      )
      select cuid, decile, orig_nums, int_num, row_number() over (partition by decile order by int_num)final_num from inter_nums;
      AP_PUBLIC.CORE_LOG_PKG.pEnd;
      commit;
      pstats('gtt_camp_ff_decile_num');
      
      ptruncate('GTT_CAMP_FF_DEC_MARK');
      if not (cur_decile%ISOPEN) then
         open cur_decile;
      end if;
      loop
      fetch cur_decile into decile_mark, mark1_size, mark2_size, mark3_size, mark4_size;
      exit when cur_decile%NOTFOUND;
           select count(tdy.cuid) into pop_size from gtt_camp_ff_decile_num tdy
           where tdy.decile = decile_mark;
           
           if pop_size = 0 then  
              goto skip_decile_marking1;
           end if;
/*           dbms_output.put_line(pop_size);
           print('Decile : ' || decile_mark);
           mark1_size := ceil(pop_size * mark1_size);
           mark2_size := ceil(pop_size * mark2_size);
           mark3_size := ceil(pop_size * mark3_size);
           mark4_size := ceil(pop_size * mark4_size);
           print('Mark 1 Split : ' || mark1_size);
           print('Mark 2 Split : ' || mark2_size);
           print('Mark 3 Split : ' || mark3_size);
           print('Mark 4 Split : ' || mark4_size);
           
           mark1_low  := 1;
           mark1_high := mark1_size;
           mark2_low  := mark1_high +1;
           mark2_high := mark2_low + mark2_size;
           mark3_low  := mark2_high + 1;
           mark3_high := mark3_low + mark3_size;
           mark4_low  := mark3_high + 1;
           mark4_high := mark4_low + mark4_size;
           
           print('Mark 1 Range : ' || mark1_low || ' to ' || mark1_high);
           print('Mark 2 Range : ' || mark2_low || ' to ' || mark2_high);
           print('Mark 3 Range : ' || mark3_low || ' to ' || mark3_high);
           print('Mark 4 Range : ' || mark4_low || ' to ' || mark4_high);*/

           mark1_size := ceil(pop_size * mark1_size);
           mark2_size := ceil(pop_size * mark2_size);
           mark3_size := ceil(pop_size * mark3_size);
           mark4_size := ceil(pop_size * mark4_size);           
           mark1_low  := 1;
           mark1_high := mark1_size;
           mark2_low  := mark1_high +1;
           mark2_high := mark2_low + mark2_size;
           mark3_low  := mark2_high + 1;
           mark3_high := mark3_low + mark3_size;
           mark4_low  := mark3_high + 1;
           mark4_high := mark4_low + mark4_size;
           
           AP_PUBLIC.CORE_LOG_PKG.pStart('Insert MK1 to gtt_camp_ff_dec_mark');
           insert /*+ APPEND */ into gtt_camp_ff_dec_mark
           select cuid, decile, orig_nums, 'MK1' from gtt_camp_ff_decile_num 
           where decile = decile_mark
             and final_num between mark1_low and mark1_high;
           AP_PUBLIC.CORE_LOG_PKG.pEnd;
           commit;
             
           AP_PUBLIC.CORE_LOG_PKG.pStart('Insert MK2 to gtt_camp_ff_dec_mark');
           insert /*+ APPEND */ into gtt_camp_ff_dec_mark
           select cuid, decile, orig_nums, 'MK2' from gtt_camp_ff_decile_num 
           where decile = decile_mark
             and final_num between mark2_low and mark2_high;
           AP_PUBLIC.CORE_LOG_PKG.pEnd;  
           commit;
           
           AP_PUBLIC.CORE_LOG_PKG.pStart('Insert MK3 to gtt_camp_ff_dec_mark');  
           insert /*+ APPEND */ into gtt_camp_ff_dec_mark
           select cuid, decile, orig_nums, 'MK3' from gtt_camp_ff_decile_num 
           where decile = decile_mark
             and final_num between mark3_low and mark3_high;
           AP_PUBLIC.CORE_LOG_PKG.pEnd;
           commit;
           
           AP_PUBLIC.CORE_LOG_PKG.pStart('Insert MK4 to gtt_camp_ff_dec_mark');
           insert /*+ APPEND */ into gtt_camp_ff_dec_mark
           select cuid, decile, orig_nums, 'MK4' from gtt_camp_ff_decile_num 
           where decile = decile_mark
             and final_num between mark4_low and mark4_high;
           AP_PUBLIC.CORE_LOG_PKG.pEnd;
           commit;  
      <<skip_decile_marking1>>
      pop_size := 0;
      end loop;
      close cur_decile;
      
      ptruncate('gtt_camp_ff_decile_num');
      AP_PUBLIC.CORE_LOG_PKG.pStart('Insert gtt_camp_ff_decile_num');
      insert /*+ APPEND */ into gtt_camp_ff_decile_num
      with base as
      (
          SELECT cuid, decile, nums orig_nums, trunc(dbms_random.random)rand_nums from camp_offer_call_list_final tdy
                   left join gtt_camp_ptb_score ptb on tdy.cuid = ptb.id_cuid
                   where info2 like '6.Follow Up - In 1stBOD%'
      ),
      inter_nums as
      (
          select cuid, decile, orig_nums, row_number() over (partition by decile, rand_nums order by rand_nums)int_num from base       
      )
      select cuid, decile, orig_nums, int_num, row_number() over (partition by decile order by int_num)final_num from inter_nums;
      AP_PUBLIC.CORE_LOG_PKG.pEnd;
      commit;
      pstats('gtt_camp_ff_decile_num');
      
      if not (cur_decile%ISOPEN) then
         open cur_decile;
      end if;
      loop
      fetch cur_decile into decile_mark, mark1_size, mark2_size, mark3_size, mark4_size;
      exit when cur_decile%NOTFOUND;
           select count(tdy.cuid) into pop_size from gtt_camp_ff_decile_num tdy
           where tdy.decile = decile_mark;
           
           if pop_size = 0 then  
              goto skip_decile_marking2;
           end if;

           mark1_size := ceil(pop_size * mark1_size);
           mark2_size := ceil(pop_size * mark2_size);
           mark3_size := ceil(pop_size * mark3_size);
           mark4_size := ceil(pop_size * mark4_size);           
           mark1_low  := 1;
           mark1_high := mark1_size;
           mark2_low  := mark1_high +1;
           mark2_high := mark2_low + mark2_size;
           mark3_low  := mark2_high + 1;
           mark3_high := mark3_low + mark3_size;
           mark4_low  := mark3_high + 1;
           mark4_high := mark4_low + mark4_size;
           
           AP_PUBLIC.CORE_LOG_PKG.pStart('Insert MK1 to gtt_camp_ff_dec_mark');
           insert /*+ APPEND */ into gtt_camp_ff_dec_mark
           select cuid, decile, orig_nums, 'MK1' from gtt_camp_ff_decile_num 
           where decile = decile_mark
             and final_num between mark1_low and mark1_high;
           AP_PUBLIC.CORE_LOG_PKG.pEnd;
           commit;
             
           AP_PUBLIC.CORE_LOG_PKG.pStart('Insert MK2 to gtt_camp_ff_dec_mark');
           insert /*+ APPEND */ into gtt_camp_ff_dec_mark
           select cuid, decile, orig_nums, 'MK2' from gtt_camp_ff_decile_num 
           where decile = decile_mark
             and final_num between mark2_low and mark2_high;
           AP_PUBLIC.CORE_LOG_PKG.pEnd;  
           commit;
           
           AP_PUBLIC.CORE_LOG_PKG.pStart('Insert MK3 to gtt_camp_ff_dec_mark');  
           insert /*+ APPEND */ into gtt_camp_ff_dec_mark
           select cuid, decile, orig_nums, 'MK3' from gtt_camp_ff_decile_num 
           where decile = decile_mark
             and final_num between mark3_low and mark3_high;
           AP_PUBLIC.CORE_LOG_PKG.pEnd;
           commit;
           
           AP_PUBLIC.CORE_LOG_PKG.pStart('Insert MK4 to gtt_camp_ff_dec_mark');
           insert /*+ APPEND */ into gtt_camp_ff_dec_mark
           select cuid, decile, orig_nums, 'MK4' from gtt_camp_ff_decile_num 
           where decile = decile_mark
             and final_num between mark4_low and mark4_high;
           AP_PUBLIC.CORE_LOG_PKG.pEnd;
           commit;  
      <<skip_decile_marking2>>
      pop_size := 0;
      end loop;
      close cur_decile;
      pstats('gtt_camp_ff_dec_mark'); 
      
      AP_PUBLIC.CORE_LOG_PKG.pStart('Merge to CAMP_OFFER_CALL_LIST_FINAL');
      merge into CAMP_OFFER_CALL_LIST_FINAL tgt
      using
      (
           select cuid, nums, markings from gtt_camp_ff_dec_mark 
      )src on (tgt.cuid = src.cuid and tgt.nums = src.nums)
      when matched then update
           set tgt.name_district = tgt.name_district || ', ' || src.markings,
					     tgt.info4 = src.markings,
							 tgt.date_generated = trunc(sysdate);
      AP_PUBLIC.CORE_LOG_PKG.pEnd;
      commit;
<<finish_line>>      
      pstats('CAMP_OFFER_CALL_LIST_FINAL'); 
AP_PUBLIC.CORE_LOG_PKG.pFinish ;      
end;
