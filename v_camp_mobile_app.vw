create or replace force view v_camp_mobile_app as
select distinct A."CLIENT_ID",A."CUID",A."CREATED_DATE",Email,PHONE
from
(
    select CLIENT_ID,CUID,max(CREATED_DATE) CREATED_DATE
    from AP_BICC.STGV_MOB_FLEXI_FAST
    where trunc(sysdate)-trunc(CREATED_DATE) <=30
    group by CLIENT_ID,CUID
) A
inner join
(
    select distinct CREATED_DATE CREATED_DATE,CLIENT_ID,CUID,Email,PHONE,USER_ID
    from AP_BICC.STGV_MOB_FLEXI_FAST
    where trunc(sysdate)-trunc(CREATED_DATE) <=30
)B on A.CLIENT_ID=B.CLIENT_ID and A.CUID=B.CUID and A.CREATED_DATE=B.CREATED_DATE
where b.user_id not in
(
    select user_id --, phone, email, CALL_ME_DTIME_TO 
    from ap_bicc.stgv_mob_call_me
    where flexi_fast_id is not null
    and call_me_dtime_to >= trunc(sysdate)
)
;

