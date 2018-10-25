CREATE OR REPLACE FORCE VIEW V_CAMP_LANDING_PAGE AS
with base as
(
    select /*+ MATERIALIZE */ initcap(lp.customer_name)customer_name, trim(lower(lp.email))email, lp.dtime_item_created dtime_created, lp.source_info,
    case when regexp_replace(lp.text_phone_number, '[^0-9]', '') like '62%' then SUBSTR(regexp_replace(lp.text_phone_number, '[^0-9]', ''), 3, 20)
    when regexp_replace(lp.text_phone_number, '[^0-9]', '') like '0%' then SUBSTR(trim(regexp_replace(lp.text_phone_number, '[^0-9]', '')) , 2, 20)
    else regexp_replace(lp.text_phone_number, '[^0-9]', '') end as mobile1
    from AP_BICC.F_HCID_LANDING_PAGE_TT lp
    where trim(lower(email)) not like '%test%'
    and dtime_item_created >= trunc(sysdate-60,'DD')
),
contacts as
(
  select /*+ MATERIALIZE */ * from
  (
    SELECT /*+ PARALLEL(2) */ distinct FAC.SKP_CLIENT, trim(FAC.TEXT_EMAIL)TEXT_CONTACT
    FROM OWNER_DWH.F_APPLICATION_CLIENT_TT FAC
    inner join owner_Dwh.dc_client dcl on fac.skp_client = dcl.skp_client
    WHERE trim(FAC.TEXT_EMAIL) <> 'XNA' and dcl.code_status = 'a'
    and fac.skp_client <> -1 and trim(FAC.TEXT_EMAIL) in (select email from base)
    union
    SELECT /*+ PARALLEL(2) */ distinct CLC.SKP_CLIENT, CLC.TEXT_CONTACT
    FROM OWNER_DWH.F_CLIENT_CONTACT_TT CLC
    inner join owner_Dwh.dc_client dcl on clc.skp_client = dcl.skp_client
    JOIN OWNER_DWH.CL_CONTACT_TYPE CT2 ON CLC.SKP_CONTACT_TYPE = CT2.SKP_CONTACT_TYPE AND CT2.ID_SOURCE IN ('PRIMARY_MOBILE', 'SECONDARY_MOBILE', 'MOBILE')
    WHERE CLC.FLAG_DELETED = 'N' AND CLC.FLAG_CURRENT = 'Y' AND CLC.CODE_STATUS = 'a' and clc.skp_client <> -1 and dcl.code_status = 'a'
    and clc.text_contact in (select mobile1 from base)
  )
)
select distinct trunc(b.dtime_created)date_created, b.source_info, cons.skp_client, b.customer_name, b.email,  A.mobile1 from
(
    select mobile1, max(dtime_created)dtime_created from base group by mobile1
)A
inner join
(
    select distinct customer_name, email, dtime_created, source_info, mobile1 from base
)B on a.mobile1 = b.mobile1 and a.dtime_created = b.dtime_created
left join contacts cons on b.mobile1 = cons.text_contact or b.email = cons.text_contact;

