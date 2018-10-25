create or replace force view v_camp_final_email_addr as
with Land as 
(select distinct A.ID_CUID,A.LAND_EMAIL 
 from AP_CRM.CAMP_COMPILED_LIST A
 where A.LAND_EMAIL is not null),

Mobile_app as
 (select distinct A.ID_CUID,A.Mob_Email 
  from AP_CRM.CAMP_COMPILED_LIST A
  where A.Land_Phone is not null 
  and A.Mob_Email is not null
)

select tlist.ID_CUID,coalesce(Mobile_app.Mob_Email,Land.LAND_EMAIL,tlist.CLIENT_EMAIL) email
from AP_CRM.CAMP_COMPILED_LIST tlist
left join Land on tlist.ID_CUID = Land.ID_CUID
left join Mobile_app on tlist.ID_CUID = Mobile_app.ID_CUID;

