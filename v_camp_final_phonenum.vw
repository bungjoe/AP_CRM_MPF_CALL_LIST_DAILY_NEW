create or replace force view v_camp_final_phonenum as
with mobileApp as
(
    select distinct A.ID_CUID,A.Mob_Phone
    from AP_CRM.CAMP_COMPILED_LIST A
    where A.Mob_Phone is not null
)
,Land as
(
    select distinct A.ID_CUID,A.Land_Phone
    from AP_CRM.CAMP_COMPILED_LIST A
    where A.Land_Phone is not null
    and A.Land_Phone not in ( select Mob_Phone from mobileApp)
)
,mobile1 as
(
    select distinct  A.ID_CUID,A.PRIMARYM_1
    from AP_CRM.CAMP_COMPILED_LIST A
    where A.PRIMARYM_1 is not null
    and A.PRIMARYM_1 not in ( select Mob_Phone from mobileApp)
    and A.PRIMARYM_1 not in ( select Land_Phone from Land)
)
,mobile2 as
(
    select distinct A.ID_CUID,A.PRIMARYM_2
    from AP_CRM.CAMP_COMPILED_LIST A
    where 1=1
    and A.PRIMARYM_2 not in ( select Mob_Phone from mobileApp)
    and A.PRIMARYM_2 not in ( select Land_Phone from Land)
    and A.PRIMARYM_2 not in ( select PRIMARYM_1 from mobile1)
    and A.PRIMARYM_2 is not null
)
,mobile3 as
(select distinct A.ID_CUID,A.PRIMARYM_3
 from AP_CRM.CAMP_COMPILED_LIST A
 where A.PRIMARYM_3 is not null
   and A.PRIMARYM_3 not in ( select distinct Mob_Phone from mobileApp)
   and A.PRIMARYM_3 not in ( select distinct Land_Phone from Land)
   and A.PRIMARYM_3 not in ( select distinct PRIMARYM_1 from mobile1)
   and A.PRIMARYM_3 not in ( select distinct PRIMARYM_2 from mobile2)
)
select distinct TList.ID_CUID
      ,coalesce(mobileApp.Mob_Phone,Land.Land_Phone,mobile1.PRIMARYM_1,mobile2.PRIMARYM_2,mobile3.PRIMARYM_3) Phone1
      ,case when coalesce(mobileApp.Mob_Phone,Land.Land_Phone,mobile1.PRIMARYM_1,mobile2.PRIMARYM_2,mobile3.PRIMARYM_3) = coalesce(Land.Land_Phone,mobile1.PRIMARYM_1,mobile2.PRIMARYM_2,mobile3.PRIMARYM_3)
                 then
                    case
                         when mobile1.PRIMARYM_1 is not null and  mobile1.PRIMARYM_1  <> coalesce(mobileApp.Mob_Phone,Land.Land_Phone,mobile1.PRIMARYM_1,mobile2.PRIMARYM_2,mobile3.PRIMARYM_3) then mobile1.PRIMARYM_1
                         when mobile2.PRIMARYM_2 is not null and  mobile2.PRIMARYM_2  <> coalesce(mobileApp.Mob_Phone,Land.Land_Phone,mobile1.PRIMARYM_1,mobile2.PRIMARYM_2,mobile3.PRIMARYM_3) then mobile2.PRIMARYM_2
                         when mobile3.PRIMARYM_3 is not null and  mobile3.PRIMARYM_3  <> coalesce(mobileApp.Mob_Phone,Land.Land_Phone,mobile1.PRIMARYM_1,mobile2.PRIMARYM_2,mobile3.PRIMARYM_3) then mobile3.PRIMARYM_3
                         else null
                         end
             else coalesce(Land.Land_Phone,mobile1.PRIMARYM_1,mobile2.PRIMARYM_2,mobile3.PRIMARYM_3)
        end as Phone2
      ,mobileApp.Mob_Phone,Land.Land_Phone,mobile1.PRIMARYM_1,mobile2.PRIMARYM_2,mobile3.PRIMARYM_3
from AP_CRM.CAMP_COMPILED_LIST TList
left join mobileApp on TList.ID_CUID=mobileApp.ID_CUID
left join Land on TList.ID_CUID=Land.ID_CUID
left join mobile1 on TList.ID_CUID=Mobile1.ID_CUID
left join mobile2 on TList.ID_CUID=Mobile2.ID_CUID
left join mobile3 on TList.ID_CUID=Mobile3.ID_CUID;

