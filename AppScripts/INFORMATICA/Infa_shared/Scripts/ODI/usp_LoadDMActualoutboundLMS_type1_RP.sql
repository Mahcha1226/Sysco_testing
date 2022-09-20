SET time zone 'UTC';

DROP TABLE IF EXISTS #actualsdelta_rp;

DROP TABLE IF EXISTS #zerodrivetime_rp;

CREATE TEMP TABLE #actualsdelta_rp DISTSTYLE ALL AS
select a.rectype,a.func_id ,a.seq_num,a.seq_num as trans_num,a.assign_num,a.plan_date,a.split_indicator,a.ref_id,a.client_id,a.cstnum,a.container_license,a.route_num,
a.activity_type,a.worktype_id,a.machineid,a.warehouse_id,a.client_slot_id,a.num_pallet,a.num_layer,a.num_case,a.num_inner,a.num_each,a.num_ship,a.item_num,
a.item_desc,a."cube",a.weight,a.response_type,a.special_1,a.special_2,a.special_3,a.special_4,a.special_5,a.user_def_1,a.user_def_2,a.user_def_3,a.user_def_4,a.user_def_5,
a.usr_id,a.sign_on_time,a.copy_seq_num,a.discrete_proc_id,a.proc_repetition,a.last_discrete_move,a.assignment_flg,a.map_field_1,a.map_field_2,a.map_field_3,a.map_field_4,
a.map_field_5,a.map_field_6,a.map_field_7,a.map_field_8,a.map_field_9,a.map_field_10,a.map_field_11,a.map_field_12,a.map_field_13,a.map_field_14,
a.map_field_15,a.map_field_16,a.map_field_17,a.map_field_18,a.map_field_19,a.map_field_20,
 case when o.assign_num  is not null then o.insert_datetime else a.insert_datetime end  as insert_datetime
,case when o.assign_num is  not null then cast(getdate() as varchar(225) ) else null  end as update_datetime  
,a.md5
from ((select rectype,case when rnk = 1 then 'A' else 'N' end as func_id ,seq_num,seq_num as trans_num,assign_num,plan_date,
split_indicator,ref_id,client_id,cstnum,container_license,route_num,activity_type,worktype_id,machineid,
warehouse_id,client_slot_id,num_pallet,num_layer,num_case,num_inner,num_each,num_ship,item_num, item_desc, 
"cube", weight, response_type, special_1, special_2, special_3, special_4, special_5, user_def_1, user_def_2,
 user_def_3, user_def_4, user_def_5, usr_id, sign_on_time, copy_seq_num, discrete_proc_id, proc_repetition, 
 case when rnk = 1 then '1' else '0' end as last_discrete_move, assignment_flg, map_field_1, map_field_2, map_field_3, map_field_4, map_field_5, map_field_6, 
 map_field_7, map_field_8, map_field_9, map_field_10, map_field_11, map_field_12, map_field_13, map_field_14, 
 map_field_15, map_field_16, map_field_17, map_field_18, map_field_19, map_field_20,insert_datetime
, update_datetime ,md5   from (
select rank() over(partition by r.assign_num order by  r.seq_num desc,r.sign_on_time desc) as rnk,
r.rectype,r.func_id,r.seq_num,r.seq_num as trans_num,r.assign_num,r.plan_date,r.split_indicator,r.ref_id,r.client_id,r.cstnum,r.container_license,r.route_num,r.activity_type,r.worktype_id,r.machineid,r.warehouse_id,r.client_slot_id,r.num_pallet,r.num_layer,r.num_case,r.num_inner,r.num_each,r.num_ship,r.item_num,r. item_desc,r. "cube",r. weight,r. response_type,r. special_1,r. special_2,r. special_3,r. special_4,r. special_5,r. user_def_1,r. user_def_2,r. user_def_3,r. user_def_4,r. user_def_5,r. usr_id,r. sign_on_time,r. copy_seq_num,r. discrete_proc_id,r. proc_repetition,r. last_discrete_move,r. assignment_flg,r. map_field_1,r. map_field_2,r. map_field_3,r. map_field_4,r. map_field_5,r. map_field_6,r. map_field_7,r. map_field_8,r. map_field_9,r. map_field_10,r. map_field_11,r. map_field_12,r. map_field_13,r. map_field_14,r. map_field_15,r. map_field_16,r. map_field_17,r. map_field_18,r. map_field_19,r. map_field_20,r.insert_datetime
, update_datetime 
,r.md5  
from (
select 
cast(r.rectype as varchar(225) ),cast(r.func_id as varchar(225) ),cast(1 as varchar(225) ) as seq_num,cast(r.trans_num as varchar(225) ),cast(r.assign_num as varchar(225) ) as assign_num,
cast(r.plan_date as varchar(225) ),cast(r.split_indicator as varchar(225) ),cast(r.ref_id as varchar(225) ),cast(r.client_id as varchar(225) ),cast(r.cstnum as varchar(225) )
,cast(r.container_license as varchar(225) ),cast(r.route_num as varchar(225) ),cast(r.activity_type as varchar(225) ),cast(r.worktype_id as varchar(225) )
,cast(r.machineid as varchar(225) ),cast(r.warehouse_id as varchar(225) ),cast(r.client_slot_id as varchar(225) ),cast(null as varchar(225) ) as num_pallet 
,cast(r.num_layer as varchar(225) ),cast(r.num_case as varchar(225) ),cast(r.num_inner as varchar(225) ),cast(r.num_each as varchar(225) ),cast(r.num_ship as varchar(225) )
,cast(r.item_num as varchar(225) ),cast(r.item_desc as varchar(225) ),
cast((case when nvl(r.special_5,0) > 0 then nvl(r."cube",0)/nvl(r.special_5,0) else 0 end) as varchar(225) )  as "cube"
,cast((case when nvl(r.special_5,0) > 0 then nvl(r.weight,0)/nvl(r.special_5,0) else 0 end) as varchar(225) )  as weight
,cast(r.response_type as varchar(225) )
,cast(r.special_1 as varchar(225) ),cast(r.special_2 as varchar(225) ),cast(r.special_3 as varchar(225) ),cast(r.special_4 as varchar(225) ),cast(r.special_5 as varchar(225) ),
cast(r.user_def_1 as varchar(225) ),cast(r.user_def_2 as varchar(225) ),cast(r.user_def_3 as varchar(225) ),cast(r.user_def_4 as varchar(225) ),cast(r.user_def_5 as varchar(225) ),
cast(r.usr_id as varchar(225) ),cast(r.sign_on_time as varchar(225) ),cast(r.copy_seq_num as varchar(225) ),cast(r.discrete_proc_id as varchar(225) ),
cast(r.proc_repetition as varchar(225) ),cast(r.last_discrete_move as varchar(225) ),cast(r.assignment_flg as varchar(225) ),cast(r.map_field_1 as varchar(225) ),
cast(r.map_field_2 as varchar(225) ),cast(r.map_field_3 as varchar(225) ),cast(r.map_field_4 as varchar(225) ),cast(r.map_field_5 as varchar(225) ),
cast(r.map_field_6 as varchar(225) ),cast(r.map_field_7 as varchar(225) ),cast(r.map_field_8 as varchar(225) ),cast(r.map_field_9 as varchar(225) ),
cast(r.map_field_10 as varchar(225) ),cast(r.map_field_11 as varchar(225) ),cast(r.map_field_12 as varchar(225) ),cast(r.map_field_13 as varchar(225) ),
cast(r.map_field_14 as varchar(225) ),cast(r.map_field_15 as varchar(225) ),cast(r.map_field_16 as varchar(225) ),cast(r.map_field_17 as varchar(225) ),
cast(r.map_field_18 as varchar(225) ),cast(r.map_field_19 as varchar(225) ),cast(r.map_field_20 as varchar(225) ),
cast(getdate() as varchar(225) ) as insert_datetime,null as update_datetime
,md5(r.assign_num||r.sign_on_time||nvl(r.sign_on_time_drive,'0')) as md5
from dm.lms_actual_rt_rp r
where 
r.sign_on_time  is not null 
union all 

select
cast(r.rectype as varchar(225) ),cast(r.func_id as varchar(225) ),case when num_pallet > 0 then '2' else '1' end  as seq_num,cast(r.trans_num as varchar(225) ),
cast(r.assign_num as varchar(225) ),cast(r.plan_date as varchar(225) ),cast(null as varchar(225) ) as split_indicator ,cast(r.ref_id as varchar(225) ),
cast(r.client_id as varchar(225) ),cast(r.cstnum as varchar(225) ),cast(r.container_license as varchar(225) ),cast(r.route_num as varchar(225) ),
cast(r.activity_type as varchar(225) ),cast(r.worktype_id as varchar(225) ),cast(r.machineid as varchar(225) ),cast(r.warehouse_id as varchar(225) ),
cast(r.client_slot_id as varchar(225) ),cast(r.num_pallet as varchar(225) ),cast(null as varchar(225) ),cast(null as varchar(225) ),cast(null as varchar(225) ),
cast(null as varchar(225) ),cast(r.num_ship as varchar(225) ),cast(r.item_num as varchar(225) ),cast(r.item_desc as varchar(225) ),cast(null as varchar(225) ),
cast(null as varchar(225) ),cast(r.response_type as varchar(225) ),cast(null as varchar(225) ),cast(r.special_2 as varchar(225) ),cast(r.special_3 as varchar(225) ),
cast(r.special_4 as varchar(225) ),cast(null as varchar(225) ),
cast(r.user_def_1 as varchar(225) ),cast(r.user_def_2 as varchar(225) ),cast(r.user_def_3 as varchar(225) ),cast(r.user_def_4 as varchar(225) ),cast(r.user_def_5 as varchar(225) ),
cast(r.usr_id as varchar(225) ),cast(r.sign_on_time as varchar(225) ),cast(r.copy_seq_num as varchar(225) ),cast(r.discrete_proc_id as varchar(225) ),
cast(r.proc_repetition as varchar(225) ),cast(r.last_discrete_move as varchar(225) ),cast(r.assignment_flg as varchar(225) ),cast(r.map_field_1 as varchar(225) ),
cast(r.map_field_2 as varchar(225) ),cast(r.map_field_3 as varchar(225) ),cast(r.map_field_4 as varchar(225) ),cast(r.map_field_5 as varchar(225) ),
cast(r.map_field_6 as varchar(225) ),cast(r.map_field_7 as varchar(225) ),cast(r.map_field_8 as varchar(225) ),cast(r.map_field_9 as varchar(225) ),
cast(r.map_field_10 as varchar(225) ),cast(r.map_field_11 as varchar(225) ),cast(r.map_field_12 as varchar(225) ),cast(r.map_field_13 as varchar(225) ),
cast(r.map_field_14 as varchar(225) ),cast(r.map_field_15 as varchar(225) ),cast(r.map_field_16 as varchar(225) ),cast(r.map_field_17 as varchar(225) ),
cast(r.map_field_18 as varchar(225) ),cast(r.map_field_19 as varchar(225) ),cast(r.map_field_20 as varchar(225) ),
cast(getdate() as varchar(225) ) as insert_datetime,null as update_datetime
,md5(r.assign_num||r.sign_on_time||nvl(r.sign_on_time_drive,'0')||nvl(num_pallet,'0')) as md5
from dm.lms_actual_rt_rp r
where 
r.sign_on_time  is not null and num_pallet >0 and r.ref_id > 1

union all
select 
 cast(rectype as varchar(225) )  ,cast(func_id as varchar(225) ),case when num_pallet > 0 then '3' else '2' end  as seq_num ,cast(trans_num as varchar(225) ),
 cast(assign_num as varchar(225) ),cast(plan_date as varchar(225) ),null,cast( ref_id_discrete as varchar(225) ),null,cast(cstnum_discrete as varchar(225) ),
 null,cast(route_num as varchar(225) ),cast( activity_type_discrete as varchar(225) ),cast(worktype_id as varchar(225) ),'Truck',cast(warehouse_id as varchar(225) ),
 cast(client_slot_id as varchar(225) ),
 null,cast( num_pallet_discrete as varchar(225) ),
 cast( num_case_discrete as varchar(225) ),null,null,cast( num_ship_discrete as varchar(225) ),null,null,cast( cube_discrete as varchar(225) ),
 cast( weight_discrete as varchar(225) ),null,null,null,null,null,cast( special_5_discrete as varchar(225) ),null,null,null,null,null,cast(usr_id as varchar(225) ),
 cast(sign_on_time as varchar(225) ),null,cast( 'RETURN' as varchar(225) ),cast(proc_repetition_discrete as varchar(225) ),
 null,null,cast(map_field_1 as varchar(225) )  ,cast(map_field_2 as varchar(225) ),null,null,null,null,null,null,null,null,null,
 null,null,null,null,null,null,null,null,null,cast(getdate() as varchar(225) ) ,NULL 
, md5(assign_num||sign_on_time||nvl(sign_on_time_drive,'0')||nvl(proc_repetition_discrete,'0'))
 from dm.lms_actual_rt_rp 
 where return_ind ='Y'
) r
)
)
union all 
(
select 
cast( rectype  as varchar(225)),cast( 'A' as varchar(225)),cast( '1' as varchar(225)),cast( '1' as varchar(225)),cast( assign_num_drive as varchar(225)),
cast(plan_date as varchar(225)),null,cast( ref_id_drive as varchar(225)),null,cast( cstnum_drive as varchar(225)),null,cast( route_num as varchar(225)),
cast( 'O' as varchar(225)) as activity_type,cast( worktype_id_drive as varchar(225)),cast( machineid_drive as varchar(225)),
cast(warehouse_id as varchar(225) ),cast(client_slot_id as varchar(225) ),cast( num_pallet_drive as varchar(225)),cast( num_layer_drive as varchar(225)),cast( num_case_drive as varchar(225)),
null,null,cast( num_ship_drive as varchar(225)),null,null,cast( cube_drive as varchar(225)),cast(weight_drive as varchar(225)),null,null,null,null,null,
cast( null as varchar(225)),null,null,null,null,null,cast(usr_id as varchar(225) ),cast( sign_on_time_drive as varchar(225)),null,cast( discrete_proc_id_drive as varchar(225)),
cast( proc_repetition_drive as varchar(225)),cast('1' as varchar(225) ) as last_discrete_move,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,
cast(getdate() as varchar(225)),null
,md5(assign_num_drive||nvl(sign_on_time_drive,'0'))
from dm.lms_actual_rt_rp where sign_on_time_drive  <> sign_on_time and 
sign_on_time_drive is not null   
   union all 
select 
cast( rectype  as varchar(225)),cast( 'A' as varchar(225)),cast( '1' as varchar(225)),cast( '1' as varchar(225)),cast( assign_num_am as varchar(225)),
cast(plan_date as varchar(225)),cast(null as varchar(225)),cast( ref_id_am as varchar(225)),cast(null as varchar(225)),cast(null as varchar(225)),
cast(null as varchar(225)),cast(route_num as varchar(225)),cast( activity_type_am as varchar(225)),cast( worktype_id_am as varchar(225)),cast( machineid_am as varchar(225)),
cast(warehouse_id as varchar(225) ),cast(client_slot_id as varchar(225) ),cast( num_pallet_am as varchar(225)),cast( num_layer_am as varchar(225)),cast( num_case_am as varchar(225)),
cast(null as varchar(225)),cast(null as varchar(225)),cast( num_ship_am as varchar(225)),cast(null as varchar(225)),cast(null as varchar(225)),cast( cube_am as varchar(225)),
cast( weight_am as varchar(225)),cast(null as varchar(225)),cast(null as varchar(225)),cast(null as varchar(225)),cast(null as varchar(225)),cast(null as varchar(225)),
cast( special_5_am as varchar(225)),null,cast(null as varchar(225)),cast(null as varchar(225)),cast(null as varchar(225)),cast(null as varchar(225)),
cast(usr_id as varchar(225) ),cast( sign_on_time_am as varchar(225)),cast(null as varchar(225)),cast(null as varchar(225)),cast( proc_repetition_am as varchar(225)),
cast('1' as varchar(225)),null,cast( map_field_1_am as varchar(225)),cast( map_field_2_am as varchar(225)),null,null,null,null,null,null,null,null,null,null,null,null,
null,null,null,null,null,null,cast( getdate() as varchar(225)),NULL 
,md5(cast(assign_num_am as varchar(225))||'AM'||sign_on_time_am)
from dm.lms_actual_rt_rp where 
am_ind = 'Y' and sign_on_time_am is not null

union all
select 
cast( rectype  as varchar(225)),cast( 'A' as varchar(225)),cast( '1' as varchar(225)),cast( '1' as varchar(225)),cast( assign_num_pm as varchar(225)),
cast(plan_date as varchar(225)),cast(null as varchar(225)),cast( ref_id_pm as varchar(225)),cast(null as varchar(225)),cast(null as varchar(225)),
cast(null as varchar(225)),cast(route_num as varchar(225)),cast( activity_type_pm as varchar(225)),cast( worktype_id_pm as varchar(225)),cast( machineid_pm as varchar(225)),
cast(warehouse_id as varchar(225) ),cast(client_slot_id as varchar(225) ),cast( num_pallet_pm as varchar(225)),cast( num_layer_pm as varchar(225)),cast( num_case_pm as varchar(225)),
cast(null as varchar(225)),cast(null as varchar(225)),cast( num_ship_pm as varchar(225)),cast(null as varchar(225)),cast(null as varchar(225)),cast( cube_pm as varchar(225)),
cast( weight_pm as varchar(225)),cast(null as varchar(225)),cast(null as varchar(225)),cast(null as varchar(225)),cast(null as varchar(225)),cast(null as varchar(225)),
cast( special_5_pm as varchar(225)),null,cast(null as varchar(225)),cast(null as varchar(225)),cast(null as varchar(225)),cast(null as varchar(225)),
cast(usr_id as varchar(225) ),cast( sign_on_time_pm as varchar(225)),cast(null as varchar(225)),cast(null as varchar(225)),cast( proc_repetition_pm as varchar(225)),
cast('1' as varchar(225)) as last_discrete_move,null,cast( map_field_1_pm as varchar(225)),cast( map_field_2_pm as varchar(225)),null,null,null,null,null,null,null,null,null,null,null,null,
cast(null as varchar(225) ) as last_discrete_move,null,null,null,null,null,cast( getdate() as varchar(225)),NULL 
,md5(cast(assign_num_pm as varchar(225))||'PM'||sign_on_time_pm)
from dm.lms_actual_rt_rp
where 
pm_ind = 'Y' and sign_on_time_pm is not null)) a
left join dm.lms_actual_outbound_type1_rp o
on  a.assign_num =  o.assign_num and a.worktype_id = o.worktype_id and a.seq_num = o.seq_num and nvl(a.ref_id,'-99') = nvl(o.ref_id,'-99') and 
nvl(a.plan_date,'-99') = nvl(o.plan_date,'-99')--and o.func_id <> 'D' 
where 
 nvl(a.md5,'-99') <> nvl(o.md5,'-99');

begin;
/* delete if existing */
delete   from  dm.lms_actual_outbound_type1_rp
using
#actualsdelta_rp o
where   lms_actual_outbound_type1_rp.plan_date >= sysdate-4
and nvl(lms_actual_outbound_type1_rp.assign_num,'-99') =  nvl(o.assign_num,'-99') 
and nvl(lms_actual_outbound_type1_rp.worktype_id,'-99') = nvl(o.worktype_id,'-99') 
and nvl(lms_actual_outbound_type1_rp.seq_num,'-99') = nvl(o.seq_num,'-99')
and nvl(lms_actual_outbound_type1_rp.plan_date,'-99') = nvl(o.plan_date,'-99');

/*insert all */

insert into  dm.lms_actual_outbound_type1_rp
select  r.rectype, func_id,r.seq_num,r.trans_num as trans_num,r.assign_num,r.plan_date,r.split_indicator,r.ref_id,r.client_id,r.cstnum,r.container_license,
r.route_num,r.activity_type,r.worktype_id,r.machineid,r.warehouse_id,r.client_slot_id,r.num_pallet,
case when r.num_pallet is not null then r.num_pallet else null end as num_layer,
r.num_case,r.num_inner,r.num_each,r.num_ship,
r.item_num,r. item_desc,r. "cube",r. weight,r. response_type,r. special_1,r. special_2,r. special_3,r. special_4,r. special_5,r. user_def_1,r. user_def_2,
r. user_def_3,r. user_def_4,r. user_def_5,r. usr_id,r. sign_on_time,r. copy_seq_num,r. discrete_proc_id,r. proc_repetition,r. last_discrete_move,
r. assignment_flg,r. map_field_1,r. map_field_2,r. map_field_3,r. map_field_4,r. map_field_5,r. map_field_6,r. map_field_7,r. map_field_8,r. map_field_9,
r. map_field_10,r. map_field_11,r. map_field_12,r. map_field_13,r. map_field_14,r. map_field_15,r. map_field_16,r. map_field_17,r. map_field_18,r. map_field_19,
r. map_field_20,r.insert_datetime
, update_datetime ,r.md5,'N'  from #actualsdelta_rp r 
order by cstnum,seq_num,sign_on_time ;


CREATE TEMP TABLE #zerodrivetime_rp DISTSTYLE ALL AS
select a.* from
(select route_num,plan_date,warehouse_id,sign_on_time,assign_num from dm.lms_actual_outbound_type1_rp where  worktype_id in ('Drive')) a inner join 
(select route_num,plan_date,warehouse_id,sign_on_time,count(1) from dm.lms_actual_outbound_type1_rp where  
--route_num = 3090 and plan_date = 20211201 and 
worktype_id in ('Delivery','Drive')
group by route_num,plan_date,warehouse_id,sign_on_time
having count(1) >1 ) b on
a.route_num = b.route_num and a.plan_date = b.plan_date and a.warehouse_id = b.warehouse_id and a.sign_on_time = b.sign_on_time;

update dm.lms_actual_outbound_type1_rp set is_drivetimezero = 'Y' 
from #zerodrivetime_rp d 
where lms_actual_outbound_type1_rp.assign_num = d.assign_num
and lms_actual_outbound_type1_rp.worktype_id in ('Drive');

--This part has been updated .Fixed the zerodrive time issue:2022/01/26----------
DROP TABLE IF EXISTS #testdrive_rp;
DROP TABLE IF EXISTS #testdelivery_rp;

	
CREATE TEMP TABLE #testdrive_rp DISTSTYLE ALL AS
select route_num,plan_date,warehouse_id,sign_on_time,assign_num,is_drivetimezero 
from dm.lms_actual_outbound_type1_rp
where  worktype_id in ('Drive');

CREATE TEMP TABLE #testdelivery_rp DISTSTYLE ALL AS
select route_num,plan_date,warehouse_id,sign_on_time,assign_num 
from dm.lms_actual_outbound_type1_rp 
where  worktype_id in ('Delivery');

update dm.lms_actual_outbound_type1_rp 
set is_drivetimezero = 'N' , update_datetime = cast(getdate() as varchar(225)) 
from
	(select distinct a.assign_num,is_drivetimezero,a.warehouse_id,a.sign_on_time
		from #testdrive_rp a left outer join #testdelivery_rp b
			on b.sign_on_time =a.sign_on_time
				and a.route_num = b.route_num 
				and a.plan_date = b.plan_date
				and a.warehouse_id=b.warehouse_id
		where b.assign_num is null and a.is_drivetimezero='Y') d
where lms_actual_outbound_type1_rp.assign_num = d.assign_num
and lms_actual_outbound_type1_rp.worktype_id in ('Drive');

delete  from  dm.lms_actual_outbound_type1_rp where insert_datetime <= CURRENT_TIMESTAMP-14;

commit;