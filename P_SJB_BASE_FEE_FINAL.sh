#!/bin/bash
#*********************************************************************************************************************
# **  功能描述：  当月DED、历史DED处理，整合所有费用数据
# **  文件格式：  unix | utf-8
# **  版本记录V1.0：<YANGXIAOMENG/20180419/版本初建> 
# ** -----------------------------------------------------------------------------------------------------------------
# **  Copyright 2017 杭州东信北邮信息技术有限公司.All rights reserved.
#*********************************************************************************************************************

#*******************必选参数定义*******************
#变量赋值
###账期
month_id=$1

yyyymm=$1
yyyy=`echo ${yyyymm} | cut -c 1-4`;
mm=`echo ${yyyymm} | cut -c 5-6`;
yyyymm_len=`echo ${yyyymm} | wc -L`;

#账期判断
if [ "${yyyymm_len}" != "6" ] || [ ${mm} -lt 1 ] || [ ${mm} -gt 12 ]; then
echo '输入的日期[ ${yyyymm}] 不合法,请重新输入！'
exit 999
else

#数据库
if [ ! $2 ]; then
  echo "请指定数据库：sh $0 yyyymm db_name"
  exit
else
  db_name=$2
fi
fi

day_id=$month_id"01"
l1_month=`date -d "$day_id -1 month" +%Y%m`
n1_month=`date -d "$day_id +1 month" +%Y%m`
n1_month_fstday=$n1_month"01"

#######插入日志
##/usr/bin/mysql -u root -proot123 -e "
##use agate;  
##insert into agate_exec_log values ('$month_id','dw_orig_settle','/home/hadoop/agate/agate_dw/orig/dw_orig_settle',now(),now(),0,1,'','原创DW开始');
##quit"


#*********************HIVE代码*********************
hive -e "use $db_name;
set mapreduce.job.name=DW:PKG_SJB_1712.PKG_SJB.BASE.FEE.FINAL.sh:$1;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=256000000;
set mapred.min.split.size.per.rack=256000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.jobname.length=10;



alter table mid_sjb_history_ded_m drop if exists partition(record_month='$month_id');
--历史DED
insert into table mid_sjb_history_ded_m  partition (record_month = '$month_id')
 select
       real_month,
       province_id,
       province_name,
       oper_code,
       ded_fee,
       ded_cnt
 from dm_sjb_ded_fee_m --5月修改点：不在匹配历史结算额大于0
 where real_month < '$month_id'
 and record_month = '$month_id';
	   
alter table mid_sjb_normal_ded_m drop if exists partition(record_month='$month_id');
	   --当月DED+在当月有对应产品的历史DED
	   insert into table mid_sjb_normal_ded_m  partition (record_month = '$month_id') 
 select 
 a.province_id,
 a.province_name,
 a.oper_code,
 a.ded_fee,
 a.ded_cnt
 from
 (select 
 a.record_month,
 a.province_id,
 a.province_name,
 a.oper_code,
 sum(a.ded_fee) as ded_fee,
 sum(a.ded_cnt) as ded_cnt
  from 
(select * from dm_sjb_ded_fee_m 
  where record_month = '$month_id'
 and real_month = '$month_id' --当月下发的当月核减数据

 union all
  
 select
a.*
from
(select * from mid_sjb_history_ded_m where record_month = '$month_id')a
join
(select 
 distinct province_id, oper_code
   from dm_sjb_pack_fee_m 
  where record_month = '$month_id') b --历史ded匹配当月包月实收，匹配上则计入当月ded
on a.province_id = b.province_id
  and a.oper_code = b.oper_code
  ) a
    group by a.record_month, a.province_id, a.province_name,a.oper_code
)a;



alter table mid_sjb_share_ded_m drop if exists partition(record_month='$month_id');
--历史DED匹配不上当月实收需按同省份分摊
insert into table mid_sjb_share_ded_m partition (record_month = '$month_id')
select
province_id, ded_fee, ded_cnt
from
(select
 a.record_month,
 a.province_id,
 sum(a.ded_fee) as ded_fee,
 sum(a.ded_cnt) as ded_cnt
  from  
(select 
a.record_month,
a.province_id,
a.ded_fee,
a.ded_cnt,
b.province_id as province_id1,
b.oper_code
from
(select 
record_month,
province_id,
ded_fee,
oper_code,
ded_cnt from mid_sjb_history_ded_m where record_month = '$month_id')a
left outer join
(select 
 distinct province_id, oper_code
   from dm_sjb_pack_fee_m 
  where record_month = '$month_id') b
on a.province_id = b.province_id
  and a.oper_code = b.oper_code
) a where a.province_id1 is null and a.oper_code is null
group by a.record_month, a.province_id) a;


alter table mid_sjb_base_fee_all drop if exists partition(record_month='$month_id');
--整合包月、DED、稽核数据
insert into table mid_sjb_base_fee_all partition (record_month = '$month_id')
select 
 a.province_id,
 a.province_name,
 a.oper_code,
 a.pack_fee,
 a.pack_cnt,
 nvl(b.ded_fee, 0)as ded_fee,
 nvl(c.supprov_fee, 0) as  supprov_fee,
 nvl(c.supprov_cnt, 0)as  supprov_cnt,
 (a.pack_fee - nvl(b.ded_fee, 0) - nvl(c.supprov_fee, 0)) as fee_all
  from 
 (select 
  concat(record_month,province_id,oper_code) as flag,*
  from dm_sjb_pack_fee_m
   where record_month = '$month_id') a
   left outer join
 (select 
 concat(record_month,province_id,oper_code) as flag,
 sum(ded_fee) ded_fee,
 sum(ded_cnt) ded_cnt
  from mid_sjb_normal_ded_m 
 where record_month = '$month_id'
 group by concat(record_month,province_id,oper_code) ) b
 on a.flag = b.flag
 left outer join
 (select 
 concat(record_month,province_id,oper_code) as flag,
 sum(supprov_fee) as supprov_fee,
 sum(supprov_cnt) as supprov_cnt
  from dm_sjb_supprov_fee_m 
 where record_month = '$month_id'
 group by concat(record_month,province_id,oper_code) ) c
 on  a.flag = c.flag;




alter table dm_sjb_share_ded_m drop if exists partition(record_month='$month_id');
   --计算分摊省份金额(数据提供给李晓燕)
    insert into table dm_sjb_share_ded_m partition (record_month = '$month_id')
select
 a.province_id,
 a.oper_code,
 (b.ded_fee / b.code_cnt) as ded_share_fee
 from
 (select record_month,
  province_id,
  oper_code
 from mid_sjb_base_fee_all where record_month = '$month_id'  and fee_all > 0) a
 join
(select 
  a.province_id, b.ded_fee, count(*) as code_cnt ---当结算金额大于0时，需要进行分摊
   from
(select province_id,
        record_month
from mid_sjb_base_fee_all where record_month = '$month_id' and fee_all > 0) a
join
(select ded_fee,province_id,record_month from
    mid_sjb_share_ded_m )b
  on a.province_id = b.province_id
 and a.record_month = b.record_month
  group by a.province_id, b.ded_fee) b
      on a.province_id = b.province_id;
	  
	  
	  
alter table dm_sjb_base_fee_all drop if exists partition(record_month='$month_id');	  
	  --基础数据插入分摊DED
	   insert into table  dm_sjb_base_fee_all partition (record_month = '$month_id')
 select
a.province_id,
a.province_name,
a.oper_code,
a.pack_fee / 100 pack_fee,
a.pack_cnt,
a.ded_fee / 100 ded_fee,
a.supprov_fee / 100 supprov_fee,
a.supprov_cnt,
((a.pack_fee - a.ded_fee - a.supprov_fee) * b.flag / 100) as fee_all
 from (select 
a.record_month,
a.province_id,
a.province_name,
a.oper_code,
a.pack_fee,
a.pack_cnt,
(a.ded_fee + nvl(b.ded_share_fee, 0)) ded_fee,
a.supprov_fee,
a.supprov_cnt
from
(select 
record_month,
province_id,
province_name,
oper_code,
pack_fee,
pack_cnt,
ded_fee,
supprov_fee,
supprov_cnt
 from mid_sjb_base_fee_all where record_month = '$month_id')a
left outer join 
 (select 
 province_id,
 oper_code,
 sum(ded_share_fee) as ded_share_fee
  from dm_sjb_share_ded_m 
 where record_month = '$month_id'
 group by province_id, oper_code) b
on a.province_id = b.province_id
  and a.oper_code = b.oper_code
  ) a
 left outer join
(select oper_code, sum(flag) as flag
   from dm_sjb_cp_opercode_m
  where record_month = '$month_id'
  group by oper_code) b
 on a.oper_code = b.oper_code;
 
 
 
 
 alter table dm_sjb_base_fee_f drop if exists partition(record_month='$month_id');
 --最终包月、DED、稽核汇总表
  insert into table dm_sjb_base_fee_f partition (record_month = '$month_id')
select 
 a.province_id,
 a.province_name,
 a.oper_code,
 a.pack_fee,
 a.pack_cnt,
 a.ded_fee,
 a.supprov_fee,
 a.supprov_cnt,
 (a.fee_all - nvl(c.penalty_fee, 0)) as fee_all,
 nvl(c.penalty_fee, 0) as penalty_fee
  from (select 
 a.record_month,
 a.province_id,
 a.province_name,
 a.oper_code,
 a.pack_fee,
 a.pack_cnt,
 a.ded_fee,
 a.supprov_fee,
 a.supprov_cnt,
 (case
 when (a.pack_fee - a.ded_fee) > 0 and a.fee_all >= 0 then
  a.fee_all
 when (a.pack_fee - a.ded_fee) > 0 and a.fee_all < 0 then
  0
 when (a.pack_fee - a.ded_fee) <= 0 then
  ((a.pack_fee - a.ded_fee) * b.flag)
 else
  null
 end) as fee_all
  from 
(select * from dm_sjb_base_fee_all where record_month = '$month_id') a
left outer join 
(select oper_code, sum(flag) as flag
   from dm_sjb_cp_opercode_m 
  where record_month = '$month_id'
  group by oper_code) b
 on  a.oper_code = b.oper_code
   ) a
 left outer join
(select 
  record_month,
  oper_code,
  province_id,
  sum(penalty_fee) as penalty_fee
   from dm_sjb_opercode_penalty  --营销惩罚
  where record_month = '$month_id'
  group by record_month, oper_code, province_id) c
on a.oper_code = c.oper_code
and a.province_id = c.province_id
order by 3, 1;
"
