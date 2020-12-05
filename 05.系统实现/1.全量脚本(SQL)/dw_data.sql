-- 此脚本 主要用于存储 DW层相关的操作
-- 数据的转换工作: 从 ODS层 将数据 导入到 DWD层

-- 生成DWD数据的转换SQL:
select 
wce.session_id,
wce.sid,
unix_timestamp(wce.create_time,"yyyy-MM-dd HH:mm:ss") AS create_time,
wce.seo_source,
wce.ip,wce.area,
cast( if( wce.msg_count is null , 0 ,wce.msg_count ) as int ) as msg_count,
wce.origin_channel,
wcte.referrer, wcte.from_url, 
wcte.landing_page_url, wcte.url_title, 
wcte.platform_description, wcte.other_params, 
wcte.history,
substr(wce.create_time,12,2) as hourinfo,
substr(wce.create_time,1,4) as yearinfo ,
quarter(wce.create_time) as quarterinfo,
substr(wce.create_time,6,2) as monthinfo,
substr(wce.create_time,9,2) as dayinfo
from  itcast_ods.web_chat_ems wce join itcast_ods.web_chat_text_ems wcte
on wce.id = wcte.id ;


————————2020-12-04 完成上述代码；————————————

-- 将上述SQL的执行结果 导入到DWD层的表
--动态分区配置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
--hive压缩
set hive.exec.compress.intermediate=true;
set hive.exec.compress.output=true;
--写入时压缩生效
set hive.exec.orc.compression.strategy=COMPRESSION;

insert into table itcast_dwd.visit_consult_dwd partition(yearinfo,quarterinfo,monthinfo,dayinfo)
select 
wce.session_id,
wce.sid,
unix_timestamp(wce.create_time,"yyyy-MM-dd HH:mm:ss") AS create_time,
wce.seo_source,
wce.ip,wce.area,
cast( if( wce.msg_count is null , 0 ,wce.msg_count ) as int ) as msg_count,
wce.origin_channel,
wcte.referrer, wcte.from_url, 
wcte.landing_page_url, wcte.url_title, 
wcte.platform_description, wcte.other_params, 
wcte.history,
substr(wce.create_time,12,2) as hourinfo,
substr(wce.create_time,1,4) as yearinfo ,
quarter(wce.create_time) as quarterinfo,
substr(wce.create_time,6,2) as monthinfo,
substr(wce.create_time,9,2) as dayinfo
from  itcast_ods.web_chat_ems wce join itcast_ods.web_chat_text_ems wcte
on wce.id = wcte.id ;

————————2020-12-05 上午完成DWD层数据导入————————————


-- 从 DWD 层 生成 DWS层的数据: 数据分析
--访问量: 
  -- 维度:  时间维度  地区维度 来源渠道  搜索来源  受访页面

-- 1) 统计 总访问量  
-- 思考:
-- 需求:  
-- 统计总访问量需求
-- 统计每一年的总访问量数据 
insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select    
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1'  AS area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as  hourinfo,
    yearinfo as time_str,
    '-1' as from_url,
    '5' as grouptype,
    '5' as time_type,
    yearinfo,
    '-1' as quarterinfo,
    '-1' as monthinfo,
    '-1' as dayinfo
from  itcast_dwd.visit_consult_dwd group by yearinfo;
-- 统计每年每季度的总访问量数据
insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select    
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1'  AS area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as  hourinfo,
    concat(yearinfo,'-',quarterinfo) as time_str,
    '-1' as from_url,
    '5' as grouptype,
    '4' as time_type,
    yearinfo,
    quarterinfo,
    '-1' as monthinfo,
    '-1' as dayinfo
from  itcast_dwd.visit_consult_dwd group by yearinfo,quarterinfo;
-- 统计每年每季度每月的总访问量数据
insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select    
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1'  AS area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as  hourinfo,
    concat(yearinfo,'-',monthinfo) as time_str,
    '-1' as from_url,
    '5' as grouptype,
    '3' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    '-1' as dayinfo
from  itcast_dwd.visit_consult_dwd group by yearinfo,quarterinfo,monthinfo;
-- 统计每年每季度每月每天的总访问量数据
insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select    
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1'  AS area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as  hourinfo,
    concat(yearinfo,'-',monthinfo,'-',dayinfo) as time_str,
    '-1' as from_url,
    '5' as grouptype,
    '2' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    dayinfo
from  itcast_dwd.visit_consult_dwd group by yearinfo,quarterinfo,monthinfo,dayinfo;
-- 统计每年每季度每月每天每小时的总访问量数据
insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select    
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    '-1'  AS area,
    '-1' as seo_source,
    '-1' as origin_channel,
    hourinfo,
    concat(yearinfo,'-',monthinfo,'-',dayinfo,' ',hourinfo) as time_str,
    '-1' as from_url,
    '5' as grouptype,
    '1' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    dayinfo
from  itcast_dwd.visit_consult_dwd group by yearinfo,quarterinfo,monthinfo,dayinfo,hourinfo;
--访问量: 
  -- 维度:  时间维度  地区维度
需求: 
-- 统计每一年各个地区的总访问量数据
insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select    
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as  hourinfo,
    yearinfo as time_str,
    '-1' as from_url,
    '1' as grouptype,
    '5' as time_type,
    yearinfo,
    '-1' as quarterinfo,
    '-1' as monthinfo,
    '-1' as dayinfo
from  itcast_dwd.visit_consult_dwd group by yearinfo,area;

-- 统计每年每季度各个地区的总访问量数据
insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select    
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as  hourinfo,
    concat(yearinfo ,'-' ,quarterinfo)as time_str,
    '-1' as from_url,
    '1' as grouptype,
    '4' as time_type,
    yearinfo,
    quarterinfo,
    '-1' as monthinfo,
    '-1' as dayinfo
from  itcast_dwd.visit_consult_dwd group by yearinfo,quarterinfo,area;
-- 统计每年每季度每月各个地区的总访问量数据
insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select    
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as  hourinfo,
    concat(yearinfo ,'-' ,monthinfo)as time_str,
    '-1' as from_url,
    '1' as grouptype,
    '3' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    '-1' as dayinfo
from  itcast_dwd.visit_consult_dwd group by yearinfo,quarterinfo,monthinfo,area;
-- 统计每年每季度每月每天各个地区的总访问量数据
insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select    
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as seo_source,
    '-1' as origin_channel,
    '-1' as  hourinfo,
    concat(yearinfo ,'-' ,monthinfo,'-',dayinfo)as time_str,
    '-1' as from_url,
    '1' as grouptype,
    '2' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    dayinfo
from  itcast_dwd.visit_consult_dwd group by yearinfo,quarterinfo,monthinfo,dayinfo,area;
-- 统计每年每季度每月每天每小时各个地区的总访问量数据
insert into table itcast_dws.visit_dws partition (yearinfo,quarterinfo,monthinfo,dayinfo)
select    
    count(distinct sid) as sid_total,
    count(distinct session_id) as sessionid_total,
    count(distinct ip) as ip_total,
    area,
    '-1' as seo_source,
    '-1' as origin_channel,
    hourinfo,
    concat(yearinfo ,'-' ,monthinfo,'-',dayinfo,' ',hourinfo)as time_str,
    '-1' as from_url,
    '1' as grouptype,
    '1' as time_type,
    yearinfo,
    quarterinfo,
    monthinfo,
    dayinfo
from  itcast_dwd.visit_consult_dwd group by yearinfo,quarterinfo,monthinfo,dayinfo,hourinfo,area;

-- 此处关于基于搜索来源 来源渠道 以及来访页面维度 省略  


--  咨询量的数据统计操作: 
指标: 咨询量
维度: 时间维度 地区维度 来源渠道

需求:  
-- 按年统计每年总的咨询量
insert into table itcast_dws.consult_dws partition(yearinfo,quarterinfo,monthinfo,dayinfo)
select  
count(distinct sid) as sid_total,
count(distinct session_id) as sessionid_total,
count(distinct ip) as ip_total,
'-1' as area,
'-1' as origin_channel,
'-1' as hourinfo,
yearinfo as time_str,
'3' as grouptype,
'5' as time_type,
yearinfo,
'-1' as quarterinfo,
'-1' as monthinfo,
'-1' as dayinfo
from itcast_dwd.visit_consult_dwd where msg_count >0 group by yearinfo;


-- 按天统计各个来源渠道的咨询量
insert into table itcast_dws.consult_dws partition(yearinfo,quarterinfo,monthinfo,dayinfo)
select  
count(distinct sid) as sid_total,
count(distinct session_id) as sessionid_total,
count(distinct ip) as ip_total,
'-1' as area,
origin_channel,
'-1' as hourinfo,
concat(yearinfo,'-',monthinfo,'-',dayinfo) as time_str,
'2' as grouptype,
'2' as time_type,
yearinfo,
quarterinfo,
monthinfo,
dayinfo
from itcast_dwd.visit_consult_dwd  where msg_count >0 group by yearinfo,quarterinfo,monthinfo,dayinfo,origin_channel;




