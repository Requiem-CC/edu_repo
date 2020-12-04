-- 此脚本是用于放置 从 业务数据库--> ODS层操作记录
--访问咨询主题看板: 
-- 抽取 SQL:
SELECT   
  id,create_date_time,session_id,sid,create_time,
  seo_source,seo_keywords,ip,`area`,country,province,
  city,origin_channel,`user` AS user_match,manual_time,
  begin_time,end_time,last_customer_msg_time_stamp,
  last_agent_msg_time_stamp,reply_msg_count,msg_count,
  browser_name,os_info, '2020-11-28' AS starts_time
FROM  web_chat_ems_2019_07; 
-- 通过sqoop命令来执行上述SQL
sqoop import \
--connect jdbc:mysql://192.168.52.150:3306/nev \
--username root --password 123456 \
--query "SELECT   
  id,create_date_time,session_id,sid,create_time,
  seo_source,seo_keywords,ip,area,country,province,
  city,origin_channel,user AS user_match,manual_time,
  begin_time,end_time,last_customer_msg_time_stamp,
  last_agent_msg_time_stamp,reply_msg_count,msg_count,
  browser_name,os_info, '2020-11-28' AS starts_time
FROM  web_chat_ems_2019_07 where 1=1 and \$CONDITIONS" \
--fields-terminated-by '\t' \
--hcatalog-database itcast_ods \
--hcatalog-table web_chat_ems \
-m 3 \
--split-by id


————————2020-12-04开始往下进行————————


-- 抽取SQL(副表) :

SELECT  
id,referrer,from_url,landing_page_url,
url_title,platform_description,
other_params,history, '2020-11-28' AS start_time
FROM web_chat_text_ems_2019_07;

-- 通过sqoop命令来执行上述SQL
sqoop import \
--connect jdbc:mysql://192.168.52.150:3306/nev \
--username root --password 123456 \
--query "SELECT  
id,referrer,from_url,landing_page_url,
url_title,platform_description,
other_params,history, '2020-11-28' AS start_time
FROM web_chat_text_ems_2019_07 where 1=1 and \$CONDITIONS" \
--fields-terminated-by '\t' \
--hcatalog-database itcast_ods \
--hcatalog-table web_chat_text_ems \
-m 3 \
--split-by id

-- 注意在 hive中进行校验时 发现原有hiveserver2的服务的堆栈内存配置过低, 导致hiveserver2出现
-- 异常宕机问题, 对堆栈内存重新这只为 1GB , 重启服务





