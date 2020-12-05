-- 此脚本用于放置：数据导出到MySQL的脚本内容
-- 访问咨询主题看板: 
-- 1.访问量的结果表数据导出
-- 在MySQL中 构建一个 存储结果表的库 以及创建相关的结果表
create database scrm_bi default character set utf8mb4 collate utf8mb4_general_ci;

CREATE TABLE `itcast_visit` (
  sid_total int(11) COMMENT '根据sid去重求count',
  sessionid_total int(11) COMMENT '根据sessionid去重求count',
  ip_total int(11) COMMENT '根据IP去重求count',
  area varchar(32) COMMENT '区域信息',
  seo_source varchar(32) COMMENT '搜索来源',
  origin_channel varchar(32) COMMENT '来源渠道',
  hourinfo varchar(32) COMMENT '小时信息',
  time_str varchar(32) COMMENT '时间明细',
  from_url varchar(1000) comment '会话来源页面',
  groupType varchar(32) COMMENT '产品属性类型：1.地区；2.搜索来源；3.来源渠道；4.会话来源页面；5.总访问量',
  time_type varchar(32) COMMENT '时间聚合类型：1、按小时聚合；2、按天聚合；3、按月聚合；4、按季度聚合；5、按年聚合；',
  yearinfo varchar(32) COMMENT '年信息',
  quarterinfo varchar(32) COMMENT '季度',
  monthinfo varchar(32) COMMENT '月信息',
  dayinfo varchar(32) COMMENT '日信息'
);
-- 通过 sqoop 实现 将hive表数据 全量导出到 MySQL的目标表: 
sqoop export \
--connect "jdbc:mysql://192.168.52.150:3306/scrm_bi?useUnicode=true&characterEncoding=utf-8" \
--username root --password 123456 \
--table itcast_visit \
--hcatalog-database itcast_dws \
--hcatalog-table visit_dws \
-m 1


-- 2 在MySQL中 构建 咨询量的结果表
CREATE TABLE `itcast_consult` (
  sid_total int(11) COMMENT '去重并聚合sid',
  sessionid_total int(11) COMMENT '去重并聚合sessionid',
  ip_total int(11) COMMENT '去重并聚合ip',
  area varchar(32) COMMENT '区域信息',
  origin_channel varchar(32) COMMENT '来源渠道',
  hourinfo varchar(5) COMMENT '创建时间，统计至小时', 
  time_str varchar(32) COMMENT '时间明细',
  groupType varchar(5) COMMENT '产品属性类型：1.地区；2.来源渠道',
  time_type varchar(5) COMMENT '时间聚合类型：1、按小时聚合；2、按天聚合；3、按月聚合；4、按季度聚合；5、按年聚合；',
  yearinfo varchar(5) COMMENT '创建时间，统计至年',
  quarterinfo varchar(5) COMMENT '季度',
  monthinfo varchar(5) COMMENT '创建时间，统计至月',
  dayinfo varchar(5) COMMENT '创建时间，统计至天'
) COMMENT='客户咨询统计数据';

-- 执行 sqoop 数据导出
sqoop export \
--connect "jdbc:mysql://192.168.52.150:3306/scrm_bi?useUnicode=true&characterEncoding=utf-8" \
--username root --password 123456 \
--table itcast_consult \
--hcatalog-database itcast_dws \
--hcatalog-table consult_dws \
-m 1