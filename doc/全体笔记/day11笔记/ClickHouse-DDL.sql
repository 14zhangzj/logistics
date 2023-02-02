

/* ================================================================= */
-- ReplacingMergeTree
CREATE TABLE tbl_test_replacingmergetree_users
(
    `id` UInt64, 
    `email` String, 
    `username` String, 
    `gender` UInt8, 
    `birthday` Date, 
    `mobile` FixedString(13), 
    `pwd` String, 
    `regDT` DateTime, 
    `lastLoginDT` DateTime, 
    `lastLoginIP` String
)
ENGINE = ReplacingMergeTree(id)
PARTITION BY toYYYYMMDD(regDT)
ORDER BY id
SETTINGS index_granularity = 8192 ;

insert into tbl_test_replacingmergetree_users select * from tbl_test_mergetree_users where id<=5;


select * from tbl_test_replacingmergetree_users ;

insert into tbl_test_replacingmergetree_users(id,email,username,gender,birthday,mobile,pwd,regDT,lastLoginIP,lastLoginDT) select id,email,username,gender,birthday,mobile,pwd,regDT,lastLoginIP,now() as lastLoginDT from tbl_test_mergetree_users where id<=3;


/* ================================================================= */
-- SummingMergeTree
CREATE TABLE tbl_test_summingmergetree
(
    `key` UInt64, 
    `value` UInt64
)
ENGINE = SummingMergeTree()
ORDER BY key ;

insert into tbl_test_summingmergetree(key,value) values(1,13);


/* ================================================================= */
CREATE TABLE tbl_test_mergetree_logs
(
    `guid` String, 
    `url` String, 
    `refUrl` String, 
    `cnt` UInt16, 
    `cdt` DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(cdt)
ORDER BY toYYYYMMDD(cdt) ;

insert into tbl_test_mergetree_logs(guid,url,refUrl,cnt,cdt) values('a','www.itheima.com','www.itcast.cn',1,'2019-12-17 12:12:12'),('a','www.itheima.com','www.itcast.cn',1,'2019-12-17 12:14:45'),('b','www.itheima.com','www.itcast.cn',1,'2019-12-17 13:13:13');


CREATE TABLE tbl_test_aggregationmergetree_visitor
(
    `guid` String, 
    `cnt` AggregateFunction(count, UInt16), 
    `cdt` Date
)
ENGINE = AggregatingMergeTree()
PARTITION BY cdt
ORDER BY cnt ;


insert into tbl_test_aggregationmergetree_visitor select guid,countState(cnt),toDate(cdt) from tbl_test_mergetree_logs group by guid,cnt,cdt;



/* ================================================================= */


CREATE TABLE tbl_test_collapsingmergetree_day_mall_sale
(
    `mallId` UInt64, 
    `mallName` String, 
    `totalAmount` Decimal(32, 2), 
    `cdt` Date, 
    `sign` Int8
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMMDD(cdt)
ORDER BY mallId ;


insert into tbl_test_collapsingmergetree_day_mall_sale(mallId,mallName,totalAmount,cdt,sign) values(1,'西单大悦城',17649135.64,'2019-12-24',1);
insert into tbl_test_collapsingmergetree_day_mall_sale(mallId,mallName,totalAmount,cdt,sign) values(2,'朝阳大悦城',16341742.99,'2019-12-24',1);


insert into tbl_test_collapsingmergetree_day_mall_sale(mallId,mallName,totalAmount,cdt,sign) values(1,'西单大悦城',17649135.64,'2019-12-24',-1);
insert into tbl_test_collapsingmergetree_day_mall_sale(mallId,mallName,totalAmount,cdt,sign) values(2,'朝阳大悦城',16341742.99,'2019-12-24',-1);


insert into tbl_test_collapsingmergetree_day_mall_sale(mallId,mallName,totalAmount,cdt,sign) values(1,'西单大悦城',17649135.64,'2019-12-24',1);
insert into tbl_test_collapsingmergetree_day_mall_sale(mallId,mallName,totalAmount,cdt,sign) values(2,'朝阳大悦城',16341742.99,'2019-12-24',1);
insert into tbl_test_collapsingmergetree_day_mall_sale(mallId,mallName,totalAmount,cdt,sign) values(1,'西单大悦城',17649135.64,'2019-12-24',-1);

/* ================================================================= */



CREATE TABLE UAct
(
UserID UInt64,
PageViews UInt8,
Duration UInt8,
Sign Int8,
Version UInt8
)
ENGINE = VersionedCollapsingMergeTree(Sign, Version) 
ORDER BY UserID ;

INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1, 1) ;

INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1, 1),(4324182021466249494, 6, 185, 1, 2) ;










