
/*
	[root@node2 ~]# systemctl start clickhouse-server 
	[root@node2 ~]# 
	[root@node2 ~]# ps -ef|grep clickhouse 
	clickho+  3323     1 31 14:48 ?        00:00:01 /usr/bin/clickhouse-server --config=/etc/clickhouse-server/config.xml --pid-file=/run/clickhouse-server/clickhouse-server.pid
	root      3362  1969  2 14:48 pts/0    00:00:00 grep --color=auto clickhouse
	[root@node2 ~]# 
	[root@node2 ~]# 
	[root@node2 ~]# clickhouse-client -m --host node2.itcast.cn --port 9999 --user root --password 123456
	ClickHouse client version 20.4.2.9 (official build).
	Connecting to node2.itcast.cn:9999 as user root.
	Connected to ClickHouse server version 20.4.2 revision 54434.
*/

-- ARRAY JOIN
CREATE TABLE tbl_test_array_join
(
    `str` String, 
    `arr` Array(Int8)
)
ENGINE = Memory ;

insert into tbl_test_array_join(str,arr) values('a',[1,3,5]),('b',[2,4,6]);

SELECT 
    str, 
    arr, 
    arrItem
FROM tbl_test_array_join
ARRAY JOIN arr AS arrItem


/* ==============================================================*/

-- ALTER 语法
CREATE TABLE mt_table
(
    `date` Date, 
    `id` UInt8, 
    `name` String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(date)
ORDER BY id
SETTINGS index_granularity = 8192 ;

insert into mt_table values ('2020-09-15', 1, 'zhangsan');
insert into mt_table values ('2020-09-15', 2, 'lisi');
insert into mt_table values ('2020-09-15', 3, 'wangwu');

-- 添加列
alter table mt_table add column age UInt8 ;
-- 查看表结构
desc mt_table ;
-- 查看数据
select * from mt_table ;


-- 修改列
alter table mt_table modify column age UInt16 ;

-- 删除列
alter table mt_table drop column age ; 



/* ==============================================================*/
-- upate和delete，基于alter语法实现

CREATE TABLE tbl_test_users
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
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(regDT)
ORDER BY id
SETTINGS index_granularity = 8192 ;



insert into tbl_test_users(id, email, username, gender, birthday, mobile, pwd, regDT, lastLoginDT,lastLoginIP) values (1,'wcfr817e@yeah.net','督咏',2,'1992-05-31','13306834911','7f930f90eb6604e837db06908cc95149','2008-08-06 11:48:12','2015-05-08 10:51:41','106.83.54.165'),(2,'xuwcbev9y@ask.com','上磊',1,'1983-10-11','15302753472','7f930f90eb6604e837db06908cc95149','2008-08-10 05:37:32','2014-07-28 23:43:04','121.77.119.233'),(3,'mgaqfew@126.com','涂康',1,'1970-11-22','15200570030','96802a851b4a7295fb09122b9aa79c18','2008-08-10 11:37:55','2014-07-22 23:45:47','171.12.206.122'),(4,'b7zthcdg@163.net','金俊振',1,'2002-02-10','15207308903','96802a851b4a7295fb09122b9aa79c18','2008-08-10 14:47:09','2013-12-26 15:55:02','61.235.143.92'),(5,'ezrvy0p@163.net','阴福',1,'1987-09-01','13005861359','96802a851b4a7295fb09122b9aa79c18','2008-08-12 21:58:11','2013-12-26 15:52:33','182.81.200.32');


ALTER TABLE tbl_test_users UPDATE username='张三' WHERE id=1;


ALTER TABLE tbl_test_users DELETE WHERE id=1;



/* ==============================================================*/

-- 枚举类型
CREATE TABLE tbl_test_enum
(
    `e1` Enum8('male' = 1, 'female' = 2), 
    `e2` Enum16('hello' = 1, 'word' = 2), 
    `e3` Nullable(Enum8('A' = 1, 'B' = 2)), 
    `e4` Nullable(Enum16('a' = 1, 'b' = 2))
)
ENGINE = TinyLog ;


insert into tbl_test_enum values('male', 'hello', 'A', null),('male', 'word', null, 'a');

insert into tbl_test_enum values(2, 1, 'A', null);


/* ==============================================================*/

-- Nullable 类型
CREATE TABLE tbl_test_nullable
(
    `f1` String, 
    `f2` Int8, 
    `f3` Nullable(Int8)
)
ENGINE = TinyLog ;

insert into tbl_test_nullable(f1,f2,f3) values('NoNull',1,1);

insert into tbl_test_nullable(f1,f2,f3) values(null,2,2);
insert into tbl_test_nullable(f1,f2,f3) values('NoNull2',null,2);

/* ==============================================================*/

CREATE TABLE tbl_test_domain
(
    `url` String, 
    `ip4` IPv4, 
    `ip6` IPv6
)
ENGINE = MergeTree()
ORDER BY url ;



/* ==============================================================*/
CREATE TABLE aggMT
(
    `whatever` Date DEFAULT '2019-12-18', 
    `key` String, 
    `value` String, 
    `first` AggregateFunction(min, DateTime), 
    `last` AggregateFunction(max, DateTime), 
    `total` AggregateFunction(count, UInt64)
)
ENGINE = AggregatingMergeTree(whatever, (key, value), 8192) ;


insert into aggMT (key,value,first,last,total) select 'test','1.2.3.4',minState(toDateTime(1576654217)),maxState(toDateTime(1576654217)),countState(cast(1 as UInt64));
insert into aggMT (key,value,first,last,total) select 'test','1.2.3.5',minState(toDateTime(1576654261)),maxState(toDateTime(1576654261)),countState(cast(1 as UInt64));
insert into aggMT (key,value,first,last,total) select 'test','1.2.3.6',minState(toDateTime(1576654273)),maxState(toDateTime(1576654273)),countState(cast(1 as UInt64));


/* ==============================================================*/

CREATE TABLE tbl_test_mergetree_users
(
    `id` UInt64, 
    `email` String, 
    `username` String, 
    `gender` UInt8, 
    `birthday` DATE, 
    `mobile` FixedString(13), 
    `pwd` String, 
    `regDT` DateTime, 
    `lastLoginDT` DateTime, 
    `lastLoginIP` String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(regDT)
ORDER BY id
SETTINGS index_granularity = 8192 ;

insert into tbl_test_mergetree_users(id, email, username, gender, birthday, mobile, pwd, regDT, lastLoginDT,lastLoginIP)values (1,'wcfr817e@yeah.net','督咏',2,'1992-05-31','13306834911','7f930f90eb6604e837db06908cc95149','2008-08-06 11:48:12','2015-05-08 10:51:41','106.83.54.165'),(2,'xuwcbev9y@ask.com','上磊',1,'1983-10-11','15302753472','7f930f90eb6604e837db06908cc95149','2008-08-10 05:37:32','2014-07-28 23:43:04','121.77.119.233'),(3,'mgaqfew@126.com','涂康',1,'1970-11-22','15200570030','96802a851b4a7295fb09122b9aa79c18','2008-08-10 11:37:55','2014-07-22 23:45:47','171.12.206.122'),(4,'b7zthcdg@163.net','金俊振',1,'2002-02-10','15207308903','96802a851b4a7295fb09122b9aa79c18','2008-08-10 14:47:09','2013-12-26 15:55:02','61.235.143.92'),(5,'ezrvy0p@163.net','阴福',1,'1987-09-01','13005861359','96802a851b4a7295fb09122b9aa79c18','2008-08-12 21:58:11','2013-12-26 15:52:33','182.81.200.32'),(6,'juestiua@263.net','岑山裕',1,'1996-02-12','13008315314','96802a851b4a7295fb09122b9aa79c18','2008-08-14 05:48:16','2013-12-26 15:49:12','36.59.3.248'),(7,'oyyrzd@yahoo.com.cn','姚茗咏',2,'1977-02-06','13303203846','96e79218965eb72c92a549dd5a330112','2008-08-15 10:07:31','2013-12-26 15:55:05','106.91.23.177'),(8,'lhrwkwwf@163.com','巫红影',2,'1996-02-21','15107523748','96802a851b4a7295fb09122b9aa79c18','2008-08-15 14:37:41','2013-12-26 15:55:05','123.234.85.27'),(9,'v2zqak8kh@0355.net','空进',1,'1974-01-16','13903178080','96802a851b4a7295fb09122b9aa79c18','2008-08-16 03:24:44','2013-12-26 15:52:42','121.77.192.123'),(10,'mqqqmf@yahoo.com','西香',2,'1980-10-13','13108330898','96802a851b4a7295fb09122b9aa79c18','2008-08-16 04:42:08','2013-12-26 15:55:08','36.57.21.234'),(11,'sf8oubu@yahoo.com.cn','壤晶媛',2,'1976-03-05','15202615557','96802a851b4a7295fb09122b9aa79c18','2008-08-16 06:08:51','2013-12-26 15:55:03','182.83.220.201'),(12,'k6k21ce@qq.com','东平',1,'2005-04-25','13603648382','96802a851b4a7295fb09122b9aa79c18','2008-08-16 06:18:20','2013-12-26 15:55:05','210.34.111.155'),(13,'zguxgg@qq.com','夹影悦',2,'2002-08-23','15300056290','96802a851b4a7295fb09122b9aa79c18','2008-08-16 06:57:45','2013-12-26 15:55:02','61.232.211.180'),(14,'g2jqhbzrf@aol.com','生晓怡',2,'1974-06-22','13507515420','96802a851b4a7295fb09122b9aa79c18','2008-08-16 08:23:43','2013-12-26 15:55:02','182.86.5.162'),(15,'1evn3spn@126.com','咎梦',2,'1998-04-14','15204567060','060ed80051e6384b77ddfaa26191778b','2008-08-16 08:29:57','2013-12-26 15:55:02','210.30.171.70'),(16,'tqndz6l@googlemail.com','司韵',2,'1992-08-28','15202706709','96802a851b4a7295fb09122b9aa79c18','2008-08-16 14:34:25','2013-12-26 15:55:03','171.10.115.59'),(17,'3472gs22x@live.com','李言',1,'1997-09-08','15701526600','96802a851b4a7295fb09122b9aa79c18','2008-08-16 15:04:07','2013-12-26 15:52:39','171.14.80.71'),(18,'p385ii@gmail.com','詹芸',2,'2004-11-05','15001974846','96802a851b4a7295fb09122b9aa79c18','2008-08-16 15:26:06','2013-12-26 15:55:02','182.89.147.245'),(19,'mfbncfu@yahoo.com','蒙芬霞',2,'1990-09-10','15505788156','96802a851b4a7295fb09122b9aa79c18','2008-08-16 15:30:58','2013-12-26 15:55:05','182.91.15.89'),(20,'l24ffbn@ask.com','后冠',1,'2000-09-02','15608241150','96802a851b4a7295fb09122b9aa79c18','2008-08-17 01:15:55','2014-08-29 00:51:12','36.58.7.85'),(21,'m26lggpe@qq.com','宋美月',2,'2003-01-13','15606561425','96802a851b4a7295fb09122b9aa79c18','2008-08-17 01:24:09','2013-12-26 15:52:36','123.235.233.160'),(22,'ndmfm13qf@0355.net','邬玲',2,'2002-08-11','13207844859','96802a851b4a7295fb09122b9aa79c18','2008-08-17 03:31:11','2013-12-26 15:55:03','36.60.8.4'),(23,'5shmvnd@sina.com','乐心有',1,'1998-05-01','13201212693','96802a851b4a7295fb09122b9aa79c18','2008-08-17 03:33:41','2013-12-26 15:55:02','123.234.184.210'),(24,'pwa0hu@3721.net','任学诚',1,'1978-03-19','15802040152','7f930f90eb6604e837db06908cc95149','2008-08-17 07:24:01','2013-12-26 15:52:34','210.43.167.14'),(25,'1ybjhul@googlemail.com','巫纨',2,'1995-01-20','15900530105','96802a851b4a7295fb09122b9aa79c18','2008-08-17 07:48:06','2013-12-26 15:55:02','222.55.139.104'),(26,'b31me2i8b@yeah.net','石娅',2,'2000-02-25','13908735198','96802a851b4a7295fb09122b9aa79c18','2008-08-17 08:17:24','2013-12-26 15:52:45','123.235.99.123'),(27,'qgb2w4n7@163.net','柏菁',2,'1975-02-09','15306552661','96802a851b4a7295fb09122b9aa79c18','2008-08-17 08:47:39','2013-12-26 15:55:02','121.77.245.202'),(28,'cfb3ck@sohu.com','鲜梦',2,'1974-01-26','13801751668','96802a851b4a7295fb09122b9aa79c18','2008-08-17 08:55:47','2013-12-26 15:55:02','210.26.163.24'),(29,'nfrf6mp@msn.com','鄂珍',2,'1974-04-14','13300247433','96802a851b4a7295fb09122b9aa79c18','2008-08-17 09:02:14','2013-12-26 15:55:08','210.31.214.157'),(30,'o1isumh@126.com',' 法姬',2,'1978-06-16','15607848127','96802a851b4a7295fb09122b9aa79c18','2008-08-17 09:09:59','2013-12-26 15:55:08','222.24.34.19'),(31,'y2wrclkq@msn.com','太以',1,'1998-09-07','13608585923','96802a851b4a7295fb09122b9aa79c18','2008-08-17 11:35:39','2013-12-26 15:52:35','182.89.218.177'),(32,'fv9avnuo@263.net','庚姣欣',2,'1982-09-14','13004625187','96802a851b4a7295fb09122b9aa79c18','2008-08-17 12:50:36','2013-12-26 15:55:02','106.82.225.130'),(33,'o1e96z@yahoo.com','微伟',1,'1981-07-30','13707663880','96802a851b4a7295fb09122b9aa79c18','2008-08-17 15:12:05','2013-12-26 15:49:12','171.13.152.247'),(34,'cm3oz64ja@msn.com','那竹娜',2,'1989-01-09','13607294767','96802a851b4a7295fb09122b9aa79c18','2008-08-17 15:51:08','2013-12-26 15:55:02','171.13.110.67'),(35,'g7impl@msn.com','闾和栋',1,'1994-10-12','13907368366','96802a851b4a7295fb09122b9aa79c18','2008-08-17 16:51:02','2013-12-26 15:55:01','210.28.163.83'),(36,'jz2fjtt@163.com','夏佳悦',2,'2001-03-17','15102554998','7af1b63f0d1f37c35c1274339c12b6a8','1970-01-01 08:00:00','1970-01-01 08:00:00','222.91.138.221'),(37,'klwrtomws@yahoo.com','南义梁',1,'1981-03-19','15105745902','96802a851b4a7295fb09122b9aa79c18','2008-08-18 01:49:17','2013-12-26 15:55:01','36.62.155.17'),(38,'yhzs1nnlk@3721.net','牧元',1,'2001-06-07','13501780163','96802a851b4a7295fb09122b9aa79c18','2008-08-18 04:24:52','2013-12-26 15:55:01','171.15.184.130'),(39,'hem76ot33@gmail.com','凌伟文',1,'1988-03-04','13201417535','96802a851b4a7295fb09122b9aa79c18','2008-08-18 05:34:52','2013-12-26 15:55:14','61.237.105.3'),(40,'ndp40j@sohu.com','弘枝',2,'2000-09-05','13001236425','96802a851b4a7295fb09122b9aa79c18','2008-08-18 06:23:48','2013-12-26 15:55:01','106.82.172.45'),(41,'zeyacpr@gmail.com','台凡',2,'1998-05-26','15102913418','96802a851b4a7295fb09122b9aa79c18','2008-08-18 06:41:24','2013-12-26 15:55:07','123.233.69.218'),(42,'0ts0wiz@aol.com','任晓红',2,'1984-05-02','13502366778','96802a851b4a7295fb09122b9aa79c18','2008-08-18 06:55:16','2013-12-26 15:55:01','210.26.44.18'),(43,'zi7dhzo@googlemail.com','蔡艺艳',2,'1990-08-07','15603954810','96802a851b4a7295fb09122b9aa79c18','2008-08-18 06:57:30','2013-12-26 15:55:01','171.12.171.179'),(44,'b0yfzilu@hotmail.com','郎诚',1,'1994-05-18','13602127171','96802a851b4a7295fb09122b9aa79c18','2008-08-18 07:02:04','2013-12-26 15:55:02','171.8.22.92'),(45,'er9az5e9s@163.com','台翰',1,'1994-06-22','15900953220','96802a851b4a7295fb09122b9aa79c18','2008-08-18 07:05:08','2013-12-26 15:55:14','222.31.141.156'),(46,'e34jy2@yeah.net','彭筠',2,'1983-08-12','15106915420','96802a851b4a7295fb09122b9aa79c18','2008-08-18 07:09:37','2013-12-26 15:52:34','36.60.51.67'),(47,'1u0zc56h@163.net','包华婉',2,'1998-10-03','13102518450','96802a851b4a7295fb09122b9aa79c18','2008-08-18 07:47:24','2013-12-26 15:55:02','121.76.76.105'),(48,'cs8kyk3@ask.com','淳盛',1,'2002-06-19','13203151569','96802a851b4a7295fb09122b9aa79c18','2008-08-18 08:01:58','2013-12-26 15:55:02','36.60.76.111'),(49,'ibcgi5ll@yahoo.com','车珍枫',2,'1975-07-27','15605361319','96802a851b4a7295fb09122b9aa79c18','2008-08-18 08:12:45','2013-12-26 15:55:01','106.83.110.76'),(50,'gzxcx6vz@live.com','应冰红',2,'2004-04-19','15104154370','96802a851b4a7295fb09122b9aa79c18','2008-08-18 09:00:09','2013-12-26 15:55:01','182.88.181.216');



select * from tbl_test_mergetree_users where email like '%gmail%' and id >= 30 and toYYYYMMDD(regDT) = 20080818 ;






