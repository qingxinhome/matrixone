-- @skip:issue#16438
--env prepare statement
drop table if exists pt_table_1;
drop table if exists pt_table_2;
drop table if exists pt_table_3;
drop table if exists pt_table_5;
drop table if exists pt_table_6;
drop table if exists pt_table_21;
drop table if exists pt_table_22;
drop table if exists pt_table_23;
drop table if exists pt_table_24;
drop table if exists pt_table_31;
drop table if exists pt_table_32;
drop table if exists pt_table_33;
drop table if exists pt_table_34;
drop table if exists pt_table_35;
drop table if exists pt_table_36;
drop table if exists pt_table_37;
drop table if exists pt_table_41;
drop table if exists pt_table_42;
drop table if exists pt_table_43;
drop table if exists pt_table_44;
drop table if exists pt_table_45;

--hash partiton列为tinyint，tinyint unsigned,主键列
create table pt_table_1(col1 tinyint,col2 smallint,col3 int,clo4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text,primary key(col1))partition by hash(col1)partitions 4;
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table_1 fields terminated by ',';
select col1 from pt_table_1;

create table pt_table_2(col1 tinyint,col2 smallint,col3 int,clo4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text,primary key(col5))partition by hash(col5);
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table_2 fields terminated by ',';
select col5 from pt_table_2;

create table pt_table_3(col1 tinyint not null,col2 smallint,col3 int,clo4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 char(255) default 'style nine',primary key(col1,col20))partition by hash(col1)partitions 4;
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table_3 fields terminated by ',';
select col1 from pt_table_3;

--hash partiton列为date列表达式
create table pt_table_5(col1 tinyint,col2 smallint,col3 int,clo4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 char(255))partition by hash(year(col12));
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table_5 fields terminated by ',';
select col12 from pt_table_5;
show create table pt_table_5;

--关键字LINEAR hash
create table pt_table_6(col1 tinyint,col2 smallint,col3 int,clo4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text)partition by LINEAR hash(col2)partitions 10;
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table_6 fields terminated by ',';
select col2 from pt_table_6;

--异常：partition列不是主键列，char，float，date，语法错误
create table pt_table_10(col1 tinyint,col2 smallint,col3 int,primary key(col1))partition by hash(col2);
create table pt_table_11(col1 tinyint,col2 smallint,col3 int,clo4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text)partition by hash(col9) partitions 6;
create table pt_table_12(col1 tinyint,col2 smallint,col3 int,clo4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 char(255))partition by hash(col20);
create table pt_table_13(col1 tinyint,col2 smallint,col3 int,clo4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 char(255))partition by hash(col12);
create table pt_table_13(col1 tinyint,col2 smallint,col3 int,clo4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 char(255))partition by (col12);

--key parttiton列为smallint，smallint unsigned,主键列
create table pt_table_21(col1 tinyint,col2 smallint,col3 int,clo4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text,primary key(col2))partition by key(col2)partitions 4;
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table_21 fields terminated by ',';
select col2 from pt_table_21;

create table pt_table_22(col1 tinyint,col2 smallint,col3 int,clo4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text,primary key(col6,col18))partition by key(col6,col18)partitions 4;
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table_22 fields terminated by ',';
select col2 from pt_table_22;

--key parttiton列为varchar列
create table pt_table_23(col1 tinyint,col2 smallint,col3 int,clo4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text,primary key(col19))partition by key(col19)partitions 4;
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table_23 fields terminated by ',';
select col19 from pt_table_23;

--key partiton列为datetime列
create table pt_table_24(col1 tinyint,col2 smallint,col3 int,clo4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text)partition by key(col13)partitions 10;
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table_24 fields terminated by ',';
select col13 from pt_table_24;

--range partiton列为int， int unsigned,主键列
create table pt_table_31(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text,primary key(col3))partition by range(col3)(PARTITION p0 VALUES LESS THAN (100),PARTITION p1 VALUES LESS THAN (2000),PARTITION p2 VALUES LESS THAN (4000),PARTITION p3 VALUES LESS THAN (6000),PARTITION p5 VALUES LESS THAN MAXVALUE);
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table_31 fields terminated by ',';
select col2 from pt_table_31;

create table pt_table_32(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text)partition by range(col7)(PARTITION p0 VALUES LESS THAN (100),PARTITION p1 VALUES LESS THAN (2000),PARTITION p2 VALUES LESS THAN (4000),PARTITION p3 VALUES LESS THAN (6000),PARTITION p5 VALUES LESS THAN MAXVALUE);
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table_32 fields terminated by ',';
select col2 from pt_table_32;

--range partiton列是复合主键子集
create table pt_table_33(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 char(255),primary key(col3,col7))partition by range(col7)(PARTITION p0 VALUES LESS THAN (100),PARTITION p1 VALUES LESS THAN (2000),PARTITION p2 VALUES LESS THAN (4000),PARTITION p3 VALUES LESS THAN (6000),PARTITION p5 VALUES LESS THAN MAXVALUE);
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table_33 fields terminated by ',';
select col2 from pt_table_33;

--range(表达式)
create table pt_table_34(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text)partition by range(year(col14))(PARTITION p0 VALUES LESS THAN (1991) comment ='expression range',PARTITION p1 VALUES LESS THAN (2000),PARTITION p2 VALUES LESS THAN (2009)comment ='range',PARTITION p3 VALUES LESS THAN (2010),PARTITION p5 VALUES LESS THAN MAXVALUE);

--range column list
create table pt_table_35(col1 int not null,col2 smallint,col3 int not null,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text,primary key(col3,col1))partition by range columns(col1,col3)(PARTITION p0 VALUES LESS THAN (100,300),PARTITION p1 VALUES LESS THAN (300,500),PARTITION p2 VALUES LESS THAN (500,MAXVALUE),PARTITION p3 VALUES LESS THAN (6000,MAXVALUE),PARTITION p4 VALUES LESS THAN (MAXVALUE,MAXVALUE));
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table_35 fields terminated by ',';
select col14 from pt_table_35;

--less than表达式
create table pt_table_36(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text,primary key(col3))partition by range(col3)(PARTITION p0 VALUES LESS THAN (100+50),PARTITION p1 VALUES LESS THAN (2000+100),PARTITION p2 VALUES LESS THAN (4000),PARTITION p3 VALUES LESS THAN (6000),PARTITION p5 VALUES LESS THAN MAXVALUE);
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table_36 fields terminated by ',';
select col2 from pt_table_36;

--异常：range列不在schema里
create table pt_table_37(col1 tinyint,col2 smallint,col3 int,col4 bigint)partition by range(col90)(PARTITION p0 VALUES LESS THAN (100),PARTITION p1 VALUES LESS THAN (2000),PARTITION p2 VALUES LESS THAN (4000),PARTITION p3 VALUES LESS THAN (6000),PARTITION p5 VALUES LESS THAN MAXVALUE);
--异常varhcar，timestamp,float及表达式
create table pt_table_37(col1 tinyint,col11 varchar(255),col12 Date)partition by range(col11)(PARTITION p0 VALUES LESS THAN (100),PARTITION p1 VALUES LESS THAN (2000),PARTITION p2 VALUES LESS THAN (4000),PARTITION p3 VALUES LESS THAN (6000),PARTITION p5 VALUES LESS THAN MAXVALUE);
create table pt_table_37(col1 tinyint,col11 varchar(255),col12 timestamp)partition by range(col12)(PARTITION p0 VALUES LESS THAN (100),PARTITION p1 VALUES LESS THAN (2000),PARTITION p2 VALUES LESS THAN (4000),PARTITION p3 VALUES LESS THAN (6000),PARTITION p5 VALUES LESS THAN MAXVALUE);
create table pt_table_37(col1 tinyint,col11 float,col12 timestamp)partition by range(col11)(PARTITION p0 VALUES LESS THAN (1991),PARTITION p1 VALUES LESS THAN (2000),PARTITION p2 VALUES LESS THAN (2009),PARTITION p3 VALUES LESS THAN (2010),PARTITION p5 VALUES LESS THAN MAXVALUE);
create table pt_table_37(col1 tinyint,col11 float,col12 timestamp)partition by range(ceil(col11))(PARTITION p0 VALUES LESS THAN (100),PARTITION p1 VALUES LESS THAN (2000),PARTITION p2 VALUES LESS THAN (4000),PARTITION p3 VALUES LESS THAN (6000),PARTITION p5 VALUES LESS THAN MAXVALUE);
--异常：语法错误
create table pt_table_37(col1 tinyint,col11 float,col12 timestamp)partition by range(col1);
create table pt_table_37(col1 tinyint not null,col2 smallint,col3 int not null,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text,primary key(col3,col1))partition by range columns(col1,col3)(PARTITION p0 VALUES LESS THAN (100,300),PARTITION p1 VALUES LESS THAN (300,500),PARTITION p2 VALUES LESS THAN (500,MAXVALUE),PARTITION p3 VALUES LESS THAN (6000,MAXVALUE),PARTITION p4 VALUES LESS THAN (MAXVALUE,MAXVALUE));

--range partiton列为bigint，BIGINT UNSIGNED,主键列
create table pt_table_41(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text,primary key(col4)) partition by list(col4) (PARTITION r0 VALUES IN (-6041648745842399623, 2267877015687134490, 7769629822818484334),PARTITION r1 VALUES IN (1234138289513302348, -3038428195984464330, -1681456935776973509),PARTITION r2 VALUES IN (-484407619835391694, -5246968895134993792, -3237107390156157130),PARTITION r3 VALUES IN (-2998549470145089608, 6123486173032718578, 6123486173032718570));
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table_41 fields terminated by ',';
select col8 from pt_table_41 order by col8;

create table pt_table_42(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text) partition by list(col8) (PARTITION r0 VALUES IN (14999475422109240954, 6204822205090614210, 6625004793680807490),PARTITION r1 VALUES IN (17397115807377870895, 3143191107533743301, 13381191796017069332),PARTITION r2 VALUES IN (8740918055557791046, 4029688785176298663, 6625004793680807495),PARTITION r3 VALUES IN (16635491969502097586, 7094376021034692269, 18225693328091251880));
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table_42 fields terminated by ',';
select col8 from pt_table_42 order by col8;

--异常：partiton列为double
create table pt_table_43(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text)partition by list(col10) (PARTITION r0 VALUES IN (1, 5, 9, 13, 17, 21),PARTITION r1 VALUES IN (2, 6, 10, 14, 18, 22),PARTITION r2 VALUES IN (3, 7, 11, 15, 19, 23),PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24));

--list column partiton
create table pt_table_44(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text,primary key(col3,col4)) partition by list columns(col3,col4) (PARTITION p0 VALUES IN( (-1889972806, 7769629822818484334), (NULL,NULL) ),PARTITION p1 VALUES IN( (-1030254547, -5246968895134993792),(-1006909301, -6041648745842399623),( -232972021, -3237107390156157130)) comment='list column comment' ,PARTITION p2 VALUES IN( (-179559641, 1234138289513302348),(330484802, -2998549470145089608),(476482983, -484407619835391694) ),PARTITION p3 VALUES IN( (837702822, 6123486173032718578),(1124555433, -1681456935776973509),(1287532466, -3038428195984464330),(1449911253, 2267877015687134490)));
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table_44 fields terminated by ',';
select col3,col4 from pt_table_44 order by col3,col4;

--list(exp)partition
create table pt_table_45(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text) partition by list(year(col13))(PARTITION r0 VALUES IN (5732, 9976, 3647, 6216),PARTITION r1 VALUES IN (7031, 6868, 4844, 6438),PARTITION r2 VALUES IN (3114, 1014, 4023, 2008));
load data infile '$resources/external_table_file/pt_table_data.csv' into table  pt_table_45 fields terminated by ',';
select col3,col4 from pt_table_45 order by col3,col4;
show create table pt_table_45;

--异常text，datetime，decimal，bool
create table pt_table_46(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text)partition by list(col20) (PARTITION r0 VALUES IN (1, 5, 9, 13, 17, 21),PARTITION r1 VALUES IN (2, 6, 10, 14, 18, 22),PARTITION r2 VALUES IN (3, 7, 11, 15, 19, 23),PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24));
create table pt_table_47(col13 DateTime,col14 timestamp,col15 bool,partition by list(col13) (PARTITION r0 VALUES IN (1, 5, 9, 13, 17, 21),PARTITION r1 VALUES IN (2, 6, 10, 14, 18, 22),PARTITION r2 VALUES IN (3, 7, 11, 15, 19, 23),PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24));
create table pt_table_48(col1 tinyint,col2 smallint,col10 decimal)partition by list(col10) (PARTITION r0 VALUES IN (1, 5, 9, 13, 17, 21),PARTITION r1 VALUES IN (2, 6, 10, 14, 18, 22));
create table pt_table_49(col1 tinyint,col2 smallint,col15 bool)partition by list(col15) (PARTITION r0 VALUES IN (1, 5, 9, 13, 17, 21),PARTITION r1 VALUES IN (2, 6, 10, 14, 18, 22));
create table pt_table_50(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text,primary key(col4,col3,col11))partition by list(col3) (PARTITION r0 VALUES IN (1, 5*2, 9, 13, 17-20, 21),PARTITION r1 VALUES IN (2, 6, 10, 14/2, 18, 22),PARTITION r2 VALUES IN (3, 7, 11+6, 15, 19, 23),PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24));
--异常：重复值
create table pt_table_51(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text)partition by list(year(col13))(PARTITION r0 VALUES IN (1999, 2001, 2003),PARTITION r1 VALUES IN (1999, 2001, 2003),PARTITION r2 VALUES IN (1999, 2001, 2003));
--异常：partition列不是主键子集
create table pt_table_52(col1 tinyint,col2 smallint,col3 int,col4 bigint,col11 varchar(255),col12 Date,col13 DateTime,primary key(col4,col3,col11))partition by list(col2) (PARTITION r0 VALUES IN (1, 5, 9, 13, 17, 21),PARTITION r1 VALUES IN (2, 6, 10, 14, 18, 22),PARTITION r2 VALUES IN (3, 7, 11, 15, 19, 23),PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24));
create table pt_table_53(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text,primary key(col4,col3,col11))partition by list(col3) (PARTITION r0 VALUES IN (1, 5*2, 9, 13, 17-20, 21),PARTITION r1 VALUES IN (2, 6, 10, 14*2, 18, 22),PARTITION r2 VALUES IN (3, 7, 11+6, 15, 19, 23),PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24));
create table pt_table_54(col1 tinyint,col2 smallint,col3 int,col4 bigint,col5 tinyint unsigned,col6 smallint unsigned,col7 int unsigned,col8 bigint unsigned,col9 float,col10 double,col11 varchar(255),col12 Date,col13 DateTime,col14 timestamp,col15 bool,col16 decimal(5,2),col17 text,col18 varchar(255),col19 varchar(255),col20 text,primary key(col4,col3,col11))partition by list(col3) (PARTITION r0 VALUES IN (1, 5*2, 9, 13, 17-20, 21),PARTITION r1 VALUES IN (2, 6, 11, 14*2, 18, 22),PARTITION r2 VALUES IN (3, 7, 11+6, 15, 19, 23),PARTITION r3 VALUES IN (4, 8, 12, 16, 20, 24));

--异常：兼容mysql,外键不支持与分区结合使用
create table dept(deptno int unsigned auto_increment, dname varchar(15), loc varchar(50), primary key(deptno));
create table emp(empno int unsigned auto_increment, ename varchar(15), job varchar(10), mgr int unsigned, hiredate date, sal decimal(7,2), comm decimal(7,2), deptno int unsigned, primary key(empno), foreign key (deptno) references dept(deptno)) partition by key(empno) partitions 2;

--该测试用例在mysql中是合法，在mo中不合法，原因是ceiling函数造成的，如下：
--在MySQL中，HASH分区要求分区键必须是INT类型，或者通过表达式返回INT类型。
--在matrixone中，当ceil函数的参数为decimal类型，返回值为decimal类型，不能作为分区表达式类型
--但是在mysql中，当ceil函数的参数为decimal类型，返回值为int类型，可以作为分区表达式类型
create table p_hash_table_test(col1 tinyint,col2 varchar(30),col3 decimal(6,3))partition by hash(ceil(col3)) partitions 2;