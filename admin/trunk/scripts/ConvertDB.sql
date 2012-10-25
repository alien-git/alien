#Run this script with
# LD_LIBRARY_PATH=/opt/alien/lib:/opt/alien/lib/mysql/ /opt/alien/bin/mysql -h alientest03 -u admin -p... -P 3307 --skip-column-names < ConvertDB.sql > convert.sql  

select '  use alice_users_test;';

select 'select "DROPPING TABLES";';
select concat("drop table ", table_name, " ;") FROM information_schema.TABLES 
  WHERE table_name not like 'L%L%' AND 
        table_name not like 'G%L%' AND 
        table_name not like 'LL_STATS' AND 
        table_name not like 'LL_ACTIONS' AND 
        table_name not like 'GL_STATS' AND 
        table_name not like 'GL_ACTIONS' AND 
        table_name not like 'PFN_TODELETE' AND 
        table_name not like 'LFN_UPDATES' AND 
        table_name not like 'T%' AND 
        table_name not like 'PACKAGES' AND 
        table_name not like 'USERS' AND 
        table_name not like 'USERS_LDAP' AND 
        table_name not like 'USERS_LDAP_ROLE' AND 
        table_name not like 'GRPS' AND 
        table_name not like 'UGMAP' AND 
        table_name not like 'ACTIONS' AND 
        table_name not like 'INDEXTABLE' AND 
        table_name not like 'GUIDINDEX' AND 
        table_name not like 'LFN_BOOKED' AND 
        table_name not like 'TODELETE' AND 
        table_name not like 'SE' AND 
        table_name not like 'SEDistance' AND 
        table_name not like 'SE_VOLUMES' AND 
        table_name not like 'COLLECTIONS' AND 
        table_name not like 'COLLECTIONS_ELEM' AND 
        table_name not like 'FQUOTAS' 
        AND TABLE_SCHEMA='alice_users_test';
        
select '  drop table TODELETE_SAFE;';
select '  drop table TMPGUID;';
select '  drop table TODELETE2;';
select '  drop table TODELETE4;';
select '  drop table TOKENS;';
select '  drop table LFN_BOOKED;';
select '  drop table LFN_BOOKED_DELETED;';
select '  drop table LFN_BOOKED_innodb;';
select '  drop table GROUPS;';
select '  drop table CONSTANTS;';


select 'select "Now, moving the alice_data_test to alice_users_test";';
select '    alter table alice_data_test.LFN_BOOKED rename alice_users_test.LFN_BOOKED;';


select 'select "Now, moving the alice_data_test to alice_users_test";';
select concat("    alter table alice_data_test.", table_name, " rename alice_users_test.", table_name,"_data_test;") from information_schema.tables where table_name like "T%V%" and table_schema="alice_data_test";


select 'select "We are going to do the TAG0";';
select '  insert into alice_users_test.TAG0 (tagName, path, tableName, user) select tagName, path, concat(tableName, "_data_test"), user from alice_data_test.TAG0;';


select 'select now(),"AND now, the LL STATS";';
select '  insert ignore into alice_users_test.LL_STATS select * from alice_data_test.LL_STATS where alice_data_test.LL_STATS.tableNumber not in (select tableNumber from alice_users_test.LL_STATS);';


select 'select now(),"AND NOW THE LL ACTIONS";';
select '  insert ignore into alice_users_test.LL_ACTIONS select * from alice_data_test.LL_ACTIONS where alice_data_test.LL_ACTIONS.tableNumber not in (select tableNumber from alice_users_test.LL_ACTIONS);';


select 'select now(),"AND NOW THE GL ACTIONS";';
select '  insert ignore into alice_users_test.GL_ACTIONS select * from alice_data_test.GL_ACTIONS where alice_data_test.GL_ACTIONS.tableNumber not in (select tableNumber from alice_users_test.GL_ACTIONS);';

select 'select now(),"AND now, the GL STATS";';
select '  insert ignore into alice_users_test.GL_STATS select * from alice_data_test.GL_STATS where alice_data_test.GL_STATS.tableNumber not in (select tableNumber from alice_users_test.GL_STATS);';

select '  drop table alice_data_test.GL_STATS;';
select '  drop table alice_data_test.LL_STATS;';
select '  drop table alice_data_test.GL_ACTIONS;';
select '  drop table alice_data_test.LL_ACTIONS;';


select 'select now(),"DROPPING TRIGGERS";';
select concat("    drop trigger ", trigger_schema,".",trigger_name,";")  from information_schema.TRIGGERS;


select 'select now(),"DOING TABLE L10L";';
select '  update alice_users_test.INDEXTABLE set tableName=9 where tableName=10 and lfn="/alice/";';
select '  alter table alice_data_test.L10L rename alice_users_test.L9L;';                                  
select '  alter table alice_data_test.L10L_QUOTA rename alice_users_test.L9L_QUOTA;';                      
select '  alter table alice_data_test.L10L_broken rename alice_users_test.L9L_broken;';  

select 'select now(),"DROPPING TABLES";';
select '  drop table alice_data_test.L10LTMP;';
select '  drop table alice_data_test.L0L;';
select '  drop table alice_data_test.L0L_QUOTA;';
select '  drop table alice_data_test.L0L_broken;';

select 'select now(),"Moving L tables";';
select concat("    alter table alice_data_test.", table_name, " rename alice_users_test.", table_name,";") from 
information_schema.tables where  table_schema="alice_data_test" and (table_name like "L%L%" or table_name like "G%L%") and table_name not like 'LL%' and table_name not like 'GL%';  


select ' select now(), "Doing the collections";';
select '  set @maxC :=  (select max(collectionId) from alice_users_test.COLLECTIONS);';
select '  insert into alice_users_test.COLLECTIONS (collectionId, collGUID) select collectionId+@maxC, collGUID from alice_data_test.COLLECTIONS;';
select '  alter table COLLECTIONS_ELEM add unique (collectionId, origLFN);';
select '  insert ignore into alice_users_test.COLLECTIONS_ELEM (collectionId, origLFN, localName, guid, data) select collectionId+@maxC, origLFN, localName, guid, data from alice_data_test.COLLECTIONS_ELEM; ';
#select '  alter table alice_users_test.COLLECTIONS_ELEM convert to character set latin1 collate latin1_general_cs;';
#select '  insert into alice_users_test.COLLECTIONS_ELEM (collectionId, origLFN, localName, guid, data) select collectionId+@maxC, origLFN, localName, guid, data from alice_data_test.COLLECTIONS_ELEM where concat(collectionId+@maxC, origLFN) not in (select concat(collectionId, origLFN) from alice_users_test.COLLECTIONS_ELEM); ';

select ' delete from GL_STATS where tableNumber not in (select tableName from GUIDINDEX);';
select ' delete from GL_STATS where seNumber not in (select seNumber from SE);';
select ' delete from GL_ACTIONS where tableNumber not in (select tableName from GUIDINDEX);';
select ' delete from LL_STATS where tableNumber not in (select tableName from INDEXTABLE);';
select ' delete from LL_ACTIONS where tableNumber not in (select tableName from INDEXTABLE);';

select 'select "DROPPING DATA TABLES";';
select concat("    drop table alice_data_test.", table_name, ";") from 
information_schema.tables where table_schema="alice_data_test" ;
