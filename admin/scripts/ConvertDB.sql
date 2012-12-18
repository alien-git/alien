#Run this script with
# LD_LIBRARY_PATH=/opt/alien/lib:/opt/alien/lib/mysql/ /opt/alien/bin/mysql -h alientest03 -u admin -p... -P 3307 --skip-column-names  

select '  use alice_users;';
select 'select "Dropping the HOSTS TABLE"; ';
select '  drop table HOSTS;';
select '  alter table INDEXTABLE drop column hostindex, drop column  indexId;';

select 'select "Now, moving the alice_data to alice_users";';
select concat("    alter table alice_data.", table_name, " rename alice_users.", table_name,"_data;") from information_schema.tables where table_name like "T%V%" and table_schema="alice_data";


select 'select "We are going to do the TAG0";';
select '  insert into alice_users.TAG0 (tagName, path, tableName, user) select tagName, path, concat(tableName, "_data"), user from alice_data.TAG0;';
select '  drop table  alice_data.TAG0;';


select 'select now(),"AND now, the stats";';
select '  insert ignore into alice_users.LL_STATS select * from alice_data.LL_STATS;';
select '  drop table alice_data.LL_STATS;';


select 'select now(),"AND NOW THE ACTIONS";';
select '  insert ignore into alice_users.LL_ACTIONS select * from alice_data.LL_ACTIONS;';
select '  drop table alice_data.LL_ACTIONS;';

select 'select now(),"DROPPING TRIGGERS";';
select concat("    drop trigger ", trigger_schema,".",trigger_name,";")  from information_schema.TRIGGERS;


select 'select now(),"DOING TABLE L10L";';
select '  update alice_users.INDEXTABLE set tableName=9 where tableName=10 and lfn="/alice/";';
select '  alter table alice_data.L10L rename alice_users.L9L;';                                  
select '  alter table alice_data.L10L_QUOTA rename alice_users.L9L_QUOTA;';                      
select '  alter table alice_data.L10L_broken rename alice_users.L9L_broken;';  

select 'select now(),"DROPPING TABLES";';
select '  drop table alice_data.L10LTMP;';
select '  drop table alice_data.L0L;';
select '  drop table alice_data.L0L_QUOTA;';
select '  drop table alice_data.L0L_broken;';

select 'select now(),"Moving L tables";';
select concat("    alter table alice_data.", table_name, " rename alice_users.", table_name,";") from 
information_schema.tables where table_name like "L_%L%" and table_schema="alice_data" ;  



select ' select now(), "Doing the collections";';
select '  set @maxC :=  (select max(collectionId) from alice_users.COLLECTIONS);';
select '  insert into alice_users.COLLECTIONS (collectionId, collGUID) select collectionId+@maxC, collGUID from alice_data.COLLECTIONS;';
select '  insert into alice_users.COLLECTIONS_ELEM (collectionId, origLFN, localName, guid, data) 
  select collectionId+@maxC, origLFN, localName, guid, data from alice_data.COLLECTIONS_ELEM; ';



select 'select "Modifying the SEDistance table";';
select '  alter table SE modify column  seDemoteRead float not null default 0 ;';
select '  alter table SE modify column  seDemoteWrite float not null default 0;';
select '  alter table SEDistance modify   seNumber integer not null;';
