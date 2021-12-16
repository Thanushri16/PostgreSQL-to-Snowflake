/* ******************************************************************************

* Product: Snowflake Migration Platform

* Utility: "Analyzer" which generates the metatables of the PostgreSQL database

* Date: Jun 2021

* Company: Dattendriya Data Science Solutions

********************************************************************************* */

-- SCHEMA: psg_meta_dvdrental_public

-- DROP SCHEMA psg_meta_dvdrental_public ;

CREATE SCHEMA psg_meta_dvdrental_public
    AUTHORIZATION postgres;


--table_metadata creation 
CREATE TABLE psg_meta_dvdrental_public.meta_tables AS (
SELECT tab.table_catalog, tab.table_schema, tab_columns.table_name, tab.table_type,tab_columns.column_name, data_type, character_maximum_length,
numeric_precision, is_nullable, tab_constraints.constraint_type, col_constraints.constraint_name,
col_check_constraints.check_clause
FROM information_schema.tables AS tab
 LEFT OUTER JOIN 
 information_schema.columns AS tab_columns
 ON tab.table_name = tab_columns.table_name
LEFT OUTER JOIN
information_schema.constraint_column_usage AS col_constraints
ON tab_columns.table_name = col_constraints.table_name AND
tab_columns.column_name = col_constraints.column_name
LEFT OUTER JOIN
information_schema.table_constraints AS tab_constraints
ON tab_constraints.constraint_name = col_constraints.constraint_name
LEFT OUTER JOIN
information_schema.check_constraints AS col_check_constraints
ON col_check_constraints.constraint_name = tab_constraints.constraint_name
WHERE  
tab_columns.table_schema = 'public' AND tab.table_type = 'BASE TABLE' 
ORDER BY tab_columns.table_name
	);



--view_metadata creation 
CREATE TABLE psg_meta_dvdrental_public.meta_views AS (
SELECT tab.table_catalog, tab.table_schema, tab_columns.table_name, tab.table_type,tab_columns.column_name, data_type, character_maximum_length,
numeric_precision, is_nullable, tab_constraints.constraint_type, col_constraints.constraint_name,
col_check_constraints.check_clause
FROM information_schema.tables AS tab
 LEFT OUTER JOIN 
 information_schema.columns AS tab_columns
 ON tab.table_name = tab_columns.table_name
LEFT OUTER JOIN
information_schema.constraint_column_usage AS col_constraints
ON tab_columns.table_name = col_constraints.table_name AND
tab_columns.column_name = col_constraints.column_name
LEFT OUTER JOIN
information_schema.table_constraints AS tab_constraints
ON tab_constraints.constraint_name = col_constraints.constraint_name
LEFT OUTER JOIN
information_schema.check_constraints AS col_check_constraints
ON col_check_constraints.constraint_name = tab_constraints.constraint_name
WHERE  
tab_columns.table_schema = 'public' AND tab.table_type = 'VIEW' 
ORDER BY tab_columns.table_name
	);


--function metadata creation
CREATE TABLE psg_meta_dvdrental_public.meta_functions AS (
SELECT routine_catalog,routine_schema,routine_name,routine_type,data_type,type_udt_catalog,
type_udt_schema, type_udt_name, external_language, is_deterministic
FROM information_schema.routines
 WHERE specific_schema NOT IN
       ('pg_catalog', 'information_schema')
   AND type_udt_name != 'trigger'
	);

--stored procedure metadata without the ones in pg_catalog
CREATE TABLE psg_meta_dvdrental_public.meta_procedures AS (
select n.nspname as schema_name, 
       p.proname as procedure_name,
	   a.rolname AS procedure_owner,
	   t.typname AS return_type,
       l.lanname as language_type, 
       case when l.lanname = 'internal' then p.prosrc 
            else pg_get_functiondef(p.oid) 
            end as definition, 
       pg_get_function_arguments(p.oid) as arguments 
from pg_proc p 
LEFT JOIN pg_type t ON p.prorettype=t.oid   
LEFT JOIN pg_authid a ON p.proowner=a.oid 
left join pg_namespace n on p.pronamespace = n.oid 
left join pg_language l on p.prolang = l.oid 
where n.nspname not in ('pg_catalog', 'information_schema') 
      and p.prokind = 'p' 
order by schema_name, 
         procedure_name 
	);

--Created a custom function to count the number of rows in each table
create or replace function 
psg_meta_dvdrental_public.fn_meta_rowcount(schema text, tablename text) returns integer
as
$body$
declare
  result integer;
  query varchar;
begin
  query := 'SELECT count(1) FROM ' || schema || '.' || tablename;
  execute query into result;
  return result;
end;
$body$
language plpgsql;

--table metadata creation with row count
CREATE TABLE psg_meta_dvdrental_public.rowcount_tables AS ( 
SELECT tab.table_catalog, tab.table_schema, tab_columns.table_name, psg_meta_dvdrental_public.fn_meta_rowcount(tab.table_schema, tab_columns.table_name),
tab.table_type,tab_columns.column_name, data_type, character_maximum_length,
numeric_precision, is_nullable, tab_constraints.constraint_type, col_constraints.constraint_name,
col_check_constraints.check_clause
FROM information_schema.tables AS tab
 LEFT OUTER JOIN 
 information_schema.columns AS tab_columns
 ON tab.table_name = tab_columns.table_name
LEFT OUTER JOIN
information_schema.constraint_column_usage AS col_constraints
ON tab_columns.table_name = col_constraints.table_name AND
tab_columns.column_name = col_constraints.column_name
LEFT OUTER JOIN
information_schema.table_constraints AS tab_constraints
ON tab_constraints.constraint_name = col_constraints.constraint_name
LEFT OUTER JOIN
information_schema.check_constraints AS col_check_constraints
ON col_check_constraints.constraint_name = tab_constraints.constraint_name
WHERE  
tab_columns.table_schema = 'public' AND tab.table_type = 'BASE TABLE' 
ORDER BY tab_columns.table_name
);





--Select row count using pg_catalog
CREATE TABLE psg_meta_dvdrental_public.tablerowcount AS (
SELECT 
  nspname AS schemaname,relname,reltuples
FROM pg_class C
LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
WHERE 
  nspname NOT IN ('pg_catalog', 'information_schema') AND
  relkind='r' AND nspname='public'
ORDER BY reltuples DESC );




--Base tables of extracted views
CREATE TABLE psg_meta_dvdrental_public.meta_views_basetables AS (
select v.table_catalog,v.table_schema,v.table_name as view_name, v.view_definition,
view_usage.table_name AS base_table,
v.check_option, v.is_updatable 
FROM 
 information_schema.views AS v
 LEFT OUTER JOIN information_schema.view_table_usage as view_usage
 ON v.table_schema=view_usage.view_schema AND v.table_catalog=view_usage.view_catalog
 AND v.table_name=view_usage.view_name
WHERE v.table_schema NOT IN ('pg_catalog', 'information_schema')
);





--Parameters of stored procedures
CREATE TABLE psg_meta_dvdrental_public.meta_procedureparameters AS ( 
select n.nspname as schema_name, 
       p.proname as procedure_name,
	   a.rolname AS procedure_owner,
	   t.typname AS return_type,
       l.lanname as language_type, 
       case when l.lanname = 'internal' then p.prosrc 
            else pg_get_functiondef(p.oid) 
            end as definition, 
       pg_get_function_arguments(p.oid) as arguments,
	   p.proargmodes AS argument_modes,
	   p.proargnames AS argument_names
from pg_proc p 
LEFT JOIN pg_type t ON p.prorettype=t.oid   
LEFT JOIN pg_authid a ON p.proowner=a.oid 
left join pg_namespace n on p.pronamespace = n.oid 
left join pg_language l on p.prolang = l.oid 
where n.nspname not in ('pg_catalog', 'information_schema') 
      and p.prokind = 'p' 
order by schema_name, procedure_name
);





--Parameters of functions
CREATE TABLE psg_meta_dvdrental_public.meta_functionparameters AS ( 
SELECT r.routine_catalog,r.routine_schema,r.routine_name,r.specific_name, r.routine_type, 
r.data_type AS routine_data_type,
para.parameter_mode, para.parameter_name, para.data_type AS parameter_data_type, 
r.type_udt_catalog, r.type_udt_schema, r.type_udt_name, r.external_language, r.is_deterministic
FROM information_schema.routines AS r 
LEFT OUTER JOIN 
 information_schema.parameters AS para
 ON r.specific_catalog = para.specific_catalog AND r.specific_schema=para.specific_schema 
 AND r.specific_name = para.specific_name
 WHERE r.specific_schema NOT IN
       ('pg_catalog', 'information_schema')
   AND r.type_udt_name != 'trigger' AND r.routine_schema = 'public'
);



-- Details of activity
CREATE TABLE psg_meta_dvdrental_public.stats_activity AS
(
	select * from pg_stat_activity where datname='dvdrental'
);


--Metadata of database
CREATE TABLE psg_meta_dvdrental_public.meta_db AS
(
	select * from pg_stat_database where datname='dvdrental'
);

--User table statistics
CREATE TABLE psg_meta_dvdrental_public.stats_usertables AS
(
	select * from pg_stat_user_tables where schemaname='public' order by relname
);

--Index statistics
CREATE TABLE psg_meta_dvdrental_public.stats_index AS
(
	select * from pg_stat_all_indexes where schemaname NOT IN ('pg_catalog', 'information_schema') 
	and schemaname !~ '^pg_' order by relname
);

--User function statistics
CREATE TABLE psg_meta_dvdrental_public.stats_usersfunc AS
(
	select * from pg_stat_user_functions where schemaname='public'
);

--Table and column details of the reference tables referred by the foreign key of a table 
CREATE TABLE psg_meta_dvdrental_public.parent_child_table_relation AS (
SELECT
    tc.table_name, kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM
    information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage 
        AS kcu ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage 
        AS ccu ON ccu.constraint_name = tc.constraint_name
WHERE constraint_type = 'FOREIGN KEY'
ORDER BY tc.table_name
	);


--Views depending on the tables along with column details
CREATE TABLE psg_meta_dvdrental_public.table_view_dependency AS (
SELECT d.refobjid::regclass AS ref_object,nsp.nspname AS schema_name,
a.attname AS col_name,             -- column name
v.oid::regclass AS view              
FROM pg_attribute AS a   -- columns for the table
   JOIN pg_depend AS d   -- objects that depend on the column
      ON d.refobjsubid = a.attnum AND d.refobjid = a.attrelid
   JOIN pg_rewrite AS r  -- rules depending on the column
      ON r.oid = d.objid
   JOIN pg_class AS v    -- views for the rules
      ON v.oid = r.ev_class 
   JOIN pg_namespace as nsp
      ON v.relnamespace = nsp.oid
WHERE v.relkind = 'v'    -- only interested in views
  -- dependency must be a rule depending on a relation
  AND d.classid = 'pg_rewrite'::regclass
  AND d.refclassid = 'pg_class'::regclass 
  AND d.deptype = 'n'    -- normal dependency
  AND v.relname !~ '^pg_'
  AND nsp.nspname = 'public'
  --AND a.attrelid = 'actor'::regclass for output of a particular table actor
  --AND a.attname = 'rental_rate' for output of a particular column rental rate in the table actor
  ORDER BY d.refobjid,a.attname
	);