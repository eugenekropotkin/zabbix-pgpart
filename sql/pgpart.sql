
-- create schema

CREATE SCHEMA partitions AUTHORIZATION zabbix;



-- functions


-- Function: public.trg_partition()

-- DROP FUNCTION public.trg_partition();

CREATE OR REPLACE FUNCTION public.trg_partition()
  RETURNS trigger AS
$BODY$
DECLARE
prefix text := 'partitions.';
timeformat text;
selector text;
_interval INTERVAL;
tablename text;
startdate text;
enddate text;
create_table_part text;
create_index_part text;
create_index_part1 text;
create_index_part2 text;
BEGIN
 
selector = TG_ARGV[0];
 
IF selector = 'day' THEN
timeformat := 'YYYY_MM_DD';
ELSIF selector = 'month' THEN
timeformat := 'YYYY_MM';
END IF;
 
_interval := '1 ' || selector;
tablename :=  TG_TABLE_NAME || '_p' || TO_CHAR(TO_TIMESTAMP(NEW.clock), timeformat);
 
EXECUTE 'INSERT INTO ' || prefix || quote_ident(tablename) || ' SELECT ($1).*' USING NEW;
RETURN NULL;
 
EXCEPTION
WHEN undefined_table THEN
 
startdate := EXTRACT(epoch FROM date_trunc(selector, TO_TIMESTAMP(NEW.clock)));
enddate := EXTRACT(epoch FROM date_trunc(selector, TO_TIMESTAMP(NEW.clock) + _interval ));
 
create_table_part:= 'CREATE TABLE IF NOT EXISTS '|| prefix || quote_ident(tablename) || ' (CHECK ((clock >= ' || quote_literal(startdate) || ' AND clock < ' || quote_literal(enddate) || '))) INHERITS ('|| TG_TABLE_NAME || ')';
create_index_part:= 'CREATE INDEX '|| quote_ident(tablename) || '_1 on ' || prefix || quote_ident(tablename) || '(itemid,clock)';
-- create_index_part1:= 'CREATE INDEX '|| quote_ident(tablename) || '_i on ' || prefix || quote_ident(tablename) || '(itemid)';
-- create_index_part2:= 'CREATE INDEX '|| quote_ident(tablename) || '_c on ' || prefix || quote_ident(tablename) || '(clock)';
 
EXECUTE create_table_part;
EXECUTE create_index_part;
-- EXECUTE create_index_part1;
-- EXECUTE create_index_part2;
 
EXECUTE 'INSERT INTO ' || prefix || quote_ident(tablename) || ' SELECT ($1).*' USING NEW;
RETURN NULL;
 
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION public.trg_partition()
  OWNER TO zabbix;







-- Function: public.trg_partition_events()

-- DROP FUNCTION public.trg_partition_events();

CREATE OR REPLACE FUNCTION public.trg_partition_events()
  RETURNS trigger AS
$BODY$
DECLARE
prefix text := 'partitions.';
timeformat text;
selector text;
_interval INTERVAL;
tablename text;
startdate text;
enddate text;
create_table_part text;
create_index_part1 text;
create_index_part2 text;
create_index_part3 text;
create_index_part4 text;
BEGIN
 
selector = TG_ARGV[0];
 
IF selector = 'day' THEN
timeformat := 'YYYY_MM_DD';
ELSIF selector = 'month' THEN
timeformat := 'YYYY_MM';
END IF;
 
_interval := '1 ' || selector;
tablename :=  TG_TABLE_NAME || '_p' || TO_CHAR(TO_TIMESTAMP(NEW.clock), timeformat);
 
EXECUTE 'INSERT INTO ' || prefix || quote_ident(tablename) || ' SELECT ($1).*' USING NEW;
RETURN NULL;
 
EXCEPTION
WHEN undefined_table THEN
 
startdate := EXTRACT(epoch FROM date_trunc(selector, TO_TIMESTAMP(NEW.clock)));
enddate := EXTRACT(epoch FROM date_trunc(selector, TO_TIMESTAMP(NEW.clock) + _interval ));
 
create_table_part:= 'CREATE TABLE IF NOT EXISTS '|| prefix || quote_ident(tablename) || ' (CHECK ((clock >= ' || quote_literal(startdate) || ' AND clock < ' || quote_literal(enddate) || '))) INHERITS ('|| TG_TABLE_NAME || ')';
create_index_part1:= 'CREATE INDEX '|| quote_ident(tablename) || '_o on ' || prefix || quote_ident(tablename) || '(objectid)';
create_index_part2:= 'CREATE INDEX '|| quote_ident(tablename) || '_c on ' || prefix || quote_ident(tablename) || '(clock)';
create_index_part3:= 'CREATE INDEX '|| quote_ident(tablename) || '_v on ' || prefix || quote_ident(tablename) || '(value)';
create_index_part4:= 'CREATE INDEX '|| quote_ident(tablename) || '_e on ' || prefix || quote_ident(tablename) || '(eventid)';
 
EXECUTE create_table_part;
EXECUTE create_index_part1;
EXECUTE create_index_part2;
EXECUTE create_index_part3;
EXECUTE create_index_part4;
 
EXECUTE 'INSERT INTO ' || prefix || quote_ident(tablename) || ' SELECT ($1).*' USING NEW;
RETURN NULL;
 
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION public.trg_partition_events()
  OWNER TO postgres;










-- apply to old tables

CREATE TRIGGER partition_trg
  BEFORE INSERT
  ON public.trends
  FOR EACH ROW
  EXECUTE PROCEDURE public.trg_partition('month');

CREATE TRIGGER partition_trg
  BEFORE INSERT
  ON public.trends_uint
  FOR EACH ROW
  EXECUTE PROCEDURE public.trg_partition('month');

--

CREATE TRIGGER trg_partition_events
  BEFORE INSERT
  ON public.events
  FOR EACH ROW
  EXECUTE PROCEDURE public.trg_partition_events('month');

-- + public.acknowledges ?

--

CREATE TRIGGER partition_trg
  BEFORE INSERT
  ON public.history
  FOR EACH ROW
  EXECUTE PROCEDURE public.trg_partition('day');

CREATE TRIGGER partition_trg
  BEFORE INSERT
  ON public.history_log
  FOR EACH ROW
  EXECUTE PROCEDURE public.trg_partition('day');

CREATE TRIGGER partition_trg
  BEFORE INSERT
  ON public.history_str
  FOR EACH ROW
  EXECUTE PROCEDURE public.trg_partition('day'

CREATE TRIGGER partition_trg
  BEFORE INSERT
  ON public.history_text
  FOR EACH ROW
  EXECUTE PROCEDURE public.trg_partition('day');

CREATE TRIGGER partition_trg
  BEFORE INSERT
  ON public.history_uint
  FOR EACH ROW
  EXECUTE PROCEDURE public.trg_partition('day');

--

