
CREATE FUNCTION kv_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION kv_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER kv_fdw
  HANDLER kv_fdw_handler
  VALIDATOR kv_fdw_validator;

CREATE FUNCTION kv_ddl_event_end_trigger()
RETURNS event_trigger
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE EVENT TRIGGER kv_ddl_event_end
ON ddl_command_end
EXECUTE PROCEDURE kv_ddl_event_end_trigger();