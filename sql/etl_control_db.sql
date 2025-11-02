-- public.cypher_scripts definition

-- Drop table

-- DROP TABLE public.cypher_scripts;

CREATE TABLE public.cypher_scripts (
	script_id serial4 NOT NULL,
	script_name varchar(255) NOT NULL,
	file_path varchar(1024) NOT NULL,
	file_size_kb int4 NULL,
	last_updated_at timestamptz NULL,
	last_run_time timestamptz NULL,
	CONSTRAINT cypher_scripts_file_path_key UNIQUE (file_path),
	CONSTRAINT cypher_scripts_pkey PRIMARY KEY (script_id),
	CONSTRAINT cypher_scripts_script_name_key UNIQUE (script_name)
);


-- public.etl_run_logs definition

-- Drop table

-- DROP TABLE public.etl_run_logs;

CREATE TABLE public.etl_run_logs (
	log_id serial4 NOT NULL,
	run_start_time timestamptz NOT NULL DEFAULT now(),
	run_end_time timestamptz NULL,
	status varchar(50) NOT NULL DEFAULT 'pending'::character varying,
	CONSTRAINT etl_run_logs_pkey PRIMARY KEY (log_id)
);


-- public.node_label_mappings definition

-- Drop table

-- DROP TABLE public.node_label_mappings;

CREATE TABLE public.node_label_mappings (
	mapping_id serial4 NOT NULL,
	table_name varchar(255) NOT NULL,
	node_label varchar(255) NOT NULL,
	is_active bool NOT NULL DEFAULT true,
	CONSTRAINT node_label_mappings_pkey PRIMARY KEY (mapping_id),
	CONSTRAINT node_label_mappings_table_name_key UNIQUE (table_name)
);


-- public.relationship_mappings definition

-- Drop table

-- DROP TABLE public.relationship_mappings;

CREATE TABLE public.relationship_mappings (
	relationship_id serial4 NOT NULL,
	source_db varchar(100) NOT NULL,
	relationship_type varchar(100) NOT NULL,
	from_label varchar(255) NOT NULL,
	to_label varchar(255) NOT NULL,
	join_condition text NOT NULL,
	junction_table varchar(255) NULL,
	junction_label varchar(255) NULL,
	is_active bool NOT NULL DEFAULT true,
	description text NULL,
	execution_order int4 NOT NULL DEFAULT 0,
	CONSTRAINT relationship_mappings_pkey PRIMARY KEY (relationship_id)
);


-- public.source_databases definition

-- Drop table

-- DROP TABLE public.source_databases;

CREATE TABLE public.source_databases (
	source_id serial4 NOT NULL,
	source_name varchar(100) NOT NULL,
	db_host varchar(255) NOT NULL,
	db_port int4 NOT NULL DEFAULT 5432,
	db_name varchar(100) NOT NULL,
	db_user varchar(100) NOT NULL,
	db_password varchar(255) NULL,
	is_active bool NOT NULL DEFAULT true,
	last_accessed timestamptz NULL,
	CONSTRAINT source_databases_pkey PRIMARY KEY (source_id),
	CONSTRAINT source_databases_source_name_key UNIQUE (source_name)
);


-- public.etl_table_logs definition

-- Drop table

-- DROP TABLE public.etl_table_logs;

CREATE TABLE public.etl_table_logs (
	table_log_id serial4 NOT NULL,
	log_id int4 NOT NULL,
	source_id int4 NOT NULL,
	table_name varchar(255) NOT NULL,
	status varchar(50) NOT NULL DEFAULT 'pending'::character varying,
	is_progressing bool NOT NULL DEFAULT false,
	is_done bool NOT NULL DEFAULT false,
	start_time timestamptz NULL,
	end_time timestamptz NULL,
	rows_processed int4 NULL DEFAULT 0,
	error_message text NULL,
	CONSTRAINT etl_table_logs_pkey PRIMARY KEY (table_log_id),
	CONSTRAINT etl_table_logs_log_id_fkey FOREIGN KEY (log_id) REFERENCES public.etl_run_logs(log_id) ON DELETE CASCADE,
	CONSTRAINT etl_table_logs_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.source_databases(source_id)
);


-- public.field_exclusion_rules definition

-- Drop table

-- DROP TABLE public.field_exclusion_rules;

CREATE TABLE public.field_exclusion_rules (
	rule_id serial4 NOT NULL,
	source_id int4 NOT NULL,
	table_name varchar(255) NOT NULL,
	column_name varchar(255) NOT NULL,
	is_excluded bool NOT NULL DEFAULT true,
	reason text NULL,
	CONSTRAINT field_exclusion_rules_pkey PRIMARY KEY (rule_id),
	CONSTRAINT field_exclusion_rules_source_id_table_name_column_name_key UNIQUE (source_id, table_name, column_name),
	CONSTRAINT field_exclusion_rules_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.source_databases(source_id) ON DELETE CASCADE
);