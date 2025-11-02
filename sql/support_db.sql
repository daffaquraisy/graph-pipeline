-- public.ticket_tags definition

-- Drop table

-- DROP TABLE public.ticket_tags;

CREATE TABLE public.ticket_tags (
	tag_id serial4 NOT NULL,
	tag_name varchar(50) NOT NULL,
	CONSTRAINT ticket_tags_pkey PRIMARY KEY (tag_id),
	CONSTRAINT ticket_tags_tag_name_key UNIQUE (tag_name)
);


-- public.tickets definition

-- Drop table

-- DROP TABLE public.tickets;

CREATE TABLE public.tickets (
	ticket_id varchar(20) NOT NULL,
	customer_email varchar(255) NOT NULL,
	agent_id int4 NOT NULL,
	subject text NOT NULL,
	status varchar(20) NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	last_updated_at timestamptz NULL,
	CONSTRAINT tickets_pkey PRIMARY KEY (ticket_id)
);


-- public.ticket_tag_link definition

-- Drop table

-- DROP TABLE public.ticket_tag_link;

CREATE TABLE public.ticket_tag_link (
	ticket_id varchar(20) NOT NULL,
	tag_id int4 NOT NULL,
	CONSTRAINT ticket_tag_link_pkey PRIMARY KEY (ticket_id, tag_id),
	CONSTRAINT ticket_tag_link_tag_id_fkey FOREIGN KEY (tag_id) REFERENCES public.ticket_tags(tag_id),
	CONSTRAINT ticket_tag_link_ticket_id_fkey FOREIGN KEY (ticket_id) REFERENCES public.tickets(ticket_id)
);