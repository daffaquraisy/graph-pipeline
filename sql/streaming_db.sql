-- public.media_content definition

-- Drop table

-- DROP TABLE public.media_content;

CREATE TABLE public.media_content (
	media_id varchar(50) NOT NULL,
	title varchar(255) NOT NULL,
	media_type varchar(20) NOT NULL,
	genre varchar(50) NULL,
	release_date date NULL,
	duration_minutes int4 NULL,
	CONSTRAINT media_content_pkey PRIMARY KEY (media_id)
);


-- public.play_events definition

-- Drop table

-- DROP TABLE public.play_events;

CREATE TABLE public.play_events (
	event_id bigserial NOT NULL,
	session_id varchar(100) NOT NULL,
	account_id_from_token uuid NOT NULL,
	media_id varchar(50) NOT NULL,
	event_timestamp timestamptz NOT NULL DEFAULT now(),
	"action" varchar(20) NOT NULL,
	ip_address inet NOT NULL,
	device_fingerprint varchar(100) NOT NULL,
	CONSTRAINT play_events_pkey PRIMARY KEY (event_id),
	CONSTRAINT play_events_media_id_fkey FOREIGN KEY (media_id) REFERENCES public.media_content(media_id)
);