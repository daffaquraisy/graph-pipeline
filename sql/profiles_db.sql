-- public.accounts definition

-- Drop table

-- DROP TABLE public.accounts;

CREATE TABLE public.accounts (
	account_id uuid NOT NULL DEFAULT gen_random_uuid(),
	email varchar(255) NOT NULL,
	hashed_password text NOT NULL,
	full_name varchar(100) NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT accounts_email_key UNIQUE (email),
	CONSTRAINT accounts_pkey PRIMARY KEY (account_id)
);


-- public."plans" definition

-- Drop table

-- DROP TABLE public."plans";

CREATE TABLE public."plans" (
	plan_id serial4 NOT NULL,
	plan_name varchar(50) NOT NULL,
	monthly_fee numeric(5, 2) NOT NULL,
	max_devices int4 NOT NULL,
	CONSTRAINT plans_pkey PRIMARY KEY (plan_id),
	CONSTRAINT plans_plan_name_key UNIQUE (plan_name)
);


-- public.payment_methods definition

-- Drop table

-- DROP TABLE public.payment_methods;

CREATE TABLE public.payment_methods (
	payment_method_id uuid NOT NULL DEFAULT gen_random_uuid(),
	account_id uuid NOT NULL,
	card_fingerprint varchar(100) NOT NULL,
	card_type varchar(20) NULL,
	card_last_four varchar(4) NOT NULL,
	CONSTRAINT payment_methods_account_id_card_fingerprint_key UNIQUE (account_id, card_fingerprint),
	CONSTRAINT payment_methods_pkey PRIMARY KEY (payment_method_id),
	CONSTRAINT payment_methods_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.accounts(account_id)
);


-- public.subscriptions definition

-- Drop table

-- DROP TABLE public.subscriptions;

CREATE TABLE public.subscriptions (
	subscription_id uuid NOT NULL DEFAULT gen_random_uuid(),
	account_id uuid NOT NULL,
	plan_id int4 NOT NULL,
	status varchar(20) NOT NULL,
	starts_at timestamptz NOT NULL,
	renews_at timestamptz NULL,
	CONSTRAINT subscriptions_pkey PRIMARY KEY (subscription_id),
	CONSTRAINT subscriptions_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.accounts(account_id),
	CONSTRAINT subscriptions_plan_id_fkey FOREIGN KEY (plan_id) REFERENCES public."plans"(plan_id)
);