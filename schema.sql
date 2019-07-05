CREATE TABLE public."auth-accounts"
(
    key character varying(255) NOT NULL,
    value bytea,
    CONSTRAINT "auth-accounts_pkey" PRIMARY KEY (key)
);

CREATE TABLE public."auth-requests"
(
    key character varying(255) NOT NULL,
    value bytea,
    CONSTRAINT "auth-requests_pkey" PRIMARY KEY (key)
);

CREATE TABLE public."data-stores"
(
    key character varying(255) NOT NULL,
    value bytea,
    CONSTRAINT "data-stores_pkey" PRIMARY KEY (key)
);