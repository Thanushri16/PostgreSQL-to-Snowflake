/* *************************************************************

* Product: Snowflake Migration Platform

* Utility: "Transformer" which generates Snowflake DDL script

* Date: Jun 2021

* Company: Dattendriya Data Science Solutions

*************************************************************** */

--Database Created
CREATE OR REPLACE DATABASE PSG_META_DVDRENTAL_PUBLIC;

--Schema Created
USE PSG_META_DVDRENTAL_PUBLIC.PUBLIC;

--TABLE:actor
CREATE OR REPLACE TABLE actor ( 
 first_name char (45) NOT NULL,
 last_update timestamp NOT NULL,
 last_name char (45) NOT NULL,
 actor_id integer NOT NULL
 );

--TABLE:address
CREATE OR REPLACE TABLE address ( 
 postal_code char (10),
 district char (20) NOT NULL,
 address2 char (50),
 address char (50) NOT NULL,
 address_id integer NOT NULL,
 last_update timestamp NOT NULL,
 phone char (20) NOT NULL,
 city_id smallint NOT NULL
 );

--TABLE:category
CREATE OR REPLACE TABLE category ( 
 last_update timestamp NOT NULL,
 name char (25) NOT NULL,
 category_id integer NOT NULL
 );

--TABLE:city
CREATE OR REPLACE TABLE city ( 
 last_update timestamp NOT NULL,
 city char (50) NOT NULL,
 city_id integer NOT NULL,
 country_id smallint NOT NULL
 );

--TABLE:country
CREATE OR REPLACE TABLE country ( 
 last_update timestamp NOT NULL,
 country_id integer NOT NULL,
 country char (50) NOT NULL
 );

--TABLE:customer
CREATE OR REPLACE TABLE customer ( 
 create_date date NOT NULL,
 last_update timestamp,
 active integer,
 customer_id integer NOT NULL,
 store_id smallint NOT NULL,
 first_name char (45) NOT NULL,
 last_name char (45) NOT NULL,
 email char (50),
 activebool boolean NOT NULL,
 address_id smallint NOT NULL
 );

--TABLE:film
CREATE OR REPLACE TABLE film ( 
 fulltext varchar NOT NULL, --^^WARNING: Closest target datatype is VARCHAR^^,
 rental_rate decimal NOT NULL,
 length smallint,
 replacement_cost decimal NOT NULL,
 rating, --**ALERT: Missing datatype mapping**,
 last_update timestamp NOT NULL,
 special_features, --**ALERT: Missing datatype mapping**,
 film_id integer NOT NULL,
 title char (255) NOT NULL,
 description text,
 release_year integer,
 language_id smallint NOT NULL,
 rental_duration smallint NOT NULL
 );

--TABLE:film_actor
CREATE OR REPLACE TABLE film_actor ( 
 film_id smallint NOT NULL,
 last_update timestamp NOT NULL,
 actor_id smallint NOT NULL
 );

--TABLE:film_category
CREATE OR REPLACE TABLE film_category ( 
 film_id smallint NOT NULL,
 category_id smallint NOT NULL,
 last_update timestamp NOT NULL
 );

--TABLE:inventory
CREATE OR REPLACE TABLE inventory ( 
 film_id smallint NOT NULL,
 inventory_id integer NOT NULL,
 last_update timestamp NOT NULL,
 store_id smallint NOT NULL
 );

--TABLE:language
CREATE OR REPLACE TABLE language ( 
 name char (20) NOT NULL,
 last_update timestamp NOT NULL,
 language_id integer NOT NULL
 );

--TABLE:payment
CREATE OR REPLACE TABLE payment ( 
 payment_date timestamp NOT NULL,
 payment_id integer NOT NULL,
 customer_id smallint NOT NULL,
 staff_id smallint NOT NULL,
 rental_id integer NOT NULL,
 amount decimal NOT NULL
 );

--TABLE:rental
CREATE OR REPLACE TABLE rental ( 
 rental_date timestamp NOT NULL,
 rental_id integer NOT NULL,
 return_date timestamp,
 staff_id smallint NOT NULL,
 last_update timestamp NOT NULL,
 customer_id smallint NOT NULL,
 inventory_id integer NOT NULL
 );

--TABLE:staff
CREATE OR REPLACE TABLE staff ( 
 address_id smallint NOT NULL,
 staff_id integer NOT NULL,
 first_name char (45) NOT NULL,
 last_name char (45) NOT NULL,
 email char (50),
 store_id smallint NOT NULL,
 active boolean NOT NULL,
 username char (16) NOT NULL,
 password char (40),
 last_update timestamp NOT NULL,
 picture binary
 );

--TABLE:store
CREATE OR REPLACE TABLE store ( 
 store_id integer NOT NULL,
 manager_staff_id smallint NOT NULL,
 address_id smallint NOT NULL,
 last_update timestamp NOT NULL
 );

--Adding Primary Key constraints
ALTER TABLE actor ADD CONSTRAINT actor_pkey primary key (actor_id);
ALTER TABLE address ADD CONSTRAINT address_pkey primary key (address_id);
ALTER TABLE category ADD CONSTRAINT category_pkey primary key (category_id);
ALTER TABLE city ADD CONSTRAINT city_pkey primary key (city_id);
ALTER TABLE country ADD CONSTRAINT country_pkey primary key (country_id);
ALTER TABLE customer ADD CONSTRAINT customer_pkey primary key (customer_id);
ALTER TABLE film ADD CONSTRAINT film_pkey primary key (film_id);
ALTER TABLE film_actor ADD CONSTRAINT film_actor_pkey primary key (actor_id,film_id);
ALTER TABLE film_category ADD CONSTRAINT film_category_pkey primary key (film_id,category_id);
ALTER TABLE inventory ADD CONSTRAINT inventory_pkey primary key (inventory_id);
ALTER TABLE language ADD CONSTRAINT language_pkey primary key (language_id);
ALTER TABLE payment ADD CONSTRAINT payment_pkey primary key (payment_id);
ALTER TABLE rental ADD CONSTRAINT rental_pkey primary key (rental_id);
ALTER TABLE staff ADD CONSTRAINT staff_pkey primary key (staff_id);
ALTER TABLE store ADD CONSTRAINT store_pkey primary key (store_id);

--Adding unique constraints
ALTER TABLE customer ADD CONSTRAINT uni_cus unique (store_id,address_id);
ALTER TABLE staff ADD CONSTRAINT uni_staff unique (address_id);

--Adding Foreign Key constraints
ALTER TABLE film_actor ADD CONSTRAINT film_actor_actor_id_fkey foreign key (actor_id) references actor (actor_id);
ALTER TABLE customer ADD CONSTRAINT customer_address_id_fkey foreign key (address_id) references address (address_id);
ALTER TABLE staff ADD CONSTRAINT staff_address_id_fkey foreign key (address_id) references address (address_id);
ALTER TABLE store ADD CONSTRAINT store_address_id_fkey foreign key (address_id) references address (address_id);
ALTER TABLE film_category ADD CONSTRAINT film_category_category_id_fkey foreign key (category_id) references category (category_id);
ALTER TABLE address ADD CONSTRAINT fk_address_city foreign key (city_id) references city (city_id);
ALTER TABLE city ADD CONSTRAINT fk_city foreign key (country_id) references country (country_id);
ALTER TABLE payment ADD CONSTRAINT payment_customer_id_fkey foreign key (customer_id) references customer (customer_id);
ALTER TABLE rental ADD CONSTRAINT rental_customer_id_fkey foreign key (customer_id) references customer (customer_id);
ALTER TABLE film_actor ADD CONSTRAINT film_actor_film_id_fkey foreign key (film_id) references film (film_id);
ALTER TABLE film_category ADD CONSTRAINT film_category_film_id_fkey foreign key (film_id) references film (film_id);
ALTER TABLE inventory ADD CONSTRAINT inventory_film_id_fkey foreign key (film_id) references film (film_id);
ALTER TABLE rental ADD CONSTRAINT rental_inventory_id_fkey foreign key (inventory_id) references inventory (inventory_id);
ALTER TABLE film ADD CONSTRAINT film_language_id_fkey foreign key (language_id) references language (language_id);
ALTER TABLE payment ADD CONSTRAINT payment_rental_id_fkey foreign key (rental_id) references rental (rental_id);
ALTER TABLE payment ADD CONSTRAINT payment_staff_id_fkey foreign key (staff_id) references staff (staff_id);
ALTER TABLE rental ADD CONSTRAINT rental_staff_id_key foreign key (staff_id) references staff (staff_id);
ALTER TABLE store ADD CONSTRAINT store_manager_staff_id_fkey foreign key (manager_staff_id) references staff (staff_id);
