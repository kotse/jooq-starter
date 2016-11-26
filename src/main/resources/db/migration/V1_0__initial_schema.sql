-- Initial set of tables

CREATE TABLE person (
    id serial constraint pk_person primary key,
    first_name varchar not null,
    last_name varchar not null
);

CREATE TABLE location (
    id serial constraint pk_location primary key,
    name varchar not null
);

CREATE TABLE event (
	id serial constraint pk_event primary key,
	name varchar not null,
	location_id int constraint fk_event_location references location(id)
);

CREATE TABLE person_event_log (
	id serial constraint pk_person_event_log primary key,
	description varchar,
	person_id int constraint fk_person_event_log_person references person(id),
	event_id int constraint fk_person_event_log_event references event(id),
	location_id int constraint fk_person_event_log_location references location(id)
);