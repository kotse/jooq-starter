-- Initial set of tables

CREATE TABLE persons (
    id serial constraint pk_person primary key,
    first_name varchar not null,
    last_name varchar not null
);

CREATE TABLE locations (
    id serial constraint pk_location primary key,
    name varchar not null
);

CREATE TABLE events (
	id serial constraint pk_event primary key,
	name varchar not null,
	location_id int constraint fk_events_location references locations(id)
);

CREATE TABLE persons_events (
	id serial constraint pk_person_event_log primary key,
	description varchar,
	person_id int constraint fk_persons_events_person references persons(id),
	event_id int constraint fk_persons_events_event references events(id)
);