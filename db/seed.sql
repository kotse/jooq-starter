-- Sample data

INSERT INTO person (first_name, last_name) VALUES ('Kostadin', 'Golev');

INSERT INTO location (name) VALUES ('Sofia');
INSERT INTO location (name) VALUES ('Plovdiv');

INSERT INTO event (name, location_id) VALUES ('JProfessionals', (SELECT id FROM location WHERE name = 'Plovdiv'));

INSERT INTO person_event_log (description, person_id, event_id, location_id) VALUES ('Went', 1, 1, 1);
INSERT INTO person_event_log (description, person_id, event_id, location_id) VALUES ('Went', 1, 1, 2);