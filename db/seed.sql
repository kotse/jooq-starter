-- Sample data

INSERT INTO persons (id, first_name, last_name) VALUES (1, 'Kostadin', 'Golev');

INSERT INTO locations (id, name) VALUES (1, 'Sofia');
INSERT INTO locations (id, name) VALUES (2, 'Plovdiv');

INSERT INTO events (id, name, location_id) VALUES (1, 'Java Event', 1);

INSERT INTO persons_events (description, person_id, event_id) VALUES ('Went', 1, 1);