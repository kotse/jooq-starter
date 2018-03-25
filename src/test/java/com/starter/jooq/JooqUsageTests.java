package com.starter.jooq;


import org.assertj.core.api.AssertionsForInterfaceTypes;
import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.generated.tables.Events;
import org.jooq.generated.tables.Persons;
import org.jooq.generated.tables.PersonsEvents;
import org.jooq.generated.tables.records.EventsRecord;
import org.jooq.generated.tables.records.LocationsRecord;
import org.jooq.generated.tables.records.PersonsEventsRecord;
import org.jooq.generated.tables.records.PersonsRecord;
import org.jooq.impl.DSL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.jooq.generated.Tables.*;
import static org.jooq.generated.Tables.PERSONS_EVENTS;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

public class JooqUsageTests {

    private Connection connection;

    private DSLContext jooq;

    @Before
    public void setUp() throws Exception {
        setUpConnection();

        jooq = DSL.using(connection, SQLDialect.POSTGRES);

        cleanUpData();
    }

    private void setUpConnection() throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");

        final String dbUrl = "jdbc:postgresql://localhost/jooq";
        final String username = "jooq";
        final String password = "jooq";

        connection = DriverManager.getConnection(dbUrl, username, password);
    }

    private void cleanUpData() {
        jooq.deleteFrom(PERSONS_EVENTS).execute();

        jooq.deleteFrom(PERSONS).execute();
        jooq.deleteFrom(EVENTS).execute();
        jooq.deleteFrom(LOCATIONS).execute();
    }

    @After
    public void tearDown() throws Exception {
        connection.close();
    }

    @Test
    public void jooq_can_build_simple_sql() {
        String sql = jooq.select().from(table("PERSON")).getSQL();

        assertThat(sql).isEqualToIgnoringCase("select * from PERSON");
    }

    @Test
    public void jooq_can_build_sql() {
        String sql = jooq.
                select(
                        field("PERSON.first_name"),
                        field("PERSONS_EVENTS.description")).
                from(table("PERSON")).
                join(table("PERSONS_EVENTS")).on(field("PERSON.ID").equal(field("PERSONS_EVENTS.person_id")))
                .getSQL();

        System.out.println("sql = " + sql);
    }

    @Test
    public void jooq_can_build_simple_typesafe_sql() {
        String sql = jooq.selectFrom(PERSONS).getSQL();

        /*
         The SQL that will get generated in a more readable format:
         select
           "public"."person"."id",
           "public"."person"."first_name",
           "public"."person"."last_name"
         from
           "public"."person"
        */

        System.out.println("sql = " + sql);
    }

    @Test
    public void jooq_can_build_typesafe_prepared_statement_sql_with_join() {
        String sql = jooq.
                select(PERSONS.FIRST_NAME, EVENTS.NAME).
                from(PERSONS).
                join(PERSONS_EVENTS).on(PERSONS.ID.equal(PERSONS_EVENTS.PERSON_ID)).
                join(EVENTS).on(EVENTS.ID.equal(PERSONS_EVENTS.EVENT_ID)).
                where(EVENTS.NAME.equal("any_value"))
                .getSQL();

        /*
         The SQL that will get generated in a more readable format:
         select
           "public"."person"."first_name", "public"."event"."name"
         from "public"."person"
           join "public"."PERSONS_EVENTS" on "public"."person"."id" = "public"."PERSONS_EVENTS"."person_id"
           join "public"."event" on "public"."event"."id" = "public"."PERSONS_EVENTS"."event_id"
         where
           "event"."name" == '?'
        */

        System.out.println("sql = " + sql);
    }

    @Test
    public void jooq_can_build_typesafe_sql_and_inline_parameters() {

        Persons p = PERSONS.as("p");
        Events e = EVENTS.as("e");
        PersonsEvents pel = PERSONS_EVENTS.as("pel");

        String sql = jooq.
                select(p.FIRST_NAME, e.NAME).
                from(p).
                join(pel).on(p.ID.equal(pel.PERSON_ID)).
                join(e).on(e.ID.equal(pel.EVENT_ID)).
                where((e.NAME).equalIgnoreCase("j Conference"))
                .getSQL(ParamType.INLINED);

        /*
         The SQL that will get generated in a more readable format:
         select "p"."first_name", "e"."name"
         from "public"."persons" as "p"
           join "public"."PERSONS_EVENTS" as "pel" on "p"."id" = "pel"."person_id"
           join "public"."events" as "e" on "e"."id" = "pel"."event_id"
         where lower("e"."name") = lower('j Conference')
        */

        System.out.println("sql = " + sql);
    }

    @Test
    public void jooq_can_fetch_records_that_are_a_list() {
        Result<PersonsRecord> personsResult = jooq.selectFrom(PERSONS).fetch();

        AssertionsForInterfaceTypes.assertThat(personsResult).isInstanceOf(List.class);
    }

    @Test
    public void jooq_can_lazy_fetch_records() {
        Cursor<PersonsRecord> aCursor = jooq.selectFrom(PERSONS).fetchLazy();

        try (Cursor<PersonsRecord> cursor = jooq.selectFrom(PERSONS).fetchLazy()) {
            while (cursor.hasNext()) {
                PersonsRecord personRecord = cursor.fetchOne();

                // doStuff here!
            }
        }
    }

    @Test
    public void jooq_can_insert_and_fetch_records_using_active_record() {
        PersonsRecord personRecord = person("Ivan", "Ivanov");

        Result<PersonsRecord> personRecords = jooq.selectFrom(PERSONS).fetch();

        AssertionsForInterfaceTypes.assertThat(personRecords).containsExactly(personRecord);

        //or even simpler
        Result<PersonsRecord> personRecordsAgain = jooq.fetch(PERSONS);

        AssertionsForInterfaceTypes.assertThat(personRecordsAgain).containsExactly(personRecord);
    }

    @Test
    public void jooq_can_fetch_single_records() {
        PersonsRecord personRecord = person("Ivan", "Ivanov");

        PersonsRecord fetchedPersonRecord =
                jooq.selectFrom(PERSONS)
                        .where(PERSONS.FIRST_NAME.eq("Ivan"))
                        .and(PERSONS.LAST_NAME.eq("Ivanov"))
                        .fetchOne();

        AssertionsForInterfaceTypes.assertThat(fetchedPersonRecord).isEqualTo(personRecord);
    }

    @Test
    public void jooq_can_fetch_into_object() {
        person("Ivan", "Ivanov");

        String firstName = jooq
                .select(PERSONS.FIRST_NAME)
                .from(PERSONS)
                .fetchOneInto(String.class);

        assertThat(firstName).isEqualTo("Ivan");
    }

    @Test
    public void jooq_can_get_and_operate_resultSet() {
        PersonsRecord personRecord = person("Ivan", "Ivanov");

        ResultSet resultSet = jooq.selectFrom(PERSONS).fetchResultSet();

        PersonsRecord fetchedRecord = jooq.fetchOne(resultSet).into(PERSONS);

        AssertionsForInterfaceTypes.assertThat(fetchedRecord).isEqualTo(personRecord);
    }

    @Test
    public void jooq_can_fetch_into_optional() {
        person("Ivan", "Ivanov");

        Optional<Record1<String>> optionalRecord = jooq
                .select(PERSONS.FIRST_NAME)
                .from(PERSONS)
                .fetchOptional();

        assertThat(optionalRecord).isPresent();
    }

    @Test
    public void jooq_can_fetch_into_empty_optional() {
        Optional<Record1<String>> optionalRecord = jooq
                .select(PERSONS.FIRST_NAME)
                .from(PERSONS)
                .fetchOptional();

        assertThat(optionalRecord).isNotPresent();
    }

    @Test
    public void jooq_can_fetch_fields_from_different_types() {
        PersonsRecord personRecord = person("Ivan", "Ivanov");
        EventsRecord eventRecord = event("java conference");
        LocationsRecord locationRecord = location("Plovdiv");

        personEvent(personRecord, eventRecord, locationRecord);

        Record2<String, String> fetchedRecord = jooq
                .select(PERSONS.FIRST_NAME, EVENTS.NAME)
                .from(PERSONS)
                .join(PERSONS_EVENTS).on(PERSONS.ID.equal(PERSONS_EVENTS.PERSON_ID))
                .join(EVENTS).on(EVENTS.ID.equal(PERSONS_EVENTS.EVENT_ID))
                .fetchOne();

        assertThat(fetchedRecord.get(PERSONS.FIRST_NAME)).isEqualTo(personRecord.getFirstName());
        assertThat(fetchedRecord.get(EVENTS.NAME)).isEqualTo("java conference");
    }

    @Test
    public void jooq_can_perform_simple_CRUD_on_a_record() {
        PersonsRecord personRecord = person("Ivan", "Ivanov");

        //fetch and compare to what we stored
        PersonsRecord fetchedPersonRecord = jooq
                .selectFrom(PERSONS)
                .where(PERSONS.FIRST_NAME.eq("Ivan"))
                .fetchOne();

        AssertionsForInterfaceTypes.assertThat(fetchedPersonRecord).isEqualTo(personRecord);


        //update stored
        personRecord.setLastName("Dimitrov");
        personRecord.update();

        //fetch and compare that update was OK
        PersonsRecord fetchedUpdatedPersonRecord = jooq
                .selectFrom(PERSONS)
                .where(PERSONS.FIRST_NAME.eq("Ivan"))
                .fetchOne();

        assertThat(fetchedUpdatedPersonRecord.getLastName()).isEqualTo("Dimitrov");


        //delete
        PersonsRecord personRecordToBeDeleted = jooq
                .selectFrom(PERSONS)
                .where(PERSONS.FIRST_NAME.eq("Ivan"))
                .fetchOne();

        personRecordToBeDeleted.delete();

        PersonsRecord emptyRecord = jooq
                .selectFrom(PERSONS)
                .where(PERSONS.FIRST_NAME.eq("Ivan"))
                .fetchOne();

        AssertionsForInterfaceTypes.assertThat(emptyRecord).isNull();
    }

    @Test
    public void jooq_can_fetch_into_a_map() {
        PersonsRecord personRecord = person("Ivan", "Ivanov");
        PersonsRecord anotherPersonRecord = person("Kostadin", "Golev");

        EventsRecord eventRecord = event("java conference");
        LocationsRecord locationRecord = location("Plovdiv");

        personEvent(personRecord, eventRecord, locationRecord);
        personEvent(anotherPersonRecord, eventRecord, locationRecord);

        Map<String, String> personEventMap = jooq
                .select(PERSONS.FIRST_NAME, EVENTS.NAME)
                .from(PERSONS)
                .join(PERSONS_EVENTS).on(PERSONS.ID.equal(PERSONS_EVENTS.PERSON_ID))
                .join(EVENTS).on(EVENTS.ID.equal(PERSONS_EVENTS.EVENT_ID))
                .fetch()
                .intoMap(PERSONS.FIRST_NAME, EVENTS.NAME);

        AssertionsForInterfaceTypes.assertThat(personEventMap).containsKeys("Ivan", "Kostadin");
        assertThat(personEventMap.get("Ivan")).isEqualTo("java conference");
        assertThat(personEventMap.get("Kostadin")).isEqualTo("java conference");
    }

    @Test
    public void jooq_can_fetch_into_a_map_group() {
        PersonsRecord personRecord = person("Ivan", "Ivanov");
        PersonsRecord anotherPersonRecord = person("Kostadin", "Golev");

        EventsRecord eventRecord = event("Java Conference");
        LocationsRecord locationRecord = location("Plovdiv");

        personEvent(personRecord, eventRecord, locationRecord);
        personEvent(anotherPersonRecord, eventRecord, locationRecord);

        SelectOnConditionStep<Record2<String, String>> jooqQuery = jooq
                .select(PERSONS.FIRST_NAME, EVENTS.NAME)
                .from(PERSONS)
                .join(PERSONS_EVENTS).on(PERSONS.ID.equal(PERSONS_EVENTS.PERSON_ID))
                .join(EVENTS).on(EVENTS.ID.equal(PERSONS_EVENTS.EVENT_ID));

        Map<String, List<String>> groupedMap = jooqQuery
                .fetch()
                .intoGroups(EVENTS.NAME, PERSONS.FIRST_NAME);

        AssertionsForInterfaceTypes.assertThat(groupedMap).containsOnlyKeys("Java Conference");
        AssertionsForInterfaceTypes.assertThat(groupedMap.get("Java Conference")).containsExactly("Ivan", "Kostadin");
    }

    @Test
    public void jooq_can_fetch_joined_records() {
        PersonsRecord personRecord = person("Ivan", "Ivanov");
        EventsRecord eventRecord = event("Java Conference");
        LocationsRecord locationRecord = location("Plovdiv");

        personEvent(personRecord, eventRecord, locationRecord);

        Record personJoinedWithEvents = jooq
                .select().from(PERSONS)
                .join(PERSONS_EVENTS).on(PERSONS.ID.equal(PERSONS_EVENTS.PERSON_ID))
                .join(EVENTS).on(EVENTS.ID.equal(PERSONS_EVENTS.EVENT_ID))
                .fetchOne();

        PersonsRecord fetchedPerson = personJoinedWithEvents.into(PERSONS);
        EventsRecord fetchedEvent = personJoinedWithEvents.into(EVENTS);

        AssertionsForInterfaceTypes.assertThat(fetchedPerson).isEqualTo(personRecord);
        AssertionsForInterfaceTypes.assertThat(fetchedEvent).isEqualTo(eventRecord);
    }

    private PersonsRecord person(String firstName, String lastName) {
        PersonsRecord personRecord = jooq.newRecord(PERSONS);

        personRecord.setFirstName(firstName);
        personRecord.setLastName(lastName);
        personRecord.store();

        return personRecord;
    }

    private EventsRecord event(String name) {
        EventsRecord eventRecord = jooq.newRecord(EVENTS);

        eventRecord.setName(name);
        eventRecord.store();

        return eventRecord;
    }

    private LocationsRecord location(String name) {
        LocationsRecord locationRecord = jooq.newRecord(LOCATIONS);

        locationRecord.setName(name);
        locationRecord.store();

        return locationRecord;
    }

    private PersonsEventsRecord personEvent(PersonsRecord personRecord, EventsRecord eventRecord, LocationsRecord locationRecord) {
        PersonsEventsRecord pelRecord = jooq.newRecord(PERSONS_EVENTS);

        pelRecord.setPersonId(personRecord.getId());
        pelRecord.setEventId(eventRecord.getId());

        pelRecord.store();

        return pelRecord;
    }
}
