package com.starter.jooq;

import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.generated.tables.Event;
import org.jooq.generated.tables.Person;
import org.jooq.generated.tables.PersonEventLog;
import org.jooq.generated.tables.records.EventRecord;
import org.jooq.generated.tables.records.LocationRecord;
import org.jooq.generated.tables.records.PersonEventLogRecord;
import org.jooq.generated.tables.records.PersonRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.jooq.generated.tables.Event.EVENT;
import static org.jooq.generated.tables.Location.LOCATION;
import static org.jooq.generated.tables.Person.PERSON;
import static org.jooq.generated.tables.PersonEventLog.PERSON_EVENT_LOG;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@RunWith(SpringRunner.class)
@SpringBootTest
public class JooqStarterApplicationTests {

    @Autowired
    DSLContext jooq;

    @Before
    public void setUp() throws Exception {
        jooq.deleteFrom(PERSON_EVENT_LOG).execute();

        jooq.deleteFrom(PERSON).execute();
        jooq.deleteFrom(LOCATION).execute();
        jooq.deleteFrom(EVENT).execute();
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
                        field("PERSON_EVENT_LOG.description")).
                from(table("PERSON")).
                join(table("PERSON_EVENT_LOG")).on(field("PERSON.ID").equal(field("PERSON_EVENT_LOG.person_id")))
                .getSQL();

        assertThat(sql).isEqualToIgnoringCase("select PERSON.first_name, PERSON_EVENT_LOG.description from PERSON join PERSON_EVENT_LOG on PERSON.ID = PERSON_EVENT_LOG.person_id");
    }

    @Test
    public void jooq_can_build_simple_typesafe_sql() {
        String sql = jooq.selectFrom(PERSON).getSQL();

        /*
         The SQL that will get generated in a more readable format:
         select
           "public"."person"."id",
           "public"."person"."first_name",
           "public"."person"."last_name"
         from
           "public"."person"
        */

        assertThat(sql).isEqualTo("select \"public\".\"person\".\"id\", \"public\".\"person\".\"first_name\", \"public\".\"person\".\"last_name\" from \"public\".\"person\"");
    }

    @Test
    public void jooq_can_build_typesafe_prepared_statement_sql_with_join() {
        String sql = jooq.
                select(PERSON.FIRST_NAME, EVENT.NAME).
                from(PERSON).
                  join(PERSON_EVENT_LOG).on(PERSON.ID.equal(PERSON_EVENT_LOG.PERSON_ID)).
                  join(EVENT).on(EVENT.ID.equal(PERSON_EVENT_LOG.EVENT_ID)).
                where(EVENT.NAME.equal("any_value"))
                .getSQL();

        /*
         The SQL that will get generated in a more readable format:
         select
           "public"."person"."first_name", "public"."event"."name"
         from "public"."person"
           join "public"."person_event_log" on "public"."person"."id" = "public"."person_event_log"."person_id"
           join "public"."event" on "public"."event"."id" = "public"."person_event_log"."event_id"
         where
           "event"."name" == '?'
        */

        assertThat(sql).isEqualTo("select \"public\".\"person\".\"first_name\", \"public\".\"event\".\"name\" from \"public\".\"person\" join \"public\".\"person_event_log\" on \"public\".\"person\".\"id\" = \"public\".\"person_event_log\".\"person_id\" join \"public\".\"event\" on \"public\".\"event\".\"id\" = \"public\".\"person_event_log\".\"event_id\" where \"public\".\"event\".\"name\" = ?");
    }

    @Test
    public void jooq_can_build_typesafe_sql_and_inline_parameters() {

        Person p = PERSON.as("p");
        Event e = EVENT.as("e");
        PersonEventLog pel = PERSON_EVENT_LOG.as("pel");

        String sql = jooq.
                select(p.FIRST_NAME, e.NAME).
                from(p).
                  join(pel).on(p.ID.equal(pel.PERSON_ID)).
                  join(e).on(e.ID.equal(pel.EVENT_ID)).
                where((e.NAME).equalIgnoreCase("jProfessionals"))
                .getSQL(ParamType.INLINED);

        /*
         The SQL that will get generated in a more readable format:
         select "p"."first_name", "e"."name"
         from "public"."person" as "p"
           join "public"."person_event_log" as "pel" on "p"."id" = "pel"."person_id"
           join "public"."event" as "e" on "e"."id" = "pel"."event_id"
         where lower("e"."name") = lower('jProfessionals')
        */

        assertThat(sql).isEqualTo("select \"p\".\"first_name\", \"e\".\"name\" from \"public\".\"person\" as \"p\" join \"public\".\"person_event_log\" as \"pel\" on \"p\".\"id\" = \"pel\".\"person_id\" join \"public\".\"event\" as \"e\" on \"e\".\"id\" = \"pel\".\"event_id\" where lower(\"e\".\"name\") = lower(\'jProfessionals\')");
    }

    @Test
    public void jooq_can_fetch_records_that_are_a_list() {
        Result<PersonRecord> personsResult = jooq.selectFrom(PERSON).fetch();

        assertThat(personsResult).isInstanceOf(List.class);
    }

    @Test
    public void jooq_can_lazy_fetch_records() {
        Cursor<PersonRecord> aCursor = jooq.selectFrom(PERSON).fetchLazy();

        try (Cursor<PersonRecord> cursor = jooq.selectFrom(PERSON).fetchLazy()) {
            while (cursor.hasNext()) {
                PersonRecord personRecord = cursor.fetchOne();

                // doStuff here!
            }
        }
    }

    @Test
    public void jooq_can_insert_and_fetch_records_using_active_record() {
        PersonRecord personRecord = personRecord("Ivan", "Ivanov");

        Result<PersonRecord> personRecords = jooq.selectFrom(PERSON).fetch();

        assertThat(personRecords).containsExactly(personRecord);

        //or even simpler
        Result<PersonRecord> personRecordsAgain = jooq.fetch(PERSON);

        assertThat(personRecordsAgain).containsExactly(personRecord);
    }

    @Test
    public void jooq_can_fetch_single_records() {
        PersonRecord personRecord = personRecord("Ivan", "Ivanov");

        PersonRecord fetchedPersonRecord =
                jooq.selectFrom(PERSON)
                        .where(PERSON.FIRST_NAME.eq("Ivan"))
                        .and(PERSON.LAST_NAME.eq("Ivanov"))
                        .fetchOne();

        assertThat(fetchedPersonRecord).isEqualTo(personRecord);
    }

    @Test
    public void jooq_can_fetch_into_object() {
        personRecord("Ivan", "Ivanov");

        String firstName = jooq
                .select(PERSON.FIRST_NAME)
                .from(PERSON)
                .fetchOneInto(String.class);

        assertThat(firstName).isEqualTo("Ivan");
    }

    @Test
    public void jooq_can_get_and_operate_resultSet() {
        PersonRecord personRecord = personRecord("Ivan", "Ivanov");

        ResultSet resultSet = jooq.selectFrom(PERSON).fetchResultSet();

        PersonRecord fetchedRecord = jooq.fetchOne(resultSet).into(PERSON);

        assertThat(fetchedRecord).isEqualTo(personRecord);
    }

    @Test
    public void jooq_can_fetch_into_optional() {
        personRecord("Ivan", "Ivanov");

        Optional<Record1<String>> optionalRecord = jooq
                .select(PERSON.FIRST_NAME)
                .from(PERSON)
                .fetchOptional();

        assertThat(optionalRecord).isPresent();
    }

    @Test
    public void jooq_can_fetch_into_empty_optional() {
        Optional<Record1<String>> optionalRecord = jooq
                .select(PERSON.FIRST_NAME)
                .from(PERSON)
                .fetchOptional();

        assertThat(optionalRecord).isNotPresent();
    }

    @Test
    public void jooq_can_fetch_fields_from_different_types() {
        PersonRecord personRecord = personRecord("Ivan", "Ivanov");
        EventRecord eventRecord = eventRecord("JProfessionals");
        LocationRecord locationRecord = locationRecord("Plovdiv");

        personEventLogRecord(personRecord, eventRecord, locationRecord);

        Record2<String, String> fetchedRecord = jooq
                .select(PERSON.FIRST_NAME, EVENT.NAME)
                .from(PERSON)
                .join(PERSON_EVENT_LOG).on(PERSON.ID.equal(PERSON_EVENT_LOG.PERSON_ID))
                .join(EVENT).on(EVENT.ID.equal(PERSON_EVENT_LOG.EVENT_ID))
                .fetchOne();

        assertThat(fetchedRecord.get(PERSON.FIRST_NAME)).isEqualTo(personRecord.getFirstName());
        assertThat(fetchedRecord.get(EVENT.NAME)).isEqualTo("JProfessionals");
    }

    @Test
    public void jooq_can_perform_simple_CRUD_on_a_record() {
        PersonRecord personRecord = personRecord("Ivan", "Ivanov");

        //fetch and compare to what we stored
        PersonRecord fetchedPersonRecord = jooq
                .selectFrom(PERSON)
                .where(PERSON.FIRST_NAME.eq("Ivan"))
                .fetchOne();

        assertThat(fetchedPersonRecord).isEqualTo(personRecord);


        //update stored
        personRecord.setLastName("Dimitrov");
        personRecord.update();

        //fetch and compare that update was OK
        PersonRecord fetchedUpdatedPersonRecord = jooq
                .selectFrom(PERSON)
                .where(PERSON.FIRST_NAME.eq("Ivan"))
                .fetchOne();

        assertThat(fetchedUpdatedPersonRecord.getLastName()).isEqualTo("Dimitrov");


        //delete
        PersonRecord personRecordToBeDeleted = jooq
                .selectFrom(PERSON)
                .where(PERSON.FIRST_NAME.eq("Ivan"))
                .fetchOne();

        personRecordToBeDeleted.delete();

        PersonRecord emptyRecord = jooq
                .selectFrom(PERSON)
                .where(PERSON.FIRST_NAME.eq("Ivan"))
                .fetchOne();

        assertThat(emptyRecord).isNull();
    }

    @Test
    public void jooq_can_fetch_into_a_map() {
        PersonRecord personRecord = personRecord("Ivan", "Ivanov");
        PersonRecord anotherPersonRecord = personRecord("Kostadin", "Golev");

        EventRecord eventRecord = eventRecord("JProfessionals");
        LocationRecord locationRecord = locationRecord("Plovdiv");

        personEventLogRecord(personRecord, eventRecord, locationRecord);
        personEventLogRecord(anotherPersonRecord, eventRecord, locationRecord);

        Map<String, String> personEventMap = jooq
                .select(PERSON.FIRST_NAME, EVENT.NAME)
                .from(PERSON)
                .join(PERSON_EVENT_LOG).on(PERSON.ID.equal(PERSON_EVENT_LOG.PERSON_ID))
                .join(EVENT).on(EVENT.ID.equal(PERSON_EVENT_LOG.EVENT_ID))
                .fetch()
                .intoMap(PERSON.FIRST_NAME, EVENT.NAME);

        assertThat(personEventMap).containsKeys("Ivan", "Kostadin");
        assertThat(personEventMap.get("Ivan")).isEqualTo("JProfessionals");
        assertThat(personEventMap.get("Kostadin")).isEqualTo("JProfessionals");
    }

    @Test
    public void jooq_can_fetch_into_a_map_group() {
        PersonRecord personRecord = personRecord("Ivan", "Ivanov");
        PersonRecord anotherPersonRecord = personRecord("Kostadin", "Golev");

        EventRecord eventRecord = eventRecord("JProfessionals");
        LocationRecord locationRecord = locationRecord("Plovdiv");

        personEventLogRecord(personRecord, eventRecord, locationRecord);
        personEventLogRecord(anotherPersonRecord, eventRecord, locationRecord);

        Map<String, List<String>> groupedMap = jooq
                .select(PERSON.FIRST_NAME, EVENT.NAME)
                .from(PERSON)
                .join(PERSON_EVENT_LOG).on(PERSON.ID.equal(PERSON_EVENT_LOG.PERSON_ID))
                .join(EVENT).on(EVENT.ID.equal(PERSON_EVENT_LOG.EVENT_ID))
                .fetch()
                .intoGroups(EVENT.NAME, PERSON.FIRST_NAME);

        assertThat(groupedMap).containsOnlyKeys("JProfessionals");
        assertThat(groupedMap.get("JProfessionals")).containsExactly("Ivan", "Kostadin");
    }

    @Test
    public void jooq_can_fetch_joined_records() {
        PersonRecord personRecord = personRecord("Ivan", "Ivanov");
        EventRecord eventRecord = eventRecord("JProfessionals");
        LocationRecord locationRecord = locationRecord("Plovdiv");

        personEventLogRecord(personRecord, eventRecord, locationRecord);

        Record personJoinedWithEvents = jooq
                .select().from(PERSON)
                .join(PERSON_EVENT_LOG).on(PERSON.ID.equal(PERSON_EVENT_LOG.PERSON_ID))
                .join(EVENT).on(EVENT.ID.equal(PERSON_EVENT_LOG.EVENT_ID))
                .fetchOne();

        PersonRecord fetchedPerson = personJoinedWithEvents.into(PERSON);
        EventRecord fetchedEvent = personJoinedWithEvents.into(EVENT);

        assertThat(fetchedPerson).isEqualTo(personRecord);
        assertThat(fetchedEvent).isEqualTo(eventRecord);
    }

    private PersonRecord personRecord(String firstName, String lastName) {
        PersonRecord personRecord = jooq.newRecord(PERSON);

        personRecord.setFirstName(firstName);
        personRecord.setLastName(lastName);
        personRecord.store();

        return personRecord;
    }

    private EventRecord eventRecord(String name) {
        EventRecord eventRecord = jooq.newRecord(EVENT);

        eventRecord.setName(name);
        eventRecord.store();

        return eventRecord;
    }

    private LocationRecord locationRecord(String name) {
        LocationRecord locationRecord = jooq.newRecord(LOCATION);

        locationRecord.setName(name);
        locationRecord.store();

        return locationRecord;
    }

    private PersonEventLogRecord personEventLogRecord(PersonRecord personRecord, EventRecord eventRecord, LocationRecord locationRecord) {
        PersonEventLogRecord pelRecord = jooq.newRecord(PERSON_EVENT_LOG);

        pelRecord.setPersonId(personRecord.getId());
        pelRecord.setLocationId(locationRecord.getId());
        pelRecord.setEventId(eventRecord.getId());

        pelRecord.store();

        return pelRecord;
    }
}
