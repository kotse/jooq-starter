package com.starter.jooq;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.generated.tables.records.PersonRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

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
                select(field("PERSON.first_name"), field("PERSON_EVENT_LOG.description")).
                from(table("PERSON")).
                join(table("PERSON_EVENT_LOG")).on(field("PERSON.ID").equal(field("PERSON_EVENT_LOG.person_id"))).getSQL();

        assertThat(sql).isEqualToIgnoringCase("select PERSON.first_name, PERSON_EVENT_LOG.description from PERSON join PERSON_EVENT_LOG on PERSON.ID = PERSON_EVENT_LOG.person_id");
    }

    @Test
    public void jooq_can_build_simple_typesafe_sql() {
        String sql = jooq.selectFrom(PERSON).getSQL();

        // The SQL that will get generated in a more readable format:
        // select
        //   "public"."person"."id",
        //   "public"."person"."first_name",
        //   "public"."person"."last_name"
        // from
        //   "public"."person"

        assertThat(sql).isEqualTo("select \"public\".\"person\".\"id\", \"public\".\"person\".\"first_name\", \"public\".\"person\".\"last_name\" from \"public\".\"person\"");
    }

    @Test
    public void jooq_can_build_typesafe_sql_with_join() {
        String sql = jooq.select(PERSON.FIRST_NAME, EVENT.NAME)
                .from(PERSON).join(PERSON_EVENT_LOG).on(PERSON.ID.equal(PERSON_EVENT_LOG.PERSON_ID)).
                        join(EVENT).on(EVENT.ID.equal(PERSON_EVENT_LOG.EVENT_ID)).where(EVENT.NAME.equal("?")).getSQL();

        // The SQL that will get generated in a more readable format:
        // select
        //   "public"."person"."first_name", "public"."event"."name"
        // from "public"."person"
        //   join "public"."person_event_log" on "public"."person"."id" = "public"."person_event_log"."person_id"
        //   join "public"."event" on "public"."event"."id" = "public"."person_event_log"."event_id"
        // where
        //   "event"."name" == '?'

        assertThat(sql).isEqualTo("select \"public\".\"person\".\"first_name\", \"public\".\"event\".\"name\" from \"public\".\"person\" join \"public\".\"person_event_log\" on \"public\".\"person\".\"id\" = \"public\".\"person_event_log\".\"person_id\" join \"public\".\"event\" on \"public\".\"event\".\"id\" = \"public\".\"person_event_log\".\"event_id\" where \"public\".\"event\".\"name\" = ?");
    }

    @Test
    public void jooq_can_fetch_records_that_are_a_list() {
        Result<PersonRecord> personsResult = jooq.selectFrom(PERSON).fetch();

        assertThat(personsResult).isInstanceOf(List.class);
    }

    @Test
    public void jooq_can_fetched_records_list_is_empty_when_no_data() {
        Result<PersonRecord> personsResult = jooq.selectFrom(PERSON).fetch();

        assertThat(personsResult).isEmpty();
    }

    @Test
    public void jooq_can_insert_and_fetch_records_using_active_record() {
        PersonRecord personRecord = jooq.newRecord(PERSON);

        personRecord.setFirstName("Ivan");
        personRecord.setLastName("Ivanov");

        personRecord.store();

        Result<PersonRecord> personRecords = jooq.selectFrom(PERSON).fetch();

        assertThat(personRecords).containsExactly(personRecord);
    }

    @Test
    public void jooq_can_fetch_single_records() {
        PersonRecord personRecord = jooq.newRecord(PERSON);

        personRecord.setFirstName("Ivan");
        personRecord.setLastName("Ivanov");

        personRecord.store();

        PersonRecord fetchedPersonRecord = jooq.selectFrom(PERSON).where(PERSON.FIRST_NAME.eq("Ivan")).fetchOne();

        assertThat(fetchedPersonRecord).isEqualTo(personRecord);
    }

    @Test
    public void jooq_can_perform_simple_CRUD_on_a_record() {
        PersonRecord personRecord = jooq.newRecord(PERSON);

        personRecord.setFirstName("Ivan");
        personRecord.setLastName("Ivanov");

        personRecord.store();

        //fetch and compare to what we stored
        PersonRecord fetchedPersonRecord = jooq.selectFrom(PERSON).where(PERSON.FIRST_NAME.eq("Ivan")).fetchOne();
        assertThat(fetchedPersonRecord).isEqualTo(personRecord);

        //update stored
        personRecord.setLastName("Dimitrov");
        personRecord.update();

        //fetch and compare that update was OK
        PersonRecord fetchedUpdatedPersonRecord = jooq.selectFrom(PERSON).where(PERSON.FIRST_NAME.eq("Ivan")).fetchOne();
        assertThat(fetchedUpdatedPersonRecord.getLastName()).isEqualTo("Dimitrov");


        //delete
        PersonRecord personRecordToBeDeleted = jooq.selectFrom(PERSON).where(PERSON.FIRST_NAME.eq("Ivan")).fetchOne();
        personRecordToBeDeleted.delete();

        PersonRecord emptyRecord = jooq.selectFrom(PERSON).where(PERSON.FIRST_NAME.eq("Ivan")).fetchOne();
        assertThat(emptyRecord).isNull();
    }
}
