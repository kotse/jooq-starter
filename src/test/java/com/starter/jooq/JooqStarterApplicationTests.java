package com.starter.jooq;

import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.generated.tables.Events;
import org.jooq.generated.tables.Persons;
import org.jooq.generated.tables.PersonsEvents;
import org.jooq.generated.tables.records.EventsRecord;
import org.jooq.generated.tables.records.LocationsRecord;
import org.jooq.generated.tables.records.PersonsEventsRecord;
import org.jooq.generated.tables.records.PersonsRecord;
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
import static org.jooq.generated.Tables.*;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@RunWith(SpringRunner.class)
@SpringBootTest
public class JooqStarterApplicationTests {

    @Autowired
    DSLContext jooq;

    @Before
    public void setUp() throws Exception {
        jooq.deleteFrom(PERSONS_EVENTS).execute();

        jooq.deleteFrom(PERSONS).execute();
        jooq.deleteFrom(EVENTS).execute();
        jooq.deleteFrom(LOCATIONS).execute();
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
}
