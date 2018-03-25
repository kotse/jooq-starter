package com.starter.jooq;


import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.generated.tables.records.EventsRecord;
import org.jooq.impl.DSL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.jooq.generated.Tables.EVENTS;

public class JooqUsageTests {

    private Connection connection;

    private DSLContext jooq;

    @Before
    public void setUp() throws Exception {
        Class.forName("org.postgresql.Driver");

        final String dbUrl = "jdbc:postgresql://localhost/jooq";
        final String username = "jooq";
        final String password = "jooq";

        connection = DriverManager.getConnection(dbUrl, username, password);

        jooq = DSL.using(connection, SQLDialect.POSTGRES);
    }

    @After
    public void tearDown() throws Exception {
        connection.close();
    }

    @Test
    public void jooqUsageTest() {
        EventsRecord e = jooq.newRecord(EVENTS);

        e.setName("Dev.bg");

        e.store();
    }
}
