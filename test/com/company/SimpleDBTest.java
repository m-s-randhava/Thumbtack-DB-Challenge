package com.company;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.UUID;

public class SimpleDBTest {
    private SimpleDB sdb = null;
    private static Logger logger = Logger.getRootLogger();

    public void setUp() {
    }

    @Before
    public void createSimpleDBInstance() {
        logger.info("... creating simple db instance anew ...");
        this.sdb = new SimpleDB();
    }

    @After
    public void destroySimpleDBInstance() {
        logger.info("... destroying simple db instance ...");
        this.sdb = null;
    }

    @org.junit.Test
    public void testGetNotPresent() throws Exception {
        UUID session_id = this.sdb.registerClientSession();
        String value = this.sdb.get("some_value", session_id);
        Assert.assertEquals(null, value);
    }

    @org.junit.Test
    public void testSetGet() throws Exception {
        UUID session_id = this.sdb.registerClientSession();
        this.sdb.set("some_value", "1", session_id);
        String value = this.sdb.get("some_value", session_id);
        Assert.assertEquals("1", value);
    }

    @org.junit.Test
    public void testNumequalto() throws Exception {
        UUID session_id = this.sdb.registerClientSession();
        this.sdb.set("some_value", "1", session_id);
        int count = this.sdb.numequalto("1", session_id);
        Assert.assertEquals(1, count);
    }

    @org.junit.Test
    public void testNumequaltoInsideTransaction() throws Exception {
        UUID session_id = this.sdb.registerClientSession();
        String value = this.sdb.get("some_value", session_id);
        Assert.assertEquals(null, value);
        this.sdb.set("some_value", "11", session_id);
        value = this.sdb.get("some_value", session_id);
        Assert.assertEquals("11", value);
        this.sdb.set("some_other_value", "2", session_id);
        this.sdb.beginTransaction(session_id);
        this.sdb.set("some_value", "1", session_id);
        value = this.sdb.get("some_value", session_id);
        Assert.assertEquals("1", value);
        int count = this.sdb.numequalto("1", session_id);
        Assert.assertEquals(1, count);
        count = this.sdb.numequalto("2", session_id);
        Assert.assertEquals(1, count);
        count = this.sdb.numequalto("11", session_id);
        Assert.assertEquals(0, count);
    }

    @org.junit.Test
    public void testNumequaltoWithUnsetInsideTransaction() throws Exception {
        UUID session_id = this.sdb.registerClientSession();
        String value = this.sdb.get("some_value", session_id);
        Assert.assertEquals(null, value);
        this.sdb.set("some_other_value", "2", session_id);
        this.sdb.beginTransaction(session_id);
        this.sdb.set("some_value", "1", session_id);
        value = this.sdb.get("some_value", session_id);
        Assert.assertEquals("1", value);
        int count = this.sdb.numequalto("1", session_id);
        Assert.assertEquals(1, count);
        count = this.sdb.numequalto("2", session_id);
        Assert.assertEquals(1, count);
        this.sdb.unset("some_value", session_id);
        count = this.sdb.numequalto("1", session_id);
        Assert.assertEquals(0, count);
    }

    @org.junit.Test
    public void testSetInsideTransaction() throws Exception {
        UUID session_id = this.sdb.registerClientSession();
        String value = this.sdb.get("some_value", session_id);
        Assert.assertEquals(null, value);
        this.sdb.beginTransaction(session_id);
        this.sdb.set("some_value", "1", session_id);
        value = this.sdb.get("some_value", session_id);
        Assert.assertEquals("1", value);
    }

    @org.junit.Test
    public void testRollbackTransaction() throws Exception {
        UUID session_id = this.sdb.registerClientSession();
        String value = this.sdb.get("some_value", session_id);
        Assert.assertEquals(null, value);
        this.sdb.set("some_value", "1", session_id);
        value = this.sdb.get("some_value", session_id);
        Assert.assertEquals("1", value);
        this.sdb.beginTransaction(session_id);
        this.sdb.set("some_value", "2", session_id);
        value = this.sdb.get("some_value", session_id);
        Assert.assertEquals("2", value);
        this.sdb.rollbackTransaction(session_id);
        value = this.sdb.get("some_value", session_id);
        Assert.assertEquals("1", value);
    }

    @org.junit.Test
    public void testRollbackTransactionWithNumequalto() throws Exception {
        UUID session_id = this.sdb.registerClientSession();
        String value = this.sdb.get("some_value", session_id);
        Assert.assertEquals(null, value);
        int count = this.sdb.numequalto("1", session_id);
        Assert.assertEquals(0, count);
        this.sdb.set("some_value", "1", session_id);
        value = this.sdb.get("some_value", session_id);
        Assert.assertEquals("1", value);
        count = this.sdb.numequalto("1", session_id);
        Assert.assertEquals(1, count);
        this.sdb.beginTransaction(session_id);
        this.sdb.set("some_value", "2", session_id);
        value = this.sdb.get("some_value", session_id);
        Assert.assertEquals("2", value);
        this.sdb.set("some_other_value", "2", session_id);
        value = this.sdb.get("some_other_value", session_id);
        Assert.assertEquals("2", value);
        count = this.sdb.numequalto("2", session_id);
        Assert.assertEquals(2, count);
        this.sdb.rollbackTransaction(session_id);
        value = this.sdb.get("some_value", session_id);
        Assert.assertEquals("1", value);
        count = this.sdb.numequalto("2", session_id);
        Assert.assertEquals(0, count);
        count = this.sdb.numequalto("1", session_id);
        Assert.assertEquals(1, count);
    }

    @org.junit.Test
    public void testGetInsideTransaction() throws Exception {
        UUID session_id = this.sdb.registerClientSession();
        String value = this.sdb.get("some_value", session_id);
        Assert.assertEquals(null, value);
        this.sdb.set("some_value", "1", session_id);
        this.sdb.beginTransaction(session_id);
        this.sdb.set("some_other_value", "2", session_id);
        value = this.sdb.get("some_other_value", session_id);
        Assert.assertEquals("2", value);
        this.sdb.set("some_value", "3", session_id);
        value = this.sdb.get("some_value", session_id);
        Assert.assertEquals("3", value);
        int count = this.sdb.numequalto("1",session_id);
        Assert.assertEquals(0, count);
    }

    @org.junit.Test
    public void testUnset() throws Exception {
        UUID session_id = this.sdb.registerClientSession();
        String value = this.sdb.get("some_value", session_id);
        Assert.assertEquals(null, value);
        this.sdb.set("some_value", "1", session_id);
        value = this.sdb.get("some_value", session_id);
        int count = this.sdb.numequalto("1",session_id);
        Assert.assertEquals("1", value);
        Assert.assertEquals(1, count);
        this.sdb.unset("some_value", session_id);
        value = this.sdb.get("some_value", session_id);
        Assert.assertEquals(null, value);
        count = this.sdb.numequalto("1",session_id);
        Assert.assertEquals(0, count);
    }

    @org.junit.Test
    public void testUnsetInsideTransaction() throws Exception {
        UUID session_id = this.sdb.registerClientSession();
        String value = this.sdb.get("some_value", session_id);
        Assert.assertEquals(null, value);
        this.sdb.set("some_other_value", "1", session_id);
        value = this.sdb.get("some_other_value", session_id);
        Assert.assertEquals("1", value);
        this.sdb.beginTransaction(session_id);
        this.sdb.set("some_value", "1", session_id);
        value = this.sdb.get("some_value", session_id);
        Assert.assertEquals("1", value);
        this.sdb.unset("some_value", session_id);
        value = this.sdb.get("some_value", session_id);
        Assert.assertEquals(null, value);
        this.sdb.unset("some_other_value", session_id);
        value = this.sdb.get("some_other_value", session_id);
        Assert.assertEquals(null, value);
    }

    @org.junit.Test
    public void testCommit() throws Exception {
        UUID session_id = this.sdb.registerClientSession();
        String value = this.sdb.get("some_value", session_id);
        Assert.assertEquals(null, value);
        int count = this.sdb.numequalto("1", session_id);
        Assert.assertEquals(0, count);
        this.sdb.set("some_value", "1", session_id);
        value = this.sdb.get("some_value", session_id);
        Assert.assertEquals("1", value);
        count = this.sdb.numequalto("1", session_id);
        Assert.assertEquals(1, count);

        this.sdb.beginTransaction(session_id);
        this.sdb.set("some_value", "2", session_id);
        value = this.sdb.get("some_value", session_id);
        Assert.assertEquals("2", value);
        count = this.sdb.numequalto("1", session_id);
        Assert.assertEquals(0, count);
        this.sdb.set("some_other_value", "2", session_id);
        value = this.sdb.get("some_other_value", session_id);
        Assert.assertEquals("2", value);

        this.sdb.commitTransaction(session_id);
        value = this.sdb.get("some_other_value", session_id);
        Assert.assertEquals("2", value);
        value = this.sdb.get("some_value", session_id);
        Assert.assertEquals("2", value);

        try {
            this.sdb.rollbackTransaction(session_id);
        } catch(SimpleDB.SimpleDBEngineException e) {
            logger.info("Rolling back non-existent transaction.");
        }
        value = this.sdb.get("some_other_value", session_id);
        Assert.assertEquals("2", value);
        value = this.sdb.get("some_value", session_id);
        Assert.assertEquals("2", value);
        count = this.sdb.numequalto("1", session_id);
        Assert.assertEquals(0, count);
        count = this.sdb.numequalto("2", session_id);
        Assert.assertEquals(2, count);
    }
}