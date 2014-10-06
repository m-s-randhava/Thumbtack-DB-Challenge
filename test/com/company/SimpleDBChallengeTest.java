package com.company;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SimpleDBChallengeTest {
    private static Logger logger = Logger.getRootLogger();
    private SimpleDatabaseChallenge sdc;

    @Before
    public void setUp() throws Exception {
        logger.info("... creating simple db instance anew ...");
        this.sdc = new SimpleDatabaseChallenge();
    }

    @After
    public void tearDown() throws Exception {
        logger.info("... destroying simple db instance ...");
        this.sdc = null;
    }

    @Test
    public void testProcess_input_1() throws Exception {
        String c1 = "SET ex 10";
        String c2 = "GET ex";
        String c3 = "UNSET ex";
        String c4 = "GET ex";
        String c5 = "END";
        Result result;

        result = this.sdc.process_input(c1);
        result = this.sdc.process_input(c2);
        Assert.assertEquals("10", result.getResult());
        result = this.sdc.process_input(c3);
        result = this.sdc.process_input(c4);
        Assert.assertEquals("NULL", result.getResult());
        result = this.sdc.process_input(c5);
        Assert.assertTrue(result.isEnd());
    }

    @Test
    public void testProcess_input_2() throws Exception {
        String c1 = "SET a 10";
        String c2 = "SET b 10";
        String c3 = "NUMEQUALTO 10";
        String c4 = "NUMEQUALTO 20";
        String c5 = "SET b 30";
        String c6 = "NUMEQUALTO 10";
        String c7 = "END";
        Result result;

        result = this.sdc.process_input(c1);
        result = this.sdc.process_input(c2);
        result = this.sdc.process_input(c3);
        Assert.assertEquals(2, result.getResult());
        result = this.sdc.process_input(c4);
        Assert.assertEquals(0, result.getResult());
        result = this.sdc.process_input(c5);
        result = this.sdc.process_input(c6);
        Assert.assertEquals(1, result.getResult());
        result = this.sdc.process_input(c7);
        Assert.assertTrue(result.isEnd());
    }

    @Test
    public void testProcess_input_3() throws Exception {
        String c1 = "BEGIN";
        String c2 = "SET a 10";
        String c3 = "GET a";
        String c4 = "BEGIN";
        String c5 = "SET a 20";
        String c6 = "GET a";
        String c7 = "ROLLBACK";
        String c8 = "GET a";
        String c9 = "ROLLBACK";
        String c10 = "GET a";
        String c11 = "END";
        Result result;

        result = this.sdc.process_input(c1);
        result = this.sdc.process_input(c2);
        result = this.sdc.process_input(c3);
        Assert.assertEquals("10", result.getResult());
        result = this.sdc.process_input(c4);
        result = this.sdc.process_input(c5);
        result = this.sdc.process_input(c6);
        Assert.assertEquals("20", result.getResult());
        result = this.sdc.process_input(c7);
        result = this.sdc.process_input(c8);
        Assert.assertEquals("10", result.getResult());
        result = this.sdc.process_input(c9);
        result = this.sdc.process_input(c10);
        Assert.assertEquals("NULL", result.getResult());
        result = this.sdc.process_input(c11);
        Assert.assertTrue(result.isEnd());
    }

    @Test
    public void testProcess_input_4() throws Exception {
        String c1 = "BEGIN";
        String c2 = "SET a 30";
        String c3 = "BEGIN";
        String c4 = "SET a 40";
        String c5 = "COMMIT";
        String c6 = "GET a";
        String c7 = "ROLLBACK";
        String c8 = "END";
        Result result;

        result = this.sdc.process_input(c1);
        result = this.sdc.process_input(c2);
        result = this.sdc.process_input(c3);
        result = this.sdc.process_input(c4);
        result = this.sdc.process_input(c5);
        result = this.sdc.process_input(c6);
        Assert.assertEquals("40", result.getResult());
        result = this.sdc.process_input(c7);
        result = this.sdc.process_input(c8);
        Assert.assertTrue(result.isEnd());
    }

    @Test
    public void testProcess_input_5() throws Exception {
        String c1 = "SET a 50";
        String c2 = "BEGIN";
        String c3 = "GET a";
        String c4 = "SET a 60";
        String c5 = "BEGIN";
        String c6 = "UNSET a";
        String c7 = "GET a";
        String c8 = "ROLLBACK";
        String c9 = "GET a";
        String c10 = "COMMIT";
        String c11 = "GET a";
        String c12 = "END";
        Result result;

        result = this.sdc.process_input(c1);
        result = this.sdc.process_input(c2);
        result = this.sdc.process_input(c3);
        Assert.assertEquals("50", result.getResult());
        result = this.sdc.process_input(c4);
        result = this.sdc.process_input(c5);
        result = this.sdc.process_input(c6);
        result = this.sdc.process_input(c7);
        Assert.assertEquals("NULL", result.getResult());
        result = this.sdc.process_input(c8);
        result = this.sdc.process_input(c9);
        Assert.assertEquals("60", result.getResult());
        result = this.sdc.process_input(c10);
        result = this.sdc.process_input(c11);
        Assert.assertEquals("60", result.getResult());
        result = this.sdc.process_input(c12);
        Assert.assertTrue(result.isEnd());
    }

    @Test
    public void testProcess_input_6() throws Exception {
        String c1 = "SET a 10";
        String c2 = "BEGIN";
        String c3 = "NUMEQUALTO 10";
        String c4 = "BEGIN";
        String c5 = "UNSET a";
        String c6 = "NUMEQUALTO 10";
        String c7 = "ROLLBACK";
        String c8 = "NUMEQUALTO 10";
        String c9 = "COMMIT";
        String c10 = "GET a";
        String c11 = "NUMEQUALTO 10";
        String c12 = "END";
        Result result;

        result = this.sdc.process_input(c1);
        result = this.sdc.process_input(c2);
        result = this.sdc.process_input(c3);
        Assert.assertEquals(1, result.getResult());
        result = this.sdc.process_input(c4);
        result = this.sdc.process_input(c5);
        result = this.sdc.process_input(c6);
        Assert.assertEquals(0, result.getResult());
        result = this.sdc.process_input(c7);
        result = this.sdc.process_input(c8);
        Assert.assertEquals(1, result.getResult());
        result = this.sdc.process_input(c9);
        result = this.sdc.process_input(c10);
        Assert.assertEquals("10", result.getResult());
        result = this.sdc.process_input(c11);
        Assert.assertEquals(1, result.getResult());
        result = this.sdc.process_input(c12);
        Assert.assertTrue(result.isEnd());
    }
}