package com.company;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SimpleDBValuesDeltasTest {
    private SimpleDBValuesDeltas sdb_vc = null;
    private static Logger logger = Logger.getRootLogger();

    public void setUp() {
    }

    @Before
    public void createSimpleDB_ValuesCounter() {
        this.sdb_vc = new SimpleDBValuesDeltas(true);
    }

    @After
    public void destroySimpleDBInstance() {
        this.sdb_vc = null;
    }


    @Test
    public void testGetValueCountNonEntry() throws Exception {
        Integer count = this.sdb_vc.getValueCount("non_present_entry");
        Assert.assertEquals(new Integer(0), count);
    }

    @Test
    public void testInsertValue() throws Exception {
        boolean success = this.sdb_vc.insertValue("new_present_value", 1);
        Assert.assertEquals(true, success);
        Integer count = this.sdb_vc.getValueCount("new_present_value");
        Assert.assertEquals(new Integer(1), count);
    }

    @Test
    public void testDecrementValueCount() throws Exception {
        boolean success = this.sdb_vc.insertValue("new_present_value", 1);
        Integer count = this.sdb_vc.decrementValueCount("new_present_value");
        Assert.assertEquals(new Integer(0), count);
    }

    @Test
    public void testIncrementValueCount() throws Exception {
        boolean success = this.sdb_vc.insertValue("new_present_value", 1);
        Integer count = this.sdb_vc.incrementValueCount("new_present_value");
        Assert.assertEquals(new Integer(2), count);
        count = this.sdb_vc.incrementValueCount("new_present_value");
        Assert.assertEquals(new Integer(3), count);
    }

}