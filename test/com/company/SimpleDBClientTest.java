package com.company;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SimpleDBClientTest {
    SimpleDBClient sdb_us;

    @Before
    public void createSimpleDBInstance() {
        this.sdb_us = new SimpleDBClient(new SimpleDB());
    }

    @After
    public void destroySimpleDBInstance() {
        this.sdb_us = null;
    }

    @Test
    public void testDispatchCommand_ValidCommands() throws Exception {
        String input = "SET a 10";
        String[] input_components = input.split("\\s+");
        Assert.assertNotNull(this.sdb_us.dispatchCommand(input_components));

        input = "BEGIN";
        input_components = input.split("\\s+");
        Assert.assertNotNull(this.sdb_us.dispatchCommand(input_components));

        input = "NUMEQUALTO 10";
        input_components = input.split("\\s+");
        Assert.assertNotNull(this.sdb_us.dispatchCommand(input_components));

        input = "GET a";
        input_components = input.split("\\s+");
        Assert.assertNotNull(this.sdb_us.dispatchCommand(input_components));

        input = "UNSET a";
        input_components = input.split("\\s+");
        Assert.assertNotNull(this.sdb_us.dispatchCommand(input_components));

        input = "ROLLBACK";
        input_components = input.split("\\s+");
        Assert.assertNotNull(this.sdb_us.dispatchCommand(input_components));

        input = "NUMEQUALTO       10";
        input_components = input.split("\\s+");
        Assert.assertNotNull(this.sdb_us.dispatchCommand(input_components));

        input = "COMMIT";
        input_components = input.split("\\s+");
        Assert.assertNotNull(this.sdb_us.dispatchCommand(input_components));

        input = "END";
        input_components = input.split("\\s+");
        Assert.assertNotNull(this.sdb_us.dispatchCommand(input_components));
    }

    @Test (expected = InvalidCommandException.class)
    public void testDispatchCommand_InValidCommands_BEGIN () throws Exception {
        String input = "BEGINNING";
        String[] input_components = input.split("\\s+");
        Assert.assertEquals(false, this.sdb_us.dispatchCommand(input_components));
    }

    @Test (expected = InvalidCommandException.class)
    public void testDispatchCommand_InValidCommands_SET () throws Exception {
        String input = "SET a";
        String[] input_components = input.split("\\s+");
        Assert.assertEquals(false, this.sdb_us.dispatchCommand(input_components));
    }

    @Test (expected = InvalidCommandException.class)
    public void testDispatchCommand_InValidCommands_GET () throws Exception {
        String input = "GET a b";
        String[] input_components = input.split("\\s+");
        Assert.assertEquals(true, this.sdb_us.dispatchCommand(input_components));
    }

    @Test (expected = InvalidCommandException.class)
    public void testDispatchCommand_InValidCommands_END () throws Exception {
        String input = "ENDING";
        String[] input_components = input.split("\\s+");
        Result result = this.sdb_us.dispatchCommand(input_components);
        boolean isEnd = result.isEnd();
        Assert.assertEquals(true, isEnd);
    }

    @Test (expected = InvalidCommandException.class)
    public void testDispatchCommand_InValidCommands_NUMEQUALTO () throws Exception {
        String input = "NUMEQUALTO a    b";
        String[] input_components = input.split("\\s+");
        Assert.assertEquals(true, this.sdb_us.dispatchCommand(input_components));
    }

    @Test (expected = InvalidCommandException.class)
    public void testDispatchCommand_InValidCommands_ROLLBACK () throws Exception {
        String input = "ROLLBACK a";
        String[] input_components = input.split("\\s+");
        Assert.assertEquals(true, this.sdb_us.dispatchCommand(input_components));
    }

    @Test (expected = InvalidCommandException.class)
    public void testDispatchCommand_InValidCommands_UNSET () throws Exception {
        String input = "UNSET";
        String[] input_components = input.split("\\s+");
        Assert.assertEquals(true, this.sdb_us.dispatchCommand(input_components));
    }

    @Test (expected = InvalidCommandException.class)
    public void testDispatchCommand_InValidCommands_COMMIT () throws Exception {
        String input = "COMMIT a";
        String[] input_components = input.split("\\s+");
        Assert.assertEquals(true, this.sdb_us.dispatchCommand(input_components));
    }

}