package com.company;

/**
 * Created by mohanrandhava on 10/6/14.
 */

import org.junit.runner.JUnitCore;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ SimpleDBValuesDeltasTest.class, SimpleDBTest.class, SimpleDBClientTest.class, SimpleDBChallengeTest.class })
public class SimpleDBTestSuite {
    public static void main(String[] args) throws Exception {
        JUnitCore.main(
                "com.company.SimpleDBTestSuite");
    }
}
