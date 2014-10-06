package com.company.commands;

import com.company.SimpleDB;
import org.apache.log4j.Logger;

import java.util.UUID;

/**
 * Created by mohanrandhava on 9/29/14.
 *
 * GET COMMAND
 *
 */
public class GET_command implements Command<String> {
    private String name;
    private SimpleDB sdb_engine;
    private String result;
    private UUID session_id;
    private static Logger logger = Logger.getRootLogger();

    public GET_command(String name, SimpleDB sdb_engine, UUID session_id) {
        this.name = name;
        this.sdb_engine = sdb_engine;
        this.session_id = session_id;
    }

    @Override
    public void execute() {
        this.result = this.sdb_engine.get(this.name, this.session_id);
        if (this.result == null) {
            this.result = "NULL";
        }
    }

    @Override
    public String getResult() {
        return this.result;
    }
}