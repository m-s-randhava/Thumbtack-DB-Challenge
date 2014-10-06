package com.company.commands;

import com.company.SimpleDB;
import org.apache.log4j.Logger;

import java.util.UUID;

/**
 * Created by mohanrandhava on 9/29/14.
 *
 * SET COMMAND
 *
 */
public class SET_command implements Command {
    private String name;
    private String value;
    private SimpleDB sdb_engine;
    private UUID session_id;
    private static Logger logger = Logger.getRootLogger();

    public SET_command(String name, String value, SimpleDB sdb_engine, UUID session_id) {
        this.name = name;
        this.value = value;
        this.sdb_engine = sdb_engine;
        this.session_id = session_id;
    }

    @Override
    public void execute() {
        this.sdb_engine.set(this.name, this.value, this.session_id);
    }

    @Override
    public Object getResult() {
        return null;
    }

}
