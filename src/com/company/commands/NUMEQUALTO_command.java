package com.company.commands;

import com.company.SimpleDB;
import org.apache.log4j.Logger;

import java.util.UUID;

/**
 * Created by mohanrandhava on 9/29/14.
 *
 * NUMEQUALTO COMMAND
 *
 */
public class NUMEQUALTO_command implements Command<Integer> {
    private String value;
    private SimpleDB sdb_engine;
    private Integer result;
    private UUID session_id;
    private static Logger logger = Logger.getRootLogger();

    public NUMEQUALTO_command(String value, SimpleDB sdb_engine, UUID session_id) {
        this.value = value;
        this.sdb_engine = sdb_engine;
        this.session_id = session_id;
    }

    @Override
    public void execute() {
        this.result = this.sdb_engine.numequalto(this.value, this.session_id);
    }

    @Override
    public Integer getResult() {
        return this.result;
    }
}
