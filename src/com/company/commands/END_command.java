package com.company.commands;

import com.company.SimpleDB;
import org.apache.log4j.Logger;

import java.util.UUID;

/**
 * Created by mohanrandhava on 9/29/14.
 *
 * END COMMAND
 *
 */
public class END_command implements Command {
    private UUID session_id;
    private SimpleDB sdb_engine;
    private static Logger logger = Logger.getRootLogger();

    public END_command(SimpleDB sdb_engine, UUID session_id) {
        this.session_id = session_id;
        this.sdb_engine = sdb_engine;
    }

    @Override
    public void execute() {
        this.sdb_engine.end(this.session_id);
    }

    @Override
    public Object getResult() {
        return null;
    }
}
