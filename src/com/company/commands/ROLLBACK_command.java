package com.company.commands;

import com.company.SimpleDB;
import org.apache.log4j.Logger;

import java.util.UUID;

/**
 * Created by mohanrandhava on 9/29/14.
 *
 * ROLLBACK COMMAND
 *
 */
public class ROLLBACK_command implements Command<String> {
    private UUID session_id;
    private SimpleDB sdb_engine;
    private static Logger logger = Logger.getRootLogger();
    private String result = null;

    public ROLLBACK_command(SimpleDB sdb_engine, UUID session_id) {
        this.sdb_engine = sdb_engine;
        this.session_id = session_id;
    }

    @Override
    public void execute() {
        try {
            this.sdb_engine.rollbackTransaction(this.session_id);
        } catch (SimpleDB.SimpleDBEngineException e) {
            logger.info("Nothing to rollback.");
            this.result = "NO TRANSACTION";
        }
    }

    @Override
    public String getResult() {
        return this.result;
    }
}
