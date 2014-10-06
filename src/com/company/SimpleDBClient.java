package com.company;

import com.company.commands.*;
import org.apache.log4j.Logger;

import java.util.UUID;

/**
 * Created by mohanrandhava on 9/30/14.
 */
class InvalidCommandException extends Exception {
    public InvalidCommandException(String message) {
        super(message);
    }
}

public class SimpleDBClient {
    private SimpleDB sdb;
    private UUID session_id;
    private static Logger logger = Logger.getRootLogger();

    public SimpleDBClient(SimpleDB sdb) {
        this.sdb = sdb;
        this.session_id = this.sdb.registerClientSession();
    }

    public Result dispatchCommand(String[] entry) throws InvalidCommandException {
        Result result = null;
        boolean valid = validateCommandEntry(entry);
        Command command;

        if (!valid) {
            logger.info("Invalid command");
            throw new InvalidCommandException("Invalid command format");
        }

        command = createCommand(entry, this.sdb, this.session_id);

        command.execute();

        if (command instanceof END_command) {
            result = new Result(command.getResult(),true);
        } else {
            result = new Result(command.getResult(),false);
        }

        return result;
    }

    private Command createCommand(String[] entry, SimpleDB sdb, UUID session_id) {
        Command command = null;
        String command_type = entry[0].toUpperCase();

        switch(command_type) {
            case "BEGIN":
                command = new BEGIN_command(sdb, session_id);
                break;
            case "ROLLBACK":
                command = new ROLLBACK_command(sdb,session_id);
                break;
            case "COMMIT":
                command = new COMMIT_command(sdb, session_id);
                break;
            case "END":
                command = new END_command(sdb, session_id);
                break;
            case "SET":
                command = new SET_command(entry[1], entry[2], sdb, session_id);
                break;
            case "UNSET":
                command = new UNSET_command(entry[1], sdb, session_id);
                break;
            case "GET":
                command = new GET_command(entry[1], sdb, session_id);
                break;
            case "NUMEQUALTO":
                command = new NUMEQUALTO_command(entry[1], sdb, session_id);
                break;
            default:
                break;
        }

        return command;
    }

    private boolean validateCommandEntry(String[] entry) {
        boolean valid = true;

        if (entry.length == 0) {
            valid = false;
            return valid;
        }
        String command = entry[0].toUpperCase();

        switch(command) {
            case "BEGIN":
                if (entry.length != 1) {
                    valid = false;
                }
                break;
            case "ROLLBACK":
                if (entry.length != 1) {
                    valid = false;
                }
                break;
            case "COMMIT":
                if (entry.length != 1) {
                    valid = false;
                }
                break;
            case "END":
                if (entry.length != 1) {
                    valid = false;
                }
                break;
            case "SET":
                if (entry.length != 3) {
                    valid = false;
                }
                break;
            case "UNSET":
                if (entry.length != 2) {
                    valid = false;
                }
                break;
            case "GET":
                if (entry.length != 2) {
                    valid = false;
                }
                break;
            case "NUMEQUALTO":
                if (entry.length != 2) {
                    valid = false;
                }
                break;
            default:
                valid = false;
                break;
        }

        return valid;
    }
}
