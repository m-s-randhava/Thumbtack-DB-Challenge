package com.company;
import org.apache.log4j.Logger;

import java.util.Scanner;

public class SimpleDatabaseChallenge {
    private static Logger logger = Logger.getRootLogger();
    //  Database instance
    private SimpleDB db;
    //  Database client
    private SimpleDBClient db_user_session;

    public SimpleDatabaseChallenge() {
        this.db = new SimpleDB();
        this.db_user_session = new SimpleDBClient(db);
    }

    public Result process_input(String input) {
        Result result = null;
        String[] input_components = input.split("\\s+");

        for (String input_component : input_components) {
            input_component.trim();
        }

        try {
            result = db_user_session.dispatchCommand(input_components);
        } catch (InvalidCommandException e) {
            logger.info(e.getMessage());
        }

        return result;
    }

    public static void main(String[] args) {
        String input;
        //  While true, continue user shell read operations
        boolean continue_processing = true;
        //  Harness to run database and client session
        SimpleDatabaseChallenge sdc = new SimpleDatabaseChallenge();

        //  Scan user input and send to shell
        Scanner scanIn = new Scanner(System.in);

        while (continue_processing) {
            input = scanIn.nextLine();

            if (input.isEmpty()) {
                continue;
            }

            //  Ask shell to process input line
            Result result = sdc.process_input(input);

            //  Invalid command entered
            if (result == null) {
                continue;
            }

            //  If process response was from END command, end session
            if (result != null && result.isEnd()) {
                continue_processing = false;
            } else if (result.getResult() != null) {
                System.out.println(result.getResult());
            }
        }

        scanIn.close();
        System.out.println("Goodbye");
        System.exit(0);
    }
}
