package com.company.commands;

/**
 * Created by mohanrandhava on 9/29/14.
 *
 * COMMAND INTERFACE
 * -- IMPLEMENTING THE COMMAND DESIGN PATTERN --
 *
 */

public interface Command<R> {
    public void execute();
    public R getResult();
}
