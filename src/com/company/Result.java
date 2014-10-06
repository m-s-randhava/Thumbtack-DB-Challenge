package com.company;

/**
 * Created by mohanrandhava on 10/6/14.
 */
public class Result<T> {
    private T result;
    private boolean end;

    public Result(T result, boolean end) {
        this.result = result;
        this.end = end;
    }

    public T getResult() {
        return result;
    }

    public boolean isEnd() {
        return end;
    }
}
