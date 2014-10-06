package com.company;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mohanrandhava on 9/30/14.
 *
 *
 *  Encapsulates a simple Hashmap to map name ==> count in simple database
 *
 *  OPERATIONS:
 *      getValueCount
 *      decrementValueCount :   If not 'master' recording structure, but for transaction,
 *                              then can decrement into negative values
 *      incrementValueCount
 *      insertValue
 */

class SimpleDBValuesDeltasException extends Exception {
    public SimpleDBValuesDeltasException(String message) {
        super(message);
    }
}

public class SimpleDBValuesDeltas {
    //  A Hashmap mapping from name => value
    private Map<String,Integer> values_counter;
    //  Denote whether this the only 'master' recording structure for value count or
    //  is one used by a transaction
    private boolean isUniversalValuesDeltas = false;

    public SimpleDBValuesDeltas(boolean isUniversalValuesDeltas) {
        this.isUniversalValuesDeltas = isUniversalValuesDeltas;
        this.values_counter = new HashMap<String,Integer>();
    }

    public SimpleDBValuesDeltas(SimpleDBValuesDeltas another_values_counter) {
        this.values_counter = new HashMap<String,Integer>(another_values_counter.values_counter);
    }

    //  Get count based upon value if value is present, else return 0
    public Integer getValueCount(String value) {
        Integer count = 0;

        if (values_counter.get(value) != null) {
            count = values_counter.get(value);
        }

        return count;
    }

    //  Insert a value
    public boolean insertValue(String value, Integer count) {
        boolean inserted = false;

        if (values_counter.get(value) == null) {
            values_counter.put(value, count);
            inserted = true;
        }

        return inserted;
    }

    //  Tracks changes in count of a value
    //  Can be negative if used by transaction
    public Integer decrementValueCount(String value) {
        Integer count = null;

        if (values_counter.get(value) != null) {
            count = values_counter.get(value);
            count--;
            if (count == 0 && this.isUniversalValuesDeltas) {
                values_counter.remove(value);
            } else {
                values_counter.put(value,count);
            }
        } else if (!this.isUniversalValuesDeltas) {
            count = -1;
            values_counter.put(value,-1);
        }

        return count;
    }

    //  Tracks changes in count of a value
    public Integer incrementValueCount(String value) {
        Integer count;

        if (values_counter.get(value) != null) {
            count = values_counter.get(value);
            count++;
            values_counter.put(value,count);
        } else {
            count = 1;
            values_counter.put(value,1);
        }

        return count;
    }

}
