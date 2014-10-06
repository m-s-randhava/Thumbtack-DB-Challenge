package com.company;

import org.apache.log4j.Logger;

import java.util.*;

/**
 * Created by mohanrandhava on 9/30/14.
 *
 *  SIMPLE DATABASE IMPLEMENTATION
 *
 *      COMMANDS
 *
 *      QUERY
 *          SET
 *          GET
 *          NUMEQUALTO
 *          UNSET
 *      TRANSACTION
 *          BEGIN
 *          COMMIT
 *          ROLLBACK
 *
 *      ASSUMPTIONS
 *
 *      1.  Single-user client sessions are only supported
 *          a.  thus, transaction are only meaningful for the user as
 *              the implement the illusion of a scratchpad.  No locking,
 *              etc. is thus in place
 *      2.  Data resides in memory at all times and is not persisted ever
 *
 */

class SimpleDBException extends Exception {
    public SimpleDBException(String message) {
        super(message);
    }
}

public class SimpleDB {
    //  The database is a simple Hashmap mapping name => value.
    private Map<String,String> db = new HashMap<String, String>();

    //  Creating 'master' data structure to keep track of counts of values.
    private SimpleDBValuesDeltas values_deltas = new SimpleDBValuesDeltas(true);

    //  A transaction cache.  Updates to names within a transaction are stored here.
    private Map<UUID,Map<String,String>> ts_db_update_cache_map = new HashMap<UUID, Map<String, String>>();

    //  Maps transaction ids (UUID) to a particular 'non-master' data structure to
    //  keep track of counts of values.  Transaction look to cache before querying
    //  'master.'
    private Map<UUID,SimpleDBValuesDeltas> ts_values_deltas_map = new HashMap<UUID, SimpleDBValuesDeltas>();

    //  Maps session => queue of transactions
    private Map<UUID,Deque<UUID>> session_transaction_registry = new HashMap<UUID,Deque<UUID>>();

    private static Logger logger = Logger.getRootLogger();

    public SimpleDB() {
    }

    public class SimpleDBEngineException extends Exception {
        public SimpleDBEngineException(String message) {
            super(message);
        }
    }

    //  Client registers existence with simple db.
    //  Retrieves UUID as session key.
    public UUID registerClientSession() {
        UUID session_id = UUID.randomUUID();
        session_transaction_registry.put(session_id,new LinkedList<UUID>());
        return session_id;
    }

    //  Begin a transaction for a session by creating:
    //      either:
    //          a fresh empty cache for the first transaction,
    //          or a copy of the previous transaction's cache for nested transaction
    //      and, either:
    //          a fresh empty values_deltas for the first transaction,
    //          or a copy of the previous transaction's values_deltas for nested transaction
    public void beginTransaction(UUID session_id) {
        UUID uuid = UUID.randomUUID();
        Deque<UUID> existing_transactions = session_transaction_registry.get(session_id);

        if (existing_transactions.isEmpty()) {
            ts_db_update_cache_map.put(uuid, new HashMap<String, String>());
            ts_values_deltas_map.put(uuid, new SimpleDBValuesDeltas(false));
        } else {
            UUID last_transaction = existing_transactions.peekLast();
            ts_db_update_cache_map.put(uuid, new HashMap<String, String>(ts_db_update_cache_map.get(last_transaction)));
            ts_values_deltas_map.put(uuid, new SimpleDBValuesDeltas(ts_values_deltas_map.get(last_transaction)));
        }

        existing_transactions.addLast(uuid);
        return;
    }


    //  NUMEQUAL COMMAND
    public int numequalto(String value, UUID session_id) {
        int result = 0;
        boolean inside_transaction = false;
        UUID transaction_id = null;

        Deque<UUID> existing_transactions = session_transaction_registry.get(session_id);

        //  TRANSACTION IN PROGRESS?
        if (!existing_transactions.isEmpty()) {
            inside_transaction = true;
            transaction_id = existing_transactions.peekLast();
        }

        if (inside_transaction) {
            //  If transaction in progress add the current transaction's deltas to 'master'
            SimpleDBValuesDeltas ts_values_deltas;
            ts_values_deltas = ts_values_deltas_map.get(transaction_id);
            result = ts_values_deltas.getValueCount(value) + this.values_deltas.getValueCount(value);

        } else {
            //  Not a transaction, so simply query 'master'
            result = this.values_deltas.getValueCount(value);
        }

        return result;
    }

    //  GET COMMAND
    public String get(String name, UUID session_id) {
        Map<String,String> ts_db_cache;
        String value = null;
        boolean inside_transaction = false;
        UUID transaction_id = null;

        Deque<UUID> existing_transactions = session_transaction_registry.get(session_id);

        //  TRANSACTION IN PROGRESS?
        if (!existing_transactions.isEmpty()) {
            inside_transaction = true;
            transaction_id = existing_transactions.peekLast();
        }

        if (inside_transaction) {
            //  If inside transaction, value is either in cache or
            //  else in 'master'
            ts_db_cache = ts_db_update_cache_map.get(transaction_id);

            if (ts_db_cache.containsKey(name)) {
                value = ts_db_cache.get(name);
            } else if (this.db.containsKey(name)) {
                value = this.db.get(name);
            }
        } else {
            if (this.db.containsKey(name)) {
                value = this.db.get(name);
            }
        }

        return value;
    }

    //  UNSET COMMAND
    public void unset(String name, UUID session_id) {
        Map<String,String> ts_db_cache;
        SimpleDBValuesDeltas ts_values_deltas;
        boolean inside_transaction = false;
        UUID transaction_id = null;

        Deque<UUID> existing_transactions = session_transaction_registry.get(session_id);

        //  TRANSACTION IN PROGRESS?
        if (!existing_transactions.isEmpty()) {
            inside_transaction = true;
            transaction_id = existing_transactions.peekLast();
        }

        if (inside_transaction) {
            //  Get transactional structures, cache and values_deltas
            ts_db_cache = ts_db_update_cache_map.get(transaction_id);
            ts_values_deltas = ts_values_deltas_map.get(transaction_id);

            if (ts_db_cache.containsKey(name)) {
                //  If cache entry has a current entry for 'name'
                //  take stock of old value, decrement, and then
                //  add null for name to later represent an
                //  unset command when committing
                String old_value = ts_db_cache.get(name);
                ts_values_deltas.decrementValueCount(old_value);
                ts_db_cache.put(name, null);
            } else if (this.db.containsKey(name)) {
                //  If 'master' has a current entry for 'name'
                //  take stock of old value, decrement, and then
                //  add new value to preserve consistency
                String old_value = this.db.get(name);
                ts_values_deltas.decrementValueCount(old_value);
                ts_db_cache.put(name, null);
            }
        } else {
            //  Simply remove from 'master' and decrement count of old
            //  value
            if (this.db.containsKey(name)) {
                String old_value = this.db.get(name);
                this.values_deltas.decrementValueCount(old_value);
                this.db.remove(name);
            }
        }

    }

    //  SET COMMAND
    public void set(String name, String value, UUID session_id) {
        Map<String,String> ts_db_cache;
        SimpleDBValuesDeltas ts_values_deltas;
        boolean inside_transaction = false;
        UUID transaction_id = null;

        Deque<UUID> existing_transactions = session_transaction_registry.get(session_id);

        //  TRANSACTION IN PROGRESS?
        if (!existing_transactions.isEmpty()) {
            inside_transaction = true;
            transaction_id = existing_transactions.peekLast();
        }

        if (inside_transaction) {
            //  Get transactional structures, cache and values_deltas
            ts_db_cache = ts_db_update_cache_map.get(transaction_id);
            ts_values_deltas = ts_values_deltas_map.get(transaction_id);

            if (ts_db_cache.containsKey(name)) {
                //  If cache entry has a current entry for 'name'
                //  take stock of old value, decrement, and then
                //  add new value to preserve consistency
                String old_value = ts_db_cache.get(name);
                //  Check if previously 'unset'
                if (old_value != null) {
                    ts_values_deltas.decrementValueCount(old_value);
                }
                ts_db_cache.put(name, value);
                ts_values_deltas.incrementValueCount(value);
            } else  {
                if (this.db.containsKey(name)) {
                    //  If 'master' entry has a current entry for 'name'
                    //  take stock of old value, decrement, and then
                    //  add new value to cache preserve consistency for
                    //  transactional NUMEQUALTO counts
                    String old_value = this.db.get(name);
                    ts_values_deltas.decrementValueCount(old_value);
                    ts_db_cache.put(name, value);
                    ts_values_deltas.incrementValueCount(value);
                } else {
                    //  A new value
                    ts_db_cache.put(name, value);
                    ts_values_deltas.incrementValueCount(value);
                }
            }
        } else {
            if (this.db.containsKey(name)) {
                String old_value = this.db.get(name);
                this.values_deltas.decrementValueCount(old_value);
            } else {
                this.db.put(name, value);
                this.values_deltas.incrementValueCount(value);
            }
        }

    }

    //  COMMIT COMMAND
    public void commitTransaction(UUID session_id) throws SimpleDBEngineException{
        boolean inside_transaction = false;
        UUID transaction_id = null;

        Deque<UUID> existing_transactions = session_transaction_registry.get(session_id);

        //  TRANSACTION IN PROGRESS?
        if (!existing_transactions.isEmpty()) {
            inside_transaction = true;
            transaction_id = existing_transactions.peekLast();
        }

        if (inside_transaction == false) {
            throw new SimpleDBEngineException("No transaction ids presented for commit");
        }

        //  Only necessary to update 'master' with state from last transaction's own values
        //  from cache
        Map<String,String> ts_db_cache = ts_db_update_cache_map.get(transaction_id);
        Iterator it = ts_db_cache.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry<String,String> pairs = (Map.Entry)it.next();

            String name = pairs.getKey();
            String value = pairs.getValue();

            if (value == null) {
                //  If a cache value is null that denote an 'unset' command
                if (!this.db.containsKey(name)) {
                    throw new SimpleDBEngineException("Attempting to remove non-existent element.");
                }
                //  We still have not committed, so transactional structures are still in place,
                //  so we would like to bypass normal transaction checks in normal 'unset' command
                this.unsetOutsideAnyTransaction(name);
            } else {
                //  We still have not committed, so transactional structures are still in place,
                //  so we would like to bypass normal transaction checks in normal 'set' command
                this.setOutsideAnyTransaction(name, value);
            }
        }

        while (!existing_transactions.isEmpty()) {
            UUID completed_transaction_id = existing_transactions.pollFirst();
            ts_db_update_cache_map.remove(completed_transaction_id);
            ts_values_deltas_map.remove(completed_transaction_id);
        }
    }

    //  ROLLBACK COMMAND
    public void rollbackTransaction(UUID session_id) throws SimpleDBEngineException{
        boolean inside_transaction = false;
        UUID most_recent_transaction = null;

        Deque<UUID> existing_transactions = session_transaction_registry.get(session_id);

        //  TRANSACTION IN PROGRESS?
        if (!existing_transactions.isEmpty()) {
            inside_transaction = true;
            most_recent_transaction = existing_transactions.pollLast();
        } else {
            throw new SimpleDBEngineException("No transaction.");
        }

        //  Simply clear transactional structures for the most recent transaction
        //  to rollback state
        if (inside_transaction == true) {
            ts_db_update_cache_map.remove(most_recent_transaction);
            ts_values_deltas_map.remove(most_recent_transaction);
        }

    }

    //  END COMMAND
    public void end(UUID session_id) {
        UUID most_recent_transaction = null;

        Deque<UUID> existing_transactions = session_transaction_registry.get(session_id);

        //  TRANSACTION IN PROGRESS?
        if (!existing_transactions.isEmpty()) {
            //  If transactions were in progress, clear all
            while (!existing_transactions.isEmpty()) {
                most_recent_transaction = existing_transactions.pollLast();
                ts_db_update_cache_map.remove(most_recent_transaction);
                ts_values_deltas_map.remove(most_recent_transaction);
            }
        }
    }

    //  Directly unset database, without transaction checks
    private void unsetOutsideAnyTransaction(String name) {
        if (this.db.containsKey(name)) {
            String old_value = this.db.get(name);
            this.values_deltas.decrementValueCount(old_value);
            this.db.remove(name);
        }
    }

    //  Directly set database, without transaction checks
    private void setOutsideAnyTransaction(String name, String value) {
        if (this.db.containsKey(name)) {
            String old_value = this.db.get(name);
            this.values_deltas.decrementValueCount(old_value);
            this.db.put(name, value);
            this.values_deltas.incrementValueCount(value);
        } else {
            this.db.put(name, value);
            this.values_deltas.incrementValueCount(value);
        }
    }
}
