/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

/**
 * JDBC connection pool keyed by the caller's normalized authorization set.
 *
 * <p>Borrowed connections are wrapped in a {@link PooledConnection} so
 * {@link Connection#close()} returns them to the pool instead of closing them; callers
 * keep ordinary try-with-resources semantics. Any other use after close throws.
 * {@link #close()} closes all idle connections; in-flight ones are destroyed as
 * their holders return them.
 */
final class ConnectionPool implements AutoCloseable {

    /** Idle connections kept per auth set. Excess returns are closed rather than pooled. */
    private static final int MAX_IDLE_PER_KEY = 10;

    /** How long an idle connection may sit unused before the evictor closes it. */
    private static final Duration IDLE_TTL = Duration.ofMinutes(5);

    /** How often the background evictor sweeps for expired idle connections. */
    private static final Duration EVICTION_INTERVAL = Duration.ofMinutes(1);

    /** Opens a real connection for a normalized (sorted, deduped) auth set. */
    interface Opener {
        Connection open(List<String> normalizedAuths) throws SQLException;
    }

    private final GenericKeyedObjectPool<String, Connection> pool;

    ConnectionPool(Opener opener) {
        GenericKeyedObjectPoolConfig<Connection> config = new GenericKeyedObjectPoolConfig<>();
        config.setMaxTotalPerKey(-1);
        config.setMaxTotal(-1);
        config.setBlockWhenExhausted(false);
        config.setMaxIdlePerKey(MAX_IDLE_PER_KEY);
        config.setMinEvictableIdleDuration(IDLE_TTL);
        config.setTimeBetweenEvictionRuns(EVICTION_INTERVAL);
        config.setTestOnBorrow(true);
        config.setJmxEnabled(false);
        this.pool = new GenericKeyedObjectPool<>(new Factory(opener), config);
    }

    /** Sorted, deduped, validated copy of the auths; empty for null/empty input. */
    static List<String> normalize(List<String> auths) {
        if (auths == null || auths.isEmpty()) {
            return Collections.emptyList();
        }
        AuthTokens.validate(auths);
        return List.copyOf(new TreeSet<>(auths));
    }

    /** Returns a connection whose extra credential carries exactly the given auths. */
    Connection borrow(List<String> auths) throws SQLException {
        String key = String.join(",", normalize(auths));
        Connection conn;
        try {
            conn = pool.borrowObject(key);
        } catch (SQLException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Failed to obtain pooled connection", e);
        }
        return new PooledConnection(pool, key, conn);
    }

    /** Closes all idle connections; subsequent returns are destroyed. In-flight
     *  connections are unaffected until their consumers close them. */
    @Override
    public void close() {
        pool.close();
    }

    GenericKeyedObjectPool<String, Connection> delegate() {
        return pool;
    }

    /** Opens/validates/destroys pooled connections. */
    private static final class Factory extends BaseKeyedPooledObjectFactory<String, Connection> {

        private final Opener opener;

        private Factory(Opener opener) {
            this.opener = opener;
        }

        @Override
        public Connection create(String key) throws SQLException {
            List<String> auths = key.isEmpty() ? List.of() : Arrays.asList(key.split(","));
            return opener.open(auths);
        }

        @Override
        public PooledObject<Connection> wrap(Connection conn) {
            return new DefaultPooledObject<>(conn);
        }

        @Override
        public boolean validateObject(String key, PooledObject<Connection> p) {
            try {
                return !p.getObject().isClosed();
            } catch (SQLException e) {
                return false;
            }
        }

        @Override
        public void destroyObject(String key, PooledObject<Connection> p) throws SQLException {
            p.getObject().close();
        }
    }
}
