/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.apache.commons.dbcp2.DelegatingConnection;
import org.apache.commons.pool2.KeyedObjectPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A borrowed {@link Connection} that returns itself to its {@link ConnectionPool} on
 * {@link #close()} (exactly once) instead of closing, preserving callers'
 * try-with-resources semantics. Any other use after close throws — by then the underlying
 * connection may already be serving another request with the same auths.
 *
 * <p>Extends dbcp2's {@link DelegatingConnection}: {@link #passivate()} closes any
 * statements this wrapper handed out, so a straggler statement can't touch the
 * underlying connection after it has been recycled to another consumer.
 */
final class PooledConnection extends DelegatingConnection<Connection> {

    private final KeyedObjectPool<String, Connection> pool;
    private final String key;
    private final AtomicBoolean returned = new AtomicBoolean(false);

    PooledConnection(KeyedObjectPool<String, Connection> pool, String key, Connection real) {
        super(real);
        this.pool = pool;
        this.key = key;
    }

    /** Returns the underlying connection to the pool; a closed pool destroys it
     *  instead. The wrapper then reports closed and the inherited guards reject
     *  further use. */
    @Override
    public void close() throws SQLException {
        if (returned.compareAndSet(false, true)) {
            try {
                passivate();
            } finally {
                setClosedInternal(true);
                try {
                    pool.returnObject(key, getDelegateInternal());
                } catch (SQLException | RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new SQLException("Failed to return connection to pool", e);
                }
            }
        }
    }
}
