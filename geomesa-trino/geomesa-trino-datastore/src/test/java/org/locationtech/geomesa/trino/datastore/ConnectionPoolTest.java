/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectionPoolTest {

    /** Records every open: the auths requested and a stub connection whose only real
     *  behavior is a local closed flag. */
    private static final class RecordingOpener implements ConnectionPool.Opener {
        final List<List<String>> openedWith = new ArrayList<>();
        final List<StubConnection> opened = new ArrayList<>();

        @Override
        public Connection open(List<String> normalizedAuths) {
            StubConnection stub = new StubConnection();
            openedWith.add(normalizedAuths);
            opened.add(stub);
            return stub.conn;
        }
    }

    /** Stub {@link Connection}: close()/isClosed() work, everything else returns null. */
    private static final class StubConnection {
        final AtomicBoolean closed = new AtomicBoolean(false);
        final Connection conn = (Connection) Proxy.newProxyInstance(
            Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
            (proxy, method, args) -> switch (method.getName()) {
                case "close"    -> { closed.set(true); yield null; }
                case "isClosed" -> closed.get();
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals"   -> proxy == args[0];
                case "toString" -> "stub";
                default         -> null;
            });
    }

    @Test
    void sameAuthSetReusesConnection() throws SQLException {
        RecordingOpener opener = new RecordingOpener();
        ConnectionPool pool = new ConnectionPool(opener);
        pool.borrow(List.of("a", "b")).close();
        pool.borrow(List.of("a", "b")).close();
        assertThat(opener.opened).hasSize(1);
    }

    @Test
    void orderAndDuplicatesDoNotFragmentThePool() throws SQLException {
        RecordingOpener opener = new RecordingOpener();
        ConnectionPool pool = new ConnectionPool(opener);
        pool.borrow(List.of("b", "a")).close();
        pool.borrow(List.of("a", "b", "a")).close();
        assertThat(opener.opened).hasSize(1);
        // the opener sees the normalized set, so the credential matches the pool key
        assertThat(opener.openedWith.get(0)).containsExactly("a", "b");
    }

    @Test
    void differentAuthSetsNeverShareConnections() throws SQLException {
        RecordingOpener opener = new RecordingOpener();
        ConnectionPool pool = new ConnectionPool(opener);
        pool.borrow(List.of("a")).close();
        pool.borrow(List.of("a", "b")).close();  // superset: distinct connection
        pool.borrow(List.of()).close();          // no auths:  distinct connection
        assertThat(opener.opened).hasSize(3);
        // each set reuses its own
        pool.borrow(List.of("a")).close();
        pool.borrow(List.of("b", "a")).close();
        pool.borrow(null).close();               // null normalizes like empty
        assertThat(opener.opened).hasSize(3);
    }

    @Test
    void concurrentBorrowsOfSameKeyGetDistinctConnections() throws SQLException {
        RecordingOpener opener = new RecordingOpener();
        ConnectionPool pool = new ConnectionPool(opener);
        Connection first = pool.borrow(List.of("a"));
        Connection second = pool.borrow(List.of("a"));  // first not yet returned
        assertThat(opener.opened).hasSize(2);
        first.close();
        second.close();
        pool.borrow(List.of("a")).close();
        assertThat(opener.opened).hasSize(2);  // both reusable after return
    }

    @Test
    void perKeyIdleCapClosesExcessReturns() throws SQLException {
        RecordingOpener opener = new RecordingOpener();
        ConnectionPool pool = new ConnectionPool(opener);
        List<Connection> borrowed = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            borrowed.add(pool.borrow(List.of("a")));
        }
        for (Connection c : borrowed) {
            c.close();
        }
        long stillOpen = opener.opened.stream().filter(s -> !s.closed.get()).count();
        assertThat(stillOpen).isEqualTo(10);  // MAX_IDLE_PER_KEY
    }

    @Test
    void configuredForUnboundedNonBlockingBorrowsWithIdleTtl() {
        // The non-default commons-pool2 config is load-bearing: with the defaults
        // (maxTotalPerKey=8, blockWhenExhausted=true) a burst of same-auth queries
        // would QUEUE — a latency cliff the old connection-per-query code never had.
        var delegate = new ConnectionPool(new RecordingOpener()).delegate();
        assertThat(delegate.getMaxTotalPerKey()).isEqualTo(-1);
        assertThat(delegate.getMaxTotal()).isEqualTo(-1);
        assertThat(delegate.getBlockWhenExhausted()).isFalse();
        assertThat(delegate.getMaxIdlePerKey()).isEqualTo(10);
        assertThat(delegate.getMinEvictableIdleDuration()).isEqualTo(java.time.Duration.ofMinutes(5));
        assertThat(delegate.getDurationBetweenEvictionRuns()).isPositive();  // evictor enabled
        assertThat(delegate.getTestOnBorrow()).isTrue();  // discard connections closed underneath
    }

    @Test
    void poolCloseClosesIdleAndStopsPooling() throws SQLException {
        RecordingOpener opener = new RecordingOpener();
        ConnectionPool pool = new ConnectionPool(opener);
        Connection inFlight = pool.borrow(List.of("a"));
        pool.borrow(List.of("b")).close();  // idle at pool close
        pool.close();
        assertThat(opener.opened.get(1).closed).isTrue();       // idle: closed by pool
        assertThat(opener.opened.get(0).closed).isFalse();      // in-flight: untouched
        inFlight.close();
        assertThat(opener.opened.get(0).closed).isTrue();       // return after close = real close
    }

    @Test
    void doubleCloseReturnsToPoolOnlyOnce() throws SQLException {
        RecordingOpener opener = new RecordingOpener();
        ConnectionPool pool = new ConnectionPool(opener);
        Connection conn = pool.borrow(List.of("a"));
        conn.close();
        conn.close();  // no double-pooling of the same underlying connection
        Connection reuse1 = pool.borrow(List.of("a"));
        Connection reuse2 = pool.borrow(List.of("a"));
        assertThat(opener.opened).hasSize(2);  // second borrow required a fresh open
        reuse1.close();
        reuse2.close();
    }

    @Test
    void usingConnectionAfterCloseThrows() throws SQLException {
        ConnectionPool pool = new ConnectionPool(new RecordingOpener());
        Connection conn = pool.borrow(List.of("a"));
        conn.close();
        assertThat(conn.isClosed()).isTrue();
        // the underlying connection may already be serving another request — reject use
        assertThatThrownBy(conn::createStatement).isInstanceOf(SQLException.class);
    }

    @Test
    void connectionClosedUnderneathIsNotHandedOut() throws SQLException {
        RecordingOpener opener = new RecordingOpener();
        ConnectionPool pool = new ConnectionPool(opener);
        pool.borrow(List.of("a")).close();
        opener.opened.get(0).closed.set(true);  // dies while idle (e.g. server-side)
        pool.borrow(List.of("a")).close();
        assertThat(opener.opened).hasSize(2);
    }

    @Test
    void invalidTokensRejectedBeforeKeying() {
        ConnectionPool pool = new ConnectionPool(new RecordingOpener());
        // ',' joins tokens in the pool key (and the is_visible SQL literal) — a token
        // containing it would collide with a two-token set that was never issued
        assertThatThrownBy(() -> pool.borrow(List.of("a,b")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("authorization token");
    }

    @Test
    void normalizeSortsDedupesAndCopies() {
        assertThat(ConnectionPool.normalize(null)).isEmpty();
        assertThat(ConnectionPool.normalize(List.of())).isEmpty();
        assertThat(ConnectionPool.normalize(List.of("b", "a", "b"))).containsExactly("a", "b");
    }
}
