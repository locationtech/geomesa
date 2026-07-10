/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;

import io.trino.spi.security.ConnectorIdentity;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AuthorizationResolver} backed by a properties file mapping Trino
 * identities to authorization tokens:
 *
 * <pre>
 *   user.alice=basic,privileged
 *   group.admins=basic,privileged
 * </pre>
 *
 * A request's authorizations are the union of its {@code user.<name>} entry and
 * the {@code group.<name>} entry for every group it belongs to. Tokens are
 * trimmed; blank tokens and tokens carrying transport-delimiter characters (see
 * {@link AuthTokens}) are dropped with a warning. An unknown identity (no matching user or group
 * key) resolves to an empty set — fail-closed. A missing or unreadable file
 * resolves everyone to empty (fail-closed) and logs a warning.
 *
 * <p>The file is re-read when its last-modified time changes, so edits take
 * effect without a Trino restart.
 */
public final class FileAuthorizationResolver implements AuthorizationResolver {

    private static final Logger LOG = LoggerFactory.getLogger(FileAuthorizationResolver.class);

    private static final String USER_PREFIX  = "user.";
    private static final String GROUP_PREFIX = "group.";

    private final Path file;

    /** Loaded mapping plus the file mtime it came from, published atomically so
     *  a reload can't expose a mapping/mtime pair that don't correspond. */
    private record Snapshot(Properties mapping, long modified) {}

    private volatile Snapshot snapshot = new Snapshot(new Properties(), Long.MIN_VALUE);

    /**
     * Builds a resolver backed by the given properties file.
     *
     * @param file the auth-mapping properties file to read
     */
    public FileAuthorizationResolver(Path file) {
        this.file = file;
    }

    /**
     * Union of the identity's {@code user.<name>} entry and the {@code group.<name>}
     * entry for each of its groups; empty set if unmapped (fail-closed).
     *
     * @param identity the Trino session identity (user and groups)
     * @return authorization tokens mapped to the identity; empty set if none
     */
    @Override
    public Set<String> authorizationsFor(ConnectorIdentity identity) {
        Properties m = current();
        Set<String> auths = new LinkedHashSet<>();
        addTokens(auths, m.getProperty(USER_PREFIX + identity.getUser()));
        for (String group : identity.getGroups()) {
            addTokens(auths, m.getProperty(GROUP_PREFIX + group));
        }
        return auths;
    }

    /** Returns the mapping, reloading if the file's mtime changed. The new
     *  (mapping, mtime) pair is published in a single volatile write so readers
     *  never see a mismatched pair. Failures keep the last snapshot
     *  (fail-closed: empty until a file is first read successfully). */
    private Properties current() {
        Snapshot current = snapshot;
        try {
            long modified = Files.exists(file) ? Files.getLastModifiedTime(file).toMillis() : Long.MIN_VALUE;
            if (modified == current.modified()) {
                return current.mapping();
            }
            Properties next = new Properties();
            if (Files.exists(file)) {
                try (InputStream in = Files.newInputStream(file)) {
                    next.load(in);
                }
            } else {
                LOG.warn("Auth-mapping file not found: " + file
                    + " — all identities resolve to empty authorizations (fail-closed)");
            }
            Snapshot next0 = new Snapshot(next, modified);
            snapshot = next0;  // single atomic publish
            return next0.mapping();
        } catch (IOException e) {
            LOG.warn("Failed to read auth-mapping file " + file + ": " + e.getMessage()
                + " — using last good mapping (or empty)");
            return current.mapping();
        }
    }

    private static void addTokens(Set<String> out, String csv) {
        if (csv == null) return;
        for (String token : csv.split(",")) {
            String t = token.trim();
            if (t.isEmpty()) continue;
            // A token carrying a transport delimiter (e.g. '|', ':', interior space)
            // can't round-trip through the row-filter chain — drop it (narrowing =
            // fail-closed) rather than honor it; see AuthTokens.
            if (!AuthTokens.isValid(t)) {
                LOG.warn("Dropping invalid authorization token '" + t + "' from auth-mapping file "
                    + "(tokens must be printable ASCII without ',', '|', ';', ':', or whitespace)");
                continue;
            }
            out.add(t);
        }
    }
}
