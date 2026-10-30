/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.List;
import java.util.Objects;

/** Authoritative visibility filtering carried to workers, optionally wrapping a spatial filter.
 * Each auth literal is evaluated separately: intersecting token sets would change OR semantics. */
public record VisibilityTableHandle(
        ConnectorTableHandle delegate, IcebergColumnHandle visibilityColumn, List<String> auths) implements ConnectorTableHandle {
    @JsonCreator
    public VisibilityTableHandle(@JsonProperty("delegate") ConnectorTableHandle delegate,
            @JsonProperty("visibilityColumn") IcebergColumnHandle visibilityColumn,
            @JsonProperty("auths") List<String> auths) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.visibilityColumn = Objects.requireNonNull(visibilityColumn, "visibilityColumn");
        this.auths = List.copyOf(auths);
        if (this.auths.isEmpty()) throw new IllegalArgumentException("No visibility predicates");
    }
    @JsonProperty public ConnectorTableHandle delegate() { return delegate; }
    @JsonProperty public IcebergColumnHandle visibilityColumn() { return visibilityColumn; }
    @JsonProperty public List<String> auths() { return auths; }

    VisibilityTableHandle withDelegate(ConnectorTableHandle handle) {
        return new VisibilityTableHandle(handle, visibilityColumn, auths);
    }
}
