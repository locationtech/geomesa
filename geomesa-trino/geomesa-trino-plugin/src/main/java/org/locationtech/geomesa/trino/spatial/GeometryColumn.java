/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial;

import org.locationtech.geomesa.trino.spatial.iceberg.BboxHandles;
import org.locationtech.geomesa.trino.spatial.iceberg.GeoMesaColumnCatalog;
import org.locationtech.geomesa.trino.spatial.iceberg.SpatialPartitionHandle;

import java.util.Optional;

/**
 * Per-geometry-column descriptor produced by {@link GeoMesaColumnCatalog} discovery.
 * One {@code GeometryColumn} corresponds to a single VARBINARY column {@code X} in
 * a table that has at least one of {@code __X_bbox__}, {@code __X_z2__}, or
 * {@code __X_xz2__} companion columns.
 *
 * <p>{@code bbox} and {@code partition} are {@link Optional} because companions are
 * independent: a geom column may have bbox only (per-file pruning), partition only
 * (rare; possible for an externally created table), or both.
 */
public record GeometryColumn(
    String name,
    Optional<BboxHandles> bbox,
    Optional<SpatialPartitionHandle> partition
) {}
