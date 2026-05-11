/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg;

import io.trino.plugin.iceberg.IcebergColumnHandle;
import org.locationtech.geomesa.trino.spatial.SpatialIndexKind;

/** Iceberg column handle for a spatial-partition column ({@code __<X>_z2__} or
 *  {@code __<X>_xz2__}) and the effective bit resolution N, derived from the
 *  column's {@code truncate(width)} transform in the table's partition spec
 *  (N = 4 × width). See {@link GeoMesaColumnCatalog#deriveBitsFromTruncateWidth}. */
public record SpatialPartitionHandle(SpatialIndexKind kind, IcebergColumnHandle column, int bits) {}
