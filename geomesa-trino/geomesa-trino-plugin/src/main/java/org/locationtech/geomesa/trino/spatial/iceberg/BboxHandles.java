/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg;

import io.trino.plugin.iceberg.IcebergColumnHandle;

/** Four leaf-field handles of a {@code __<X>_bbox__} struct column. */
public record BboxHandles(
    IcebergColumnHandle xmin,
    IcebergColumnHandle ymin,
    IcebergColumnHandle xmax,
    IcebergColumnHandle ymax
) {}
