/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg;

import org.apache.iceberg.types.Type;

/**
 * Iceberg type that represents a WKB-encoded geometry column.
 *
 * {@link #typeId()} returns {@code TypeID.BINARY} — geometry is stored as raw WKB bytes
 * and columns are identified by the {@code geomesa.geometry.columns} catalog property.
 * A future stage could introduce {@code TypeID.GEOMETRY} in a forked iceberg-core, which
 * would allow column identification without the table-property workaround.
 */
public class GeometryType extends Type.PrimitiveType {

    public static final String TYPE_NAME = "geometry";

    private static final GeometryType DEFAULT = new GeometryType(4326);

    private final int srid;

    private GeometryType(int srid) {
        this.srid = srid;
    }

    /** Returns a {@code GeometryType} with SRID 4326 (WGS-84). */
    public static GeometryType get() {
        return DEFAULT;
    }

    /** Returns a {@code GeometryType} with the given SRID. */
    public static GeometryType of(int srid) {
        if (srid == 4326) return DEFAULT;
        return new GeometryType(srid);
    }

    /** Returns the SRID (spatial reference ID) for this geometry type. */
    public int srid() {
        return srid;
    }

    /** Returns {@code TypeID.BINARY} — geometry is stored as raw WKB bytes. */
    @Override
    public TypeID typeId() {
        return TypeID.BINARY;
    }

    @Override
    public String toString() {
        return TYPE_NAME + "(" + srid + ")";
    }
}
