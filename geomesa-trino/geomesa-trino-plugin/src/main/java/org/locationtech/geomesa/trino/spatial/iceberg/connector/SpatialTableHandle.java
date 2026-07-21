/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.List;

/**
 * A table handle produced by {@link SpatialConnectorMetadata#applyFilter} <em>only</em> when the
 * connector has claimed a rectangle {@code ST_Intersects} on a {@code Point} geometry column as
 * fully enforced — i.e. it will do the exact filtering itself in {@link BboxFilteringPageSource}
 * rather than leave it to the engine. Wraps {@link IcebergTableHandle} and carries what
 * the worker needs that the pushed float32 bbox domains cannot convey:
 *
 * <ul>
 *   <li>{@code geomColumn} — the geometry WKB column to materialize for the boundary-shell exact test;</li>
 *   <li>the <em>exact</em> query rectangle {@code [minX,maxX] × [minY,maxY]} (double precision) —
 *       used to build the inward-rounded accept box and to run the shell {@code intersects} test.
 *       The float32 domains on the handle are outward-rounded and lossy, so the exact rectangle
 *       must ride here.</li>
 * </ul>
 *
 * <p>All Trino/Iceberg interaction unwraps this back to {@link #delegate()} first;
 * a missed unwrap surfaces as a loud {@code ClassCastException}, never a silent wrong result.
 */
public record SpatialTableHandle(
    IcebergTableHandle delegate,
    IcebergColumnHandle geomColumn,
    List<IcebergColumnHandle> bboxLeaves,
    double rectMinX,
    double rectMinY,
    double rectMaxX,
    double rectMaxY
) implements ConnectorTableHandle {

    /** Order of {@link #bboxLeaves}: xmin, ymin, xmax, ymax. */
    public static final int XMIN = 0, YMIN = 1, XMAX = 2, YMAX = 3;

    @JsonCreator
    public SpatialTableHandle(
            @JsonProperty("delegate") IcebergTableHandle delegate,
            @JsonProperty("geomColumn") IcebergColumnHandle geomColumn,
            @JsonProperty("bboxLeaves") List<IcebergColumnHandle> bboxLeaves,
            @JsonProperty("rectMinX") double rectMinX,
            @JsonProperty("rectMinY") double rectMinY,
            @JsonProperty("rectMaxX") double rectMaxX,
            @JsonProperty("rectMaxY") double rectMaxY) {
        this.delegate = delegate;
        this.geomColumn = geomColumn;
        this.bboxLeaves = bboxLeaves;
        this.rectMinX = rectMinX;
        this.rectMinY = rectMinY;
        this.rectMaxX = rectMaxX;
        this.rectMaxY = rectMaxY;
    }

    @JsonProperty public IcebergTableHandle delegate() { return delegate; }
    @JsonProperty public IcebergColumnHandle geomColumn() { return geomColumn; }
    @JsonProperty public List<IcebergColumnHandle> bboxLeaves() { return bboxLeaves; }
    @JsonProperty public double rectMinX() { return rectMinX; }
    @JsonProperty public double rectMinY() { return rectMinY; }
    @JsonProperty public double rectMaxX() { return rectMaxX; }
    @JsonProperty public double rectMaxY() { return rectMaxY; }

    /** Unwrap to the Iceberg handle if wrapped, else return the handle unchanged. */
    static ConnectorTableHandle unwrap(ConnectorTableHandle handle) {
        return handle instanceof SpatialTableHandle w ? w.delegate() : handle;
    }
}
