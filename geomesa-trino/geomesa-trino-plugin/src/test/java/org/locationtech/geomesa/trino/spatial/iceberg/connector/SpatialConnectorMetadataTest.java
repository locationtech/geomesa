/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.spi.connector.*;
import io.trino.spi.expression.*;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.*;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.spatial.iceberg.GeoMesaColumnCatalog;
import org.locationtech.geomesa.trino.spatial.iceberg.TestGeometryType;

import java.util.*;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class SpatialConnectorMetadataTest {

    /** Opaque column handle used by FakeMetadata. */
    private record FakeColumnHandle(String name) implements ColumnHandle {}

    /** Minimal fake delegate — overrides getTableMetadata and getColumnHandles. */
    private static class FakeMetadata implements ConnectorMetadata {
        private final List<ColumnMetadata> columns;
        FakeMetadata(ColumnMetadata... cols) { this.columns = List.of(cols); }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session,
                                                       ConnectorTableHandle table) {
            return new ConnectorTableMetadata(new SchemaTableName("s", "t"), columns);
        }

        @Override
        public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table) {
            return new SchemaTableName("s", "t");
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) { return List.of(); }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session,
                                                   SchemaTableName tableName,
                                                   Optional<ConnectorTableVersion> startVersion,
                                                   Optional<ConnectorTableVersion> endVersion) {
            return null;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
                                                          ConnectorTableHandle tableHandle) {
            Map<String, ColumnHandle> map = new java.util.LinkedHashMap<>();
            for (ColumnMetadata col : columns) map.put(col.getName(), new FakeColumnHandle(col.getName()));
            return map;
        }

        @Override
        public ColumnMetadata getColumnMetadata(ConnectorSession session,
                                                ConnectorTableHandle tableHandle,
                                                ColumnHandle columnHandle) {
            if (columnHandle instanceof FakeColumnHandle fch) {
                return columns.stream()
                    .filter(c -> c.getName().equals(fch.name()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("unknown handle: " + fch.name()));
            }
            return null;
        }
    }

    /** Creates a GeoMesaColumnCatalog that does NOT pre-populate — each test drives
     *  discovery via the FakeMetadata it provides. A fresh catalog per test avoids
     *  cross-test interference (the cache is keyed by SchemaTableName). */
    private static GeoMesaColumnCatalog freshCatalog() {
        return new GeoMesaColumnCatalog();
    }

    @Test
    void geometryColumnRemainsVarbinary() {
        // No type overlay: geom stays VARBINARY; users call ST_GeomFromBinary() in SQL.
        FakeMetadata delegate = new FakeMetadata(
            new ColumnMetadata("geom",  VarbinaryType.VARBINARY),
            new ColumnMetadata("__fid__", VarcharType.VARCHAR)
        );
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(delegate, freshCatalog());
        ConnectorTableMetadata result = meta.getTableMetadata(null, null);

        assertThat(result.getColumns())
            .filteredOn(c -> c.getName().equals("geom"))
            .extracting(ColumnMetadata::getType)
            .containsExactly(VarbinaryType.VARBINARY);
    }

    @Test
    void unannotatedBinaryColumnStaysVarbinary() {
        FakeMetadata delegate = new FakeMetadata(
            new ColumnMetadata("other_bytes", VarbinaryType.VARBINARY)
        );
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(delegate, freshCatalog());
        ConnectorTableMetadata result = meta.getTableMetadata(null, null);

        assertThat(result.getColumns())
            .filteredOn(c -> c.getName().equals("other_bytes"))
            .extracting(ColumnMetadata::getType)
            .containsExactly(VarbinaryType.VARBINARY);
    }

    @Test
    void getTableMetadataPassesThroughAllColumns() {
        FakeMetadata delegate = new FakeMetadata(
            new ColumnMetadata("geom",          VarbinaryType.VARBINARY),
            new ColumnMetadata("__fid__",       VarcharType.VARCHAR),
            new ColumnMetadata("__geom_bbox__", VarbinaryType.VARBINARY)
        );
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(delegate, freshCatalog());
        ConnectorTableMetadata result = meta.getTableMetadata(null, null);

        assertThat(result.getColumns()).hasSize(3);
        assertThat(result.getColumns())
            .extracting(ColumnMetadata::getName)
            .containsExactlyInAnyOrder("geom", "__fid__", "__geom_bbox__");
    }

    // ── Bbox struct predicate Z2 range extraction ─────────────────────────────

    // Field indices in __geom_bbox__: xmin=0, ymin=1, xmax=2, ymax=3
    private static final RowType BBOX_ROW_TYPE = RowType.rowType(
        RowType.field("xmin", RealType.REAL),
        RowType.field("ymin", RealType.REAL),
        RowType.field("xmax", RealType.REAL),
        RowType.field("ymax", RealType.REAL)
    );

    /** Builds the 4-predicate AND expression for a lat/lon bbox query. */
    private static ConnectorExpression bboxAndExpr(double loX, double hiX, double loY, double hiY) {
        return bboxAndExprFor("__geom_bbox__", loX, hiX, loY, hiY);
    }

    /** Like {@link #bboxAndExpr} but against an arbitrary bbox struct column. */
    private static ConnectorExpression bboxAndExprFor(String bboxColumn,
            double loX, double hiX, double loY, double hiY) {
        Variable bboxVar = new Variable(bboxColumn, BBOX_ROW_TYPE);
        FieldDereference xmin = new FieldDereference(RealType.REAL, bboxVar, 0);
        FieldDereference ymin = new FieldDereference(RealType.REAL, bboxVar, 1);
        FieldDereference xmax = new FieldDereference(RealType.REAL, bboxVar, 2);
        FieldDereference ymax = new FieldDereference(RealType.REAL, bboxVar, 3);
        FunctionName gte = GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
        FunctionName lte = LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
        FunctionName and = AND_FUNCTION_NAME;
        Call p1 = new Call(BooleanType.BOOLEAN, gte, List.of(xmax, new Constant(loX, DoubleType.DOUBLE)));
        Call p2 = new Call(BooleanType.BOOLEAN, lte, List.of(xmin, new Constant(hiX, DoubleType.DOUBLE)));
        Call p3 = new Call(BooleanType.BOOLEAN, gte, List.of(ymax, new Constant(loY, DoubleType.DOUBLE)));
        Call p4 = new Call(BooleanType.BOOLEAN, lte, List.of(ymin, new Constant(hiY, DoubleType.DOUBLE)));
        return new Call(BooleanType.BOOLEAN, and,
            List.of(new Call(BooleanType.BOOLEAN, and, List.of(p1, p2)),
                    new Call(BooleanType.BOOLEAN, and, List.of(p3, p4))));
    }

    @Test
    void bboxStructPredicateExtractsEnvelope() {
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            new FakeMetadata(), freshCatalog());
        ConnectorExpression expr = bboxAndExpr(-80.0, -70.0, 37.0, 45.0);
        Optional<SpatialConnectorMetadata.BboxPatternMatch> match = meta.tryExtractBboxPatternMatch(expr);
        assertThat(match).isPresent();
        assertThat(match.get().geomName()).isEqualTo("geom");
        assertThat(match.get().envelope().getMinX()).isEqualTo(-80.0);
        assertThat(match.get().envelope().getMaxX()).isEqualTo(-70.0);
        assertThat(match.get().envelope().getMinY()).isEqualTo(37.0);
        assertThat(match.get().envelope().getMaxY()).isEqualTo(45.0);
    }

    @Test
    void bboxStructPredicateWithRealBitEncodedConstantsExtractsEnvelope() {
        // Trino sends REAL constants as IEEE 754 bit-patterns stored as Long
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            new FakeMetadata(), freshCatalog());
        // Use a plan symbol name that Trino would actually produce (underscores stripped)
        Variable bboxVar = new Variable("geom_bbox", BBOX_ROW_TYPE);
        FunctionName gte = GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
        FunctionName lte = LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
        FunctionName and = AND_FUNCTION_NAME;
        FieldDereference xmax = new FieldDereference(RealType.REAL, bboxVar, 2);
        FieldDereference xmin = new FieldDereference(RealType.REAL, bboxVar, 0);
        FieldDereference ymax = new FieldDereference(RealType.REAL, bboxVar, 3);
        FieldDereference ymin = new FieldDereference(RealType.REAL, bboxVar, 1);
        // -80.0f = 0xC2A00000 = -1029701632 as signed int
        Constant lo_x = new Constant((long) Float.floatToIntBits(-80.0f), RealType.REAL);
        Constant hi_x = new Constant((long) Float.floatToIntBits(-70.0f), RealType.REAL);
        Constant lo_y = new Constant((long) Float.floatToIntBits(37.0f),  RealType.REAL);
        Constant hi_y = new Constant((long) Float.floatToIntBits(45.0f),  RealType.REAL);
        // Trino emits a flat $and with 4 children, not nested
        ConnectorExpression expr = new Call(BooleanType.BOOLEAN, and,
            List.of(new Call(BooleanType.BOOLEAN, gte, List.of(xmax, lo_x)),
                    new Call(BooleanType.BOOLEAN, lte, List.of(xmin, hi_x)),
                    new Call(BooleanType.BOOLEAN, gte, List.of(ymax, lo_y)),
                    new Call(BooleanType.BOOLEAN, lte, List.of(ymin, hi_y))));
        Optional<SpatialConnectorMetadata.BboxPatternMatch> match = meta.tryExtractBboxPatternMatch(expr);
        assertThat(match).isPresent();
        assertThat(match.get().geomName()).isEqualTo("geom");
        assertThat(match.get().envelope().getMinX()).isCloseTo(-80.0, within(1e-3));
        assertThat(match.get().envelope().getMaxX()).isCloseTo(-70.0, within(1e-3));
        assertThat(match.get().envelope().getMinY()).isCloseTo(37.0, within(1e-3));
        assertThat(match.get().envelope().getMaxY()).isCloseTo(45.0, within(1e-3));
    }

    @Test
    void bboxPatternUnderOrIsNotReconstructed() {
        // Bound comparisons inside a disjunction are not top-level constraints —
        // reconstructing an envelope from them would over-prune the other branch.
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            new FakeMetadata(), freshCatalog());
        ConnectorExpression bbox = bboxAndExpr(-80.0, -70.0, 37.0, 45.0);
        ConnectorExpression or = new Call(BooleanType.BOOLEAN, OR_FUNCTION_NAME,
            List.of(bbox, new Variable("flag", BooleanType.BOOLEAN)));
        assertThat(meta.tryExtractBboxPatternMatch(or)).isEmpty();
    }

    @Test
    void bboxPatternReconstructsPerGeomForMultiGeomConjunctions() {
        // Two geoms' bbox comparisons ANDed together yield one envelope EACH —
        // multi-geom queries get independent pruning, not first-geom-wins.
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            new FakeMetadata(), freshCatalog());
        ConnectorExpression both = new Call(BooleanType.BOOLEAN, AND_FUNCTION_NAME,
            List.of(bboxAndExprFor("__center_bbox__", -80.0, -70.0, 37.0, 45.0),
                    bboxAndExprFor("__ellipse_bbox__", 10.0, 11.0, 20.0, 21.0)));
        List<SpatialConnectorMetadata.BboxPatternMatch> matches =
            meta.tryExtractBboxPatternMatches(both);
        assertThat(matches)
            .extracting(SpatialConnectorMetadata.BboxPatternMatch::geomName)
            .containsExactlyInAnyOrder("center", "ellipse");
        SpatialConnectorMetadata.BboxPatternMatch ellipse = matches.stream()
            .filter(bp -> bp.geomName().equals("ellipse")).findFirst().orElseThrow();
        assertThat(ellipse.envelope().getMinX()).isEqualTo(10.0);
        assertThat(ellipse.envelope().getMaxY()).isEqualTo(21.0);
    }

    @Test
    void incompleteBboxReturnEmpty() {
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            new FakeMetadata(), freshCatalog());
        // Only 3 of 4 bbox predicates — not enough to reconstruct the envelope
        Variable bboxVar = new Variable("__geom_bbox__", BBOX_ROW_TYPE);
        FunctionName gte = GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
        FunctionName lte = LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
        FunctionName and = AND_FUNCTION_NAME;
        Call p1 = new Call(BooleanType.BOOLEAN, gte, List.of(
            new FieldDereference(RealType.REAL, bboxVar, 2), new Constant(-80.0, DoubleType.DOUBLE)));
        Call p2 = new Call(BooleanType.BOOLEAN, lte, List.of(
            new FieldDereference(RealType.REAL, bboxVar, 0), new Constant(-70.0, DoubleType.DOUBLE)));
        Call p3 = new Call(BooleanType.BOOLEAN, gte, List.of(
            new FieldDereference(RealType.REAL, bboxVar, 3), new Constant(37.0, DoubleType.DOUBLE)));
        // ymin predicate missing
        ConnectorExpression expr = new Call(BooleanType.BOOLEAN, and, List.of(p1, p2, p3));
        assertThat(meta.tryExtractBboxPatternMatch(expr)).isEmpty();
    }

    @Test
    void bboxOnDifferentFieldNamesReturnEmpty() {
        // A struct with different field names is not a bbox column
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            new FakeMetadata(), freshCatalog());
        RowType otherType = RowType.rowType(
            RowType.field("a", RealType.REAL),
            RowType.field("b", RealType.REAL),
            RowType.field("c", RealType.REAL),
            RowType.field("d", RealType.REAL)
        );
        Variable otherVar = new Variable("other_struct", otherType);
        FunctionName gte = GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
        FunctionName and = AND_FUNCTION_NAME;
        Call p = new Call(BooleanType.BOOLEAN, gte, List.of(
            new FieldDereference(RealType.REAL, otherVar, 2), new Constant(-80.0, DoubleType.DOUBLE)));
        ConnectorExpression expr = new Call(BooleanType.BOOLEAN, and, List.of(p));
        assertThat(meta.tryExtractBboxPatternMatch(expr)).isEmpty();
    }

    // ── Spatial-function envelope extraction with function-name capture ───────

    @Test
    void tryExtractEnvelopeReturnsFunctionNameForStIntersects() {
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            new FakeMetadata(), freshCatalog());
        Variable geomVar = new Variable("geom", VarbinaryType.VARBINARY);
        io.airlift.slice.Slice wktSlice = io.trino.geospatial.serde.JtsGeometrySerde.serialize(
            new org.locationtech.jts.geom.GeometryFactory().toGeometry(
                new org.locationtech.jts.geom.Envelope(-80, -70, 37, 45)));
        Constant geomConst = new Constant(wktSlice,
            TestGeometryType.GEOMETRY);
        Call expr = new Call(BooleanType.BOOLEAN,
            new FunctionName("st_intersects"),
            List.of(geomVar, geomConst));

        Optional<SpatialConnectorMetadata.SpatialMatch> match = meta.findSpatialMatch(expr);
        assertThat(match).isPresent();
        assertThat(match.get().functionName()).isEqualTo("st_intersects");
        assertThat(match.get().envelopes()).hasSize(1);
        assertThat(match.get().envelopes().get(0).getMinX()).isEqualTo(-80.0);
    }

    @Test
    void tryExtractEnvelopeReturnsFunctionNameForStWithin() {
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            new FakeMetadata(), freshCatalog());
        Variable geomVar = new Variable("geom", VarbinaryType.VARBINARY);
        io.airlift.slice.Slice wktSlice = io.trino.geospatial.serde.JtsGeometrySerde.serialize(
            new org.locationtech.jts.geom.GeometryFactory().toGeometry(
                new org.locationtech.jts.geom.Envelope(-80, -70, 37, 45)));
        Constant geomConst = new Constant(wktSlice,
            TestGeometryType.GEOMETRY);
        Call expr = new Call(BooleanType.BOOLEAN,
            new FunctionName("st_within"),
            List.of(geomVar, geomConst));

        Optional<SpatialConnectorMetadata.SpatialMatch> match = meta.findSpatialMatch(expr);
        assertThat(match).isPresent();
        assertThat(match.get().functionName()).isEqualTo("st_within");
    }

    // ── OR'd spatial predicates ───────────────────────────────────────────────

    /** ST_Intersects(ST_GeomFromBinary(<geomColumn>), <envelope literal>). */
    private static Call intersects(String geomColumn, double minX, double maxX,
                                   double minY, double maxY) {
        Variable geomVar = new Variable(geomColumn, VarbinaryType.VARBINARY);
        io.airlift.slice.Slice slice = io.trino.geospatial.serde.JtsGeometrySerde.serialize(
            new org.locationtech.jts.geom.GeometryFactory().toGeometry(
                new org.locationtech.jts.geom.Envelope(minX, maxX, minY, maxY)));
        return new Call(BooleanType.BOOLEAN, new FunctionName("st_intersects"),
            List.of(geomVar, new Constant(slice, TestGeometryType.GEOMETRY)));
    }

    @Test
    void orOfSpatialPredicatesOnSameGeomYieldsUnionMatch() {
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            new FakeMetadata(), freshCatalog());
        ConnectorExpression or = new Call(BooleanType.BOOLEAN, OR_FUNCTION_NAME,
            List.of(intersects("geom", -80, -70, 37, 45),
                    intersects("geom", 10, 11, 20, 21)));

        List<SpatialConnectorMetadata.SpatialMatch> matches = meta.findAllSpatialMatches(or);
        assertThat(matches).hasSize(1);
        SpatialConnectorMetadata.SpatialMatch m = matches.get(0);
        assertThat(m.geomName()).isEqualTo("geom");
        assertThat(m.envelopes()).hasSize(2);
        assertThat(m.envelopes().get(0).getMinX()).isEqualTo(-80.0);
        assertThat(m.envelopes().get(1).getMinX()).isEqualTo(10.0);
    }

    @Test
    void orWithNonSpatialBranchYieldsNoMatches() {
        // A row can satisfy the OR through the non-spatial branch, which says
        // nothing about geom — no envelope may be pushed.
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            new FakeMetadata(), freshCatalog());
        ConnectorExpression or = new Call(BooleanType.BOOLEAN, OR_FUNCTION_NAME,
            List.of(intersects("geom", -80, -70, 37, 45),
                    new Variable("flag", BooleanType.BOOLEAN)));
        assertThat(meta.findAllSpatialMatches(or)).isEmpty();
    }

    @Test
    void orOverDifferentGeomsYieldsNoMatches() {
        // Each branch constrains a different geometry column; neither is
        // constrained in EVERY branch, so nothing can be pushed.
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            new FakeMetadata(), freshCatalog());
        ConnectorExpression or = new Call(BooleanType.BOOLEAN, OR_FUNCTION_NAME,
            List.of(intersects("center", -80, -70, 37, 45),
                    intersects("ellipse", 10, 11, 20, 21)));
        assertThat(meta.findAllSpatialMatches(or)).isEmpty();
    }

    @Test
    void orNestedUnderAndStillYieldsUnionMatch() {
        // AND(flag, OR(intersects(g,p1), intersects(g,p2))) — the OR is a
        // top-level conjunct, so its union envelope constrains the result set.
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            new FakeMetadata(), freshCatalog());
        ConnectorExpression or = new Call(BooleanType.BOOLEAN, OR_FUNCTION_NAME,
            List.of(intersects("geom", -80, -70, 37, 45),
                    intersects("geom", 10, 11, 20, 21)));
        ConnectorExpression and = new Call(BooleanType.BOOLEAN, AND_FUNCTION_NAME,
            List.of(new Variable("flag", BooleanType.BOOLEAN), or));

        List<SpatialConnectorMetadata.SpatialMatch> matches = meta.findAllSpatialMatches(and);
        assertThat(matches).hasSize(1);
        assertThat(matches.get(0).envelopes()).hasSize(2);
    }

    @Test
    void orBranchesMixedOnSharedAndUnsharedGeomsPushOnlyTheSharedGeom() {
        // Branch 1 constrains {center, ellipse}, branch 2 only {center}:
        // center is constrained in every branch (pushable); ellipse is not.
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(
            new FakeMetadata(), freshCatalog());
        ConnectorExpression branch1 = new Call(BooleanType.BOOLEAN, AND_FUNCTION_NAME,
            List.of(intersects("center", -80, -70, 37, 45),
                    intersects("ellipse", 0, 1, 0, 1)));
        ConnectorExpression or = new Call(BooleanType.BOOLEAN, OR_FUNCTION_NAME,
            List.of(branch1, intersects("center", 10, 11, 20, 21)));

        List<SpatialConnectorMetadata.SpatialMatch> matches = meta.findAllSpatialMatches(or);
        assertThat(matches).hasSize(1);
        assertThat(matches.get(0).geomName()).isEqualTo("center");
        assertThat(matches.get(0).envelopes()).hasSize(2);
    }

    @Test
    void applyFilterReturnsSameResultAsDelegate() {
        FakeMetadata delegate = new FakeMetadata(
            new ColumnMetadata("geom", VarbinaryType.VARBINARY)
        );
        SpatialConnectorMetadata meta = new SpatialConnectorMetadata(delegate, freshCatalog());

        io.trino.spi.expression.ConnectorExpression trueExpr =
            io.trino.spi.expression.Constant.TRUE;
        Constraint constraint = new Constraint(TupleDomain.all(), trueExpr, Map.of());
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
            meta.applyFilter(null, null, constraint);

        assertThat(result).isEmpty();
    }

    // ── Pure delegation: metadata/column accessors pass through unchanged ────

    @Test
    void getTableMetadataPassesDelegateColumnsThrough() {
        // The connector no longer overlays a Geometry type or synthesizes a
        // __geom_wkb__ companion; geom stays VARBINARY and the column list is
        // exactly what the underlying iceberg connector returned.
        ColumnMetadata geomCol = ColumnMetadata.builder()
            .setName("geom").setType(VarbinaryType.VARBINARY).build();
        ColumnMetadata geomBboxCol = ColumnMetadata.builder()
            .setName("__geom_bbox__").setType(VarbinaryType.VARBINARY).build();
        ColumnMetadata payload = ColumnMetadata.builder()
            .setName("payload").setType(VarbinaryType.VARBINARY).build();
        ColumnMetadata id = ColumnMetadata.builder()
            .setName("id").setType(BigintType.BIGINT).build();
        FakeMetadata delegate = new FakeMetadata(geomCol, geomBboxCol, payload, id);
        SpatialConnectorMetadata metadata = new SpatialConnectorMetadata(delegate, freshCatalog());

        ConnectorTableMetadata result = metadata.getTableMetadata(null, null);

        assertThat(result.getColumns()).hasSize(4);
        assertThat(result.getColumns()).extracting(ColumnMetadata::getName)
            .containsExactly("geom", "__geom_bbox__", "payload", "id");
        assertThat(result.getColumns().get(0).getType()).isSameAs(VarbinaryType.VARBINARY);
    }

    @Test
    void getColumnHandlesReturnsDelegateHandlesUnchanged() {
        ColumnMetadata geomCol = ColumnMetadata.builder()
            .setName("geom").setType(VarbinaryType.VARBINARY).build();
        ColumnMetadata geomBboxCol = ColumnMetadata.builder()
            .setName("__geom_bbox__").setType(VarbinaryType.VARBINARY).build();
        FakeMetadata delegate = new FakeMetadata(geomCol, geomBboxCol);
        SpatialConnectorMetadata metadata = new SpatialConnectorMetadata(delegate, freshCatalog());

        Map<String, ColumnHandle> handles = metadata.getColumnHandles(null, null);

        // Identity match: same handle instances the delegate returned. No wrapping.
        assertThat(handles.keySet()).containsExactlyInAnyOrder("geom", "__geom_bbox__");
        assertThat(handles.get("geom")).isInstanceOf(FakeColumnHandle.class);
    }

    // ── extractGeomColumnName: unwraps ST_GeomFromBinary(geom) ────────────────

    @Test
    void extractGeomColumnNameUnwrapsStGeomFromBinary() {
        // The new emission shape is ST_Intersects(ST_GeomFromBinary(geom), <literal>).
        // The connector must see through the ST_GeomFromBinary wrap to identify the geom
        // column for pushdown routing.
        Variable geomVar = new Variable("geom", VarbinaryType.VARBINARY);
        Call wrappedGeom = new Call(
            TestGeometryType.GEOMETRY,
            new FunctionName("st_geomfrombinary"),
            List.of(geomVar));
        io.airlift.slice.Slice geomSlice = io.trino.geospatial.serde.JtsGeometrySerde.serialize(
            new org.locationtech.jts.geom.GeometryFactory().toGeometry(
                new org.locationtech.jts.geom.Envelope(-80, -70, 37, 45)));
        Constant geomConst = new Constant(geomSlice,
            TestGeometryType.GEOMETRY);
        Call expr = new Call(BooleanType.BOOLEAN,
            new FunctionName("st_intersects"),
            List.of(wrappedGeom, geomConst));

        Optional<String> name = SpatialConnectorMetadata.extractGeomColumnName(expr);
        assertThat(name).contains("geom");
    }

    @Test
    void extractGeomColumnNameStillAcceptsBareVariable() {
        // Defensive: tests built before the wrap (and any planner shape that hands
        // the bare variable through) should still resolve.
        Variable geomVar = new Variable("center", VarbinaryType.VARBINARY);
        Call expr = new Call(BooleanType.BOOLEAN,
            new FunctionName("st_intersects"),
            List.of(geomVar, new Constant(0L, BooleanType.BOOLEAN)));

        Optional<String> name = SpatialConnectorMetadata.extractGeomColumnName(expr);
        assertThat(name).contains("center");
    }

    // ── Prune-safe double→float narrowing of query-envelope bounds ────────────

    @Test
    void pruneSafeLowerBoundNeverExceedsQueryValue() {
        // nearest float (-80.0f) is greater than the query value
        double queryMin = -80.000000001;
        float bound = SpatialConnectorMetadata.pruneSafeLowerBound(queryMin);
        assertThat((double) bound).isLessThanOrEqualTo(queryMin);
    }

    @Test
    void pruneSafeUpperBoundNeverFallsBelowQueryValue() {
        // nearest float (80.0f) is less than the query value
        double queryMax = 80.000000001;
        float bound = SpatialConnectorMetadata.pruneSafeUpperBound(queryMax);
        assertThat((double) bound).isGreaterThanOrEqualTo(queryMax);
    }

    @Test
    void pruneSafeBoundsAreOutwardAcrossRepresentativeCoordinates() {
        double[] coords = {-179.99999999, -84.93000000001, -80.00003, -1e-12,
                           0.0, 1e-12, 33.14159265358979, 116.40000000001, 179.99999999};
        for (double v : coords) {
            assertThat((double) SpatialConnectorMetadata.pruneSafeLowerBound(v))
                .as("lower bound for %s", v).isLessThanOrEqualTo(v);
            assertThat((double) SpatialConnectorMetadata.pruneSafeUpperBound(v))
                .as("upper bound for %s", v).isGreaterThanOrEqualTo(v);
        }
    }

    @Test
    void pruneSafeBoundsPreserveExactlyRepresentableValues() {
        assertThat(SpatialConnectorMetadata.pruneSafeLowerBound(-80.0)).isEqualTo(-80.0f);
        assertThat(SpatialConnectorMetadata.pruneSafeUpperBound(-80.0)).isEqualTo(-80.0f);
        assertThat(SpatialConnectorMetadata.pruneSafeLowerBound(116.5)).isEqualTo(116.5f);
        assertThat(SpatialConnectorMetadata.pruneSafeUpperBound(116.5)).isEqualTo(116.5f);
    }
}
