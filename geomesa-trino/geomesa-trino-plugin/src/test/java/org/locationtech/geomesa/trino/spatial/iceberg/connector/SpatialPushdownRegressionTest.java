/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.plugin.iceberg.TableType;
import io.trino.spi.connector.*;
import io.trino.spi.expression.*;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.*;
import org.junit.jupiter.api.Test;
import org.locationtech.geomesa.trino.spatial.iceberg.GeoMesaColumnCatalog;
import org.locationtech.geomesa.trino.spatial.iceberg.TestGeometryType;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.*;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static org.assertj.core.api.Assertions.assertThat;

class SpatialPushdownRegressionTest {

    private static final RowType BBOX = RowType.rowType(
        RowType.field("xmin", RealType.REAL), RowType.field("ymin", RealType.REAL),
        RowType.field("xmax", RealType.REAL), RowType.field("ymax", RealType.REAL));
    private static final IcebergColumnHandle GEOM = column(1, "geom", VarbinaryType.VARBINARY);
    private static final IcebergColumnHandle Z2 = column(2, "__geom_z2__", VarcharType.VARCHAR);
    private static final IcebergColumnHandle BBOX_COLUMN = new IcebergColumnHandle(
        new ColumnIdentity(3, "__geom_bbox__", ColumnIdentity.TypeCategory.STRUCT, List.of(
            new ColumnIdentity(4, "xmin", ColumnIdentity.TypeCategory.PRIMITIVE, List.of()),
            new ColumnIdentity(5, "ymin", ColumnIdentity.TypeCategory.PRIMITIVE, List.of()),
            new ColumnIdentity(6, "xmax", ColumnIdentity.TypeCategory.PRIMITIVE, List.of()),
            new ColumnIdentity(7, "ymax", ColumnIdentity.TypeCategory.PRIMITIVE, List.of()))),
        BBOX, List.of(), BBOX, false, Optional.empty());

    private static IcebergColumnHandle column(int id, String name, Type type) {
        return new IcebergColumnHandle(
            new ColumnIdentity(id, name, ColumnIdentity.TypeCategory.PRIMITIVE, List.of()),
            type, List.of(), type, false, Optional.empty());
    }

    private static IcebergTableHandle table(TupleDomain<IcebergColumnHandle> predicate) {
        return new IcebergTableHandle("s", "t", TableType.DATA, OptionalLong.empty(), "{}",
            OptionalInt.empty(), Map.of(), 2, predicate, TupleDomain.all(), OptionalLong.empty(),
            Set.of(), Optional.empty(), "s3://test/t", Map.of(), Optional.empty(), false,
            Optional.empty(), Set.of(), Optional.empty());
    }

    private static Call intersects(String symbol) {
        Constant rectangle = new Constant(new GeometryFactory().toGeometry(new Envelope(0, 10, 0, 20)),
            TestGeometryType.GEOMETRY);
        return new Call(BooleanType.BOOLEAN, new FunctionName("st_intersects"), List.of(geometry(symbol), rectangle));
    }

    private static Call geometry(String symbol) {
        return new Call(TestGeometryType.GEOMETRY, new FunctionName("st_geomfrombinary"),
            List.of(new Variable(symbol, VarbinaryType.VARBINARY)));
    }

    private static final class Delegate implements ConnectorMetadata {
        Constraint lastConstraint;
        ConnectorTableHandle lastHandle;
        IcebergTableHandle returnedHandle;
        boolean steady;

        @Override
        public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle handle) {
            return new SchemaTableName("s", "t");
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle handle) {
            return Map.of("geom", GEOM, "__geom_z2__", Z2, "__geom_bbox__", BBOX_COLUMN);
        }

        @Override
        public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
                ConnectorSession session, ConnectorTableHandle handle, Constraint constraint) {
            lastHandle = handle;
            lastConstraint = constraint;
            if (steady) return Optional.empty();
            returnedHandle = table(constraint.getSummary().transformKeys(IcebergColumnHandle.class::cast));
            return Optional.of(new ConstraintApplicationResult<>(returnedHandle, constraint.getSummary(),
                constraint.getExpression(), false));
        }

        @Override
        public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
                ConnectorSession session, ConnectorTableHandle handle,
                List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments) {
            lastHandle = handle;
            if (steady) return Optional.empty();
            returnedHandle = ((IcebergTableHandle) handle).withProjectedColumns(Set.of(GEOM));
            return Optional.of(new ProjectionApplicationResult<>(returnedHandle, projections,
                List.of(new Assignment("output", GEOM, GEOM.getType())), false));
        }
    }

    @Test
    void shortcutPreservesAnUnextractableIntersectsConjunct() {
        Delegate delegate = new Delegate();
        var metadata = new SpatialConnectorMetadata(delegate, new GeoMesaColumnCatalog(), true);
        Call eligible = intersects("renamed_geom");
        Call other = new Call(BooleanType.BOOLEAN, new FunctionName("st_intersects"), List.of(
            geometry("a"), geometry("b")));
        Call expression = new Call(BooleanType.BOOLEAN, AND_FUNCTION_NAME, List.of(eligible, other));
        var result = metadata.applyFilter(null, table(TupleDomain.all()),
            new Constraint(TupleDomain.all(), expression, Map.of("renamed_geom", GEOM, "a", GEOM, "b", GEOM))).orElseThrow();
        assertThat(result.getHandle()).isInstanceOf(SpatialTableHandle.class);
        assertThat(((Call) result.getRemainingExpression().orElseThrow()).getArguments())
            .containsExactly(Constant.TRUE, other);
        assertThat(delegate.lastConstraint.getSummary().getDomains().orElseThrow()).containsKey(Z2);
    }

    @Test
    void shortcutChecksTheMatchedRectangleWhenAnotherIntersectsAppearsFirst() {
        Delegate delegate = new Delegate();
        var metadata = new SpatialConnectorMetadata(delegate, new GeoMesaColumnCatalog(), true);
        Call other = new Call(BooleanType.BOOLEAN, new FunctionName("st_intersects"), List.of(
            geometry("a"), geometry("b")));
        Call expression = new Call(BooleanType.BOOLEAN, AND_FUNCTION_NAME, List.of(other, intersects("g")));
        var result = metadata.applyFilter(null, table(TupleDomain.all()),
            new Constraint(TupleDomain.all(), expression, Map.of("g", GEOM, "a", GEOM, "b", GEOM)))
            .orElseThrow();
        assertThat(result.getHandle()).isInstanceOf(SpatialTableHandle.class);
        assertThat(((Call) result.getRemainingExpression().orElseThrow()).getArguments())
            .containsExactly(other, Constant.TRUE);
    }

    @Test
    void assignmentsOverrideSymbolsThatMatchAnotherPhysicalColumn() {
        var metadata = new SpatialConnectorMetadata(null, null);
        IcebergColumnHandle other = column(8, "other_geom", VarbinaryType.VARBINARY);
        var matches = metadata.findAllSpatialMatches(intersects("geom"), Map.of("geom", other));
        assertThat(matches).extracting(SpatialConnectorMetadata.SpatialMatch::geomName)
            .containsExactly("other_geom");
        assertThat(metadata.findAllSpatialMatches(intersects("geom"), Map.of())).isEmpty();
    }

    @Test
    void disjunctionUsesAssignmentsInEveryBranch() {
        var metadata = new SpatialConnectorMetadata(null, null);
        var expression = new Call(BooleanType.BOOLEAN, OR_FUNCTION_NAME,
            List.of(intersects("left_alias"), intersects("right_alias")));
        var matches = metadata.findAllSpatialMatches(expression,
            Map.of("left_alias", GEOM, "right_alias", GEOM));
        assertThat(matches).hasSize(1);
        assertThat(matches.get(0).geomName()).isEqualTo("geom");
        assertThat(matches.get(0).envelopes()).hasSize(2);
    }

    @Test
    void bboxReconstructionUsesTheAssignedStruct() {
        var metadata = new SpatialConnectorMetadata(null, null);
        Variable bbox = new Variable("unrelated_symbol", BBOX);
        List<ConnectorExpression> comparisons = new ArrayList<>();
        for (int index = 0; index < 4; index++) {
            comparisons.add(new Call(BooleanType.BOOLEAN,
                index < 2 ? StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME
                          : StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(new FieldDereference(RealType.REAL, bbox, index),
                    new Constant(index < 2 ? 10.0 : 0.0, DoubleType.DOUBLE))));
        }
        var expression = new Call(BooleanType.BOOLEAN, AND_FUNCTION_NAME, comparisons);
        var matches = metadata.tryExtractBboxPatternMatches(expression, Map.of("unrelated_symbol", BBOX_COLUMN));
        assertThat(matches).hasSize(1);
        assertThat(matches.get(0).geomName()).isEqualTo("geom");
        assertThat(metadata.tryExtractBboxPatternMatches(expression, Map.of())).isEmpty();
    }

    @Test
    void subsequentFilterAndProjectionKeepTheAuthoritativeSpatialHandle() {
        Delegate delegate = new Delegate();
        var metadata = new SpatialConnectorMetadata(delegate, new GeoMesaColumnCatalog(), true);
        var first = metadata.applyFilter(null, table(TupleDomain.all()),
            new Constraint(TupleDomain.all(), intersects("g"), Map.of("g", GEOM))).orElseThrow();
        SpatialTableHandle spatial = (SpatialTableHandle) first.getHandle();
        var extraColumn = column(9, "category", VarcharType.VARCHAR);
        Domain extra = Domain.singleValue(VarcharType.VARCHAR, io.airlift.slice.Slices.utf8Slice("wanted"));
        TupleDomain<ColumnHandle> summary = TupleDomain.withColumnDomains(Map.of(extraColumn, extra));
        var filtered = metadata.applyFilter(null, spatial, new Constraint(summary)).orElseThrow();
        assertThat(delegate.lastHandle).isSameAs(spatial.delegate());
        SpatialTableHandle afterFilter = (SpatialTableHandle) filtered.getHandle();
        assertThat(afterFilter.delegate()).isSameAs(delegate.returnedHandle);
        assertThat(afterFilter.geomColumn()).isSameAs(spatial.geomColumn());
        assertThat(afterFilter.rectMaxY()).isEqualTo(spatial.rectMaxY());
        assertThat(filtered.getRemainingFilter()).isEqualTo(summary);

        Variable output = new Variable("output", VarbinaryType.VARBINARY);
        var projected = metadata.applyProjection(null, afterFilter, List.of(output), Map.of("output", GEOM))
            .orElseThrow();
        assertThat(delegate.lastHandle).isSameAs(afterFilter.delegate());
        SpatialTableHandle afterProjection = (SpatialTableHandle) projected.getHandle();
        assertThat(afterProjection.delegate()).isSameAs(delegate.returnedHandle);
        assertThat(afterProjection.bboxLeaves()).isEqualTo(spatial.bboxLeaves());
        assertThat(afterProjection.rectMinX()).isEqualTo(spatial.rectMinX());
        assertThat(projected.getProjections()).containsExactly(output);
        assertThat(projected.getAssignments()).hasSize(1);
        assertWorkerStillFilters(afterProjection);
        delegate.steady = true;
        assertThat(metadata.applyFilter(null, afterProjection, new Constraint(summary))).isEmpty();
        assertThat(metadata.applyProjection(null, afterProjection, List.of(output), Map.of("output", GEOM)))
            .isEmpty();
    }

    private static void assertWorkerStillFilters(SpatialTableHandle handle) {
        var provider = new SpatialPageSourceProvider(new ConnectorPageSourceProvider() {
            @Override
            public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction,
                    ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table,
                    Optional<ConnectorTableCredentials> credentials, List<ColumnHandle> columns,
                    DynamicFilter dynamicFilter, MemoryContext memoryContext) {
                assertThat(table).isSameAs(handle.delegate());
                io.trino.spi.block.Block[] blocks = new io.trino.spi.block.Block[columns.size()];
                for (int channel = 0; channel < columns.size(); channel++) {
                    IcebergColumnHandle column = (IcebergColumnHandle) columns.get(channel);
                    if (column.getType().equals(RealType.REAL)) {
                        var builder = RealType.REAL.createBlockBuilder(null, 2);
                        RealType.REAL.writeFloat(builder, 5);
                        RealType.REAL.writeFloat(builder, 50);
                        blocks[channel] = builder.build();
                    } else {
                        var builder = VarbinaryType.VARBINARY.createBlockBuilder(null, 2);
                        var writer = new org.locationtech.jts.io.WKBWriter();
                        var factory = new GeometryFactory();
                        for (double coordinate : new double[]{5, 50}) {
                            VarbinaryType.VARBINARY.writeSlice(builder, io.airlift.slice.Slices.wrappedBuffer(
                                writer.write(factory.createPoint(new org.locationtech.jts.geom.Coordinate(
                                    coordinate, coordinate)))));
                        }
                        blocks[channel] = builder.build();
                    }
                }
                return new ConnectorPageSource() {
                    private boolean done;
                    @Override public long getCompletedBytes() { return 0; }
                    @Override public long getReadTimeNanos() { return 0; }
                    @Override public boolean isFinished() { return done; }
                    @Override public void close() { done = true; }
                    @Override public SourcePage getNextSourcePage() {
                        if (done) return null;
                        done = true;
                        return SourcePage.create(new io.trino.spi.Page(blocks));
                    }
                };
            }
        });
        var source = provider.createPageSource(null, null, null, handle, Optional.empty(),
            List.of(GEOM), DynamicFilter.EMPTY, null);
        var page = source.getNextSourcePage();
        assertThat(page.getPositionCount()).isEqualTo(1);
        assertThat(page.getChannelCount()).isEqualTo(1);
        byte[] expected = new org.locationtech.jts.io.WKBWriter().write(
            new GeometryFactory().createPoint(new org.locationtech.jts.geom.Coordinate(5, 5)));
        assertThat(VarbinaryType.VARBINARY.getSlice(page.getBlock(0), 0).getBytes()).isEqualTo(expected);
    }

    @Test
    void secondSpatialFilterStaysResidualAlongsideTheFirstShortcut() {
        Delegate delegate = new Delegate();
        var metadata = new SpatialConnectorMetadata(delegate, new GeoMesaColumnCatalog(), true);
        SpatialTableHandle spatial = (SpatialTableHandle) metadata.applyFilter(null, table(TupleDomain.all()),
            new Constraint(TupleDomain.all(), intersects("g"), Map.of("g", GEOM))).orElseThrow().getHandle();
        Call additional = intersects("another_alias");
        var result = metadata.applyFilter(null, spatial,
            new Constraint(TupleDomain.all(), additional, Map.of("another_alias", GEOM))).orElseThrow();
        assertThat(result.getRemainingExpression()).contains(additional);
        assertThat(((SpatialTableHandle) result.getHandle()).delegate()).isSameAs(delegate.returnedHandle);
        assertThat(((SpatialTableHandle) result.getHandle()).geomColumn()).isSameAs(spatial.geomColumn());
    }
}
