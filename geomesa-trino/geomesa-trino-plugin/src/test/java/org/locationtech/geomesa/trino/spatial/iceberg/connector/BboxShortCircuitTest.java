/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg.connector;

import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.type.BooleanType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pure-logic guards for the connector-side bbox-short-circuit:
 * <ul>
 *   <li>{@code dropSpatialCall} must claim <em>only</em> {@code ST_Intersects} enforced — every
 *       other conjunct, above all the {@code is_visible} row-filter, must survive. (Regression guard
 *       for the visibility-bypass bug where the whole residual was replaced with TRUE.)</li>
 *   <li>the reject/accept box bounds must round in the correct direction (outer outward, inner
 *       inward) so soundness holds against the ½-ulp error of the nearest-rounded float32 bbox.</li>
 * </ul>
 */
class BboxShortCircuitTest {

    private static Call call(String name, ConnectorExpression... args) {
        return new Call(BooleanType.BOOLEAN, new FunctionName(name), List.of(args));
    }

    private static Call and(ConnectorExpression... args) {
        return new Call(BooleanType.BOOLEAN, AND_FUNCTION_NAME, List.of(args));
    }

    @Test
    void dropSpatialCallReplacesOnlyStIntersects_keepingIsVisible() {
        Call isVisible = call("is_visible");
        Call stIntersects = call("st_intersects");
        ConnectorExpression result =
            SpatialConnectorMetadata.dropSpatialCall(and(isVisible, stIntersects));

        assertThat(result).isInstanceOf(Call.class);
        Call andResult = (Call) result;
        assertThat(andResult.getFunctionName()).isEqualTo(AND_FUNCTION_NAME);
        assertThat(andResult.getArguments()).hasSize(2);
        assertThat(andResult.getArguments().get(0)).isEqualTo(isVisible);
        assertThat(andResult.getArguments().get(1)).isEqualTo(Constant.TRUE);
    }

    @Test
    void dropSpatialCallOnBarePredicateBecomesTrue() {
        assertThat(SpatialConnectorMetadata.dropSpatialCall(call("st_intersects")))
            .isEqualTo(Constant.TRUE);
    }

    @Test
    void dropSpatialCallLeavesNonSpatialConjunctsUntouched() {
        Call isVisible = call("is_visible");
        Call other = call("some_other_predicate");
        ConnectorExpression result = SpatialConnectorMetadata.dropSpatialCall(and(isVisible, other));
        // No st_intersects present → nothing is dropped; the tree is preserved.
        assertThat(((Call) result).getArguments()).containsExactly(isVisible, other);
    }

    @Test
    void boundsRoundOutwardForRejectInwardForAccept() {
        for (double d : new double[]{-77.0, -76.0, 38.0, 40.0, 0.0, -179.999, 179.999, 123.456}) {
            float f = (float) d;
            // Reject box is expanded outward → never drops a true match.
            assertThat(SpatialPageSourceProvider.outLow(d)).isLessThan(f);
            assertThat(SpatialPageSourceProvider.outHigh(d)).isGreaterThan(f);
            // Accept box is shrunk inward → never accepts a true non-match.
            assertThat(SpatialPageSourceProvider.inLow(d)).isGreaterThan(f);
            assertThat(SpatialPageSourceProvider.inHigh(d)).isLessThan(f);
            // Accept box is strictly inside the reject box (boundary rows fall to the exact test).
            assertThat(SpatialPageSourceProvider.inLow(d)).isGreaterThan(SpatialPageSourceProvider.outLow(d));
            assertThat(SpatialPageSourceProvider.inHigh(d)).isLessThan(SpatialPageSourceProvider.outHigh(d));
        }
    }
}
