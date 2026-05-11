/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.spatial.iceberg;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GeometryTypeTest {

    @Test
    void defaultSrid() {
        GeometryType g = GeometryType.get();
        assertThat(g.srid()).isEqualTo(4326);
    }

    @Test
    void customSrid() {
        GeometryType g = GeometryType.of(32632);
        assertThat(g.srid()).isEqualTo(32632);
    }

    @Test
    void typeIdIsBinary() {
        assertThat(GeometryType.get().typeId()).isEqualTo(Type.TypeID.BINARY);
    }

    @Test
    void toStringIncludesSrid() {
        assertThat(GeometryType.get().toString()).isEqualTo("geometry(4326)");
        assertThat(GeometryType.of(32632).toString()).isEqualTo("geometry(32632)");
    }

    @Test
    void isPrimitive() {
        assertThat(GeometryType.get().isPrimitiveType()).isTrue();
    }

    @Test
    void equalsSameInstance() {
        GeometryType a = GeometryType.get();
        GeometryType b = GeometryType.get();
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void geometryTypeEqualsToBinaryType() {
        // Stage B: typeId() returns BINARY, so GeometryType equals BinaryType per Iceberg's model.
        // Stage C will introduce TypeID.GEOMETRY to make them distinguishable.
        assertThat(GeometryType.get()).isEqualTo(Types.BinaryType.get());
    }

    @Test
    void sridAccessorDistinguishesDifferentSrids() {
        // SRID is preserved as application metadata; Iceberg schema equality ignores it.
        assertThat(GeometryType.of(4326).srid()).isNotEqualTo(GeometryType.of(32632).srid());
    }
}
