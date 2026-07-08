/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TrinoSchemaDiscoveryTest {

    @Test
    void discoversCompanionStyleVisColumn() {
        assertThat(TrinoSchemaDiscovery.discoverVisibilityColumn(
            Set.of("__fid__", "geom", "__vis__"))).isEqualTo("__vis__");
    }

    @Test
    void bareVisibilitiesColumnIsNotTreatedAsVisibility() {
        // A user attribute merely named "visibilities" is ordinary data, not an
        // enforcement column — only the companion-style __vis__ engages filtering.
        assertThat(TrinoSchemaDiscovery.discoverVisibilityColumn(
            Set.of("__fid__", "geom", "visibilities"))).isNull();
    }

    @Test
    void absentVisColumnYieldsNull() {
        assertThat(TrinoSchemaDiscovery.discoverVisibilityColumn(
            Set.of("__fid__", "geom", "dtg"))).isNull();
    }

    @Test
    void z2CompanionMarksPointColumn() {
        assertThat(TrinoSchemaDiscovery.isPointColumn("center",
            Set.of("center", "__center_z2__", "__center_bbox__"))).isTrue();
    }

    @Test
    void xz2CompanionIsNotPointColumn() {
        assertThat(TrinoSchemaDiscovery.isPointColumn("ellipse",
            Set.of("ellipse", "__ellipse_xz2__", "__ellipse_bbox__"))).isFalse();
    }
}
