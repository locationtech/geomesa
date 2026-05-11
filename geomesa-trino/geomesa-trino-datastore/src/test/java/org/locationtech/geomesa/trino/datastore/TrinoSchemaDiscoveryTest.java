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
    void discoversFsdsVisibilitiesColumn() {
        assertThat(TrinoSchemaDiscovery.discoverVisibilityColumn(
            Set.of("__fid__", "geom", "visibilities"))).isEqualTo("visibilities");
    }

    @Test
    void discoversCompanionStyleVisColumn() {
        assertThat(TrinoSchemaDiscovery.discoverVisibilityColumn(
            Set.of("__fid__", "geom", "__vis__"))).isEqualTo("__vis__");
    }

    @Test
    void visibilitiesWinsWhenBothPresent() {
        assertThat(TrinoSchemaDiscovery.discoverVisibilityColumn(
            Set.of("geom", "visibilities", "__vis__"))).isEqualTo("visibilities");
    }

    @Test
    void absentVisColumnYieldsNull() {
        assertThat(TrinoSchemaDiscovery.discoverVisibilityColumn(
            Set.of("__fid__", "geom", "dtg"))).isNull();
    }
}
