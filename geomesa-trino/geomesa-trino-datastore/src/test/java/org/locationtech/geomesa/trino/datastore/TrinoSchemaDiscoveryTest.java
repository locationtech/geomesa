/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TrinoSchemaDiscoveryTest {

    @Test
    void columnSpecPropertyHasTheExpectedShape() {
        // pinned literally on purpose: IcebergCatalog.columnSpecProperty writes this key and is
        // not on this module's classpath, so drift between the two would otherwise be silent -
        // discovery reads attributes from nothing else, so every attribute would simply vanish
        assertThat(TrinoSchemaDiscovery.COLUMN_PREFIX).isEqualTo("geomesa.col.");
        assertThat(TrinoSchemaDiscovery.columnSpecProperty("identifiers"))
            .isEqualTo("geomesa.col.identifiers.spec");
    }

    @Test
    void visibilityColumnConstantsAreStable() {
        // TrinoFeatureSource reads the user-data key off the discovered feature type
        assertThat(TrinoSchemaDiscovery.VIS_COLUMN).isEqualTo("__vis__");
        assertThat(TrinoSchemaDiscovery.VIS_COLUMN_KEY).isEqualTo("trino.visibility.column");
    }
}
