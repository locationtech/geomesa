/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.datastore;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TrinoDataStoreConnectionTest {

    @Test
    void userAlwaysSet() {
        Properties props = TrinoDataStore.connectionProperties("geomesa", null, null);
        assertThat(props.getProperty("user")).isEqualTo("geomesa");
        assertThat(props.getProperty("extraCredentials")).isNull();
    }

    @Test
    void emptyAuthsSendNoCredential() {
        Properties props = TrinoDataStore.connectionProperties("geomesa", List.of(), null);
        assertThat(props.getProperty("extraCredentials")).isNull();
    }

    @Test
    void authsBecomeBase64UrlExtraCredential() {
        Properties props = TrinoDataStore.connectionProperties("svc", List.of("basic", "privileged"), null);
        assertThat(props.getProperty("extraCredentials")).isEqualTo("auths:YmFzaWMscHJpdmlsZWdlZA");
    }

    @Test
    void singleAuthEncodes() {
        Properties props = TrinoDataStore.connectionProperties("svc", List.of("basic"), null);
        assertThat(props.getProperty("extraCredentials")).isEqualTo("auths:YmFzaWM");
    }

    @Test
    void secretAddedAsSecondPairDelimitedBySemicolon() {
        // Trino JDBC extraCredentials delimits name:value pairs with SEMICOLONS.
        Properties props = TrinoDataStore.connectionProperties("svc", List.of("basic", "privileged"), "tok3n");
        assertThat(props.getProperty("extraCredentials")).isEqualTo("auths:YmFzaWMscHJpdmlsZWdlZA;secret:tok3n");
    }

    @Test
    void noValueContainsSpaces() {
        // Guards against the Trino JDBC "contains spaces or is not printable ASCII"
        // rejection of extraCredentials values.
        Properties props = TrinoDataStore.connectionProperties("svc", List.of("basic", "privileged"), "tok3n");
        assertThat(props.getProperty("extraCredentials")).doesNotContain(" ");
    }

    @Test
    void secretSentEvenWithoutAuths() {
        Properties props = TrinoDataStore.connectionProperties("svc", List.of(), "tok3n");
        assertThat(props.getProperty("extraCredentials")).isEqualTo("secret:tok3n");
    }

    @Test
    void authTokenContainingCommaIsRejected() {
        for (String bad : List.of("FOO,BAR", "")) {
            assertThatThrownBy(() ->
                TrinoDataStore.connectionProperties("svc", List.of("basic", bad), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("authorization token");
        }
    }
}
