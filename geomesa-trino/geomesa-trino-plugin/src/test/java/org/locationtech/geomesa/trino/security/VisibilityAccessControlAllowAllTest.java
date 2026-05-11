/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;

import io.trino.spi.connector.ConnectorAccessControl;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tripwire: {@link ConnectorAccessControl} methods DENY by default, so
 * {@link VisibilityAccessControl} must override every one to stay additive
 * (allow-all + row filter). If a Trino upgrade adds a new SPI method, this test
 * fails — forcing a conscious allow decision rather than silently locking the
 * {@code spatial_iceberg} catalog.
 */
class VisibilityAccessControlAllowAllTest {

    @Test
    void overridesEveryConnectorAccessControlMethod() {
        List<String> notOverridden = Arrays.stream(ConnectorAccessControl.class.getMethods())
            .filter(m -> m.getDeclaringClass() == ConnectorAccessControl.class)
            .filter(m -> !m.isSynthetic())
            .filter(m -> !isDeclaredOn(VisibilityAccessControl.class, m))
            .map(VisibilityAccessControlAllowAllTest::signature)
            .collect(Collectors.toList());

        assertThat(notOverridden)
            .as("ConnectorAccessControl methods not overridden by VisibilityAccessControl "
                + "(each DENIES by default — override to allow, or the catalog locks down)")
            .isEmpty();
    }

    private static boolean isDeclaredOn(Class<?> type, Method m) {
        try {
            type.getDeclaredMethod(m.getName(), m.getParameterTypes());
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    private static String signature(Method m) {
        return m.getName() + Arrays.stream(m.getParameterTypes())
            .map(Class::getSimpleName).collect(Collectors.joining(",", "(", ")"));
    }
}
