/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.trino.security;

import io.trino.plugin.base.security.ForwardingConnectorAccessControl;
import io.trino.spi.connector.ConnectorAccessControl;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tripwire: every SPI method must forward to the configured policy or add visibility enforcement. */
class VisibilityAccessControlAllowAllTest {

    @Test
    void everyConnectorAccessControlMethodIsForwardedOrOverridden() {
        List<String> notOverridden = Arrays.stream(ConnectorAccessControl.class.getMethods())
            .filter(m -> m.getDeclaringClass() == ConnectorAccessControl.class)
            .filter(m -> !m.isSynthetic())
            .filter(m -> !isDeclaredOn(VisibilityAccessControl.class, m)
                && !isDeclaredOn(ForwardingConnectorAccessControl.class, m))
            .map(VisibilityAccessControlAllowAllTest::signature)
            .collect(Collectors.toList());

        assertThat(notOverridden)
            .as("ConnectorAccessControl methods overridden by neither VisibilityAccessControl "
                + "nor ForwardingConnectorAccessControl")
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
