/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.features.interop;

import org.locationtech.geomesa.features.SerializationOption;
import scala.Enumeration;

public class SerializationOptions {
    public static scala.collection.immutable.Set<Enumeration.Value> withUserData() {
        return SerializationOption.builder().withUserData().build();
    }

    public static scala.collection.immutable.Set<Enumeration.Value> withoutId() {
        return SerializationOption.builder().withoutId().build();
    }
}
