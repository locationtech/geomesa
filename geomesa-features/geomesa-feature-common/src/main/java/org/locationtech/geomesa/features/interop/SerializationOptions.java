/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.interop;

import org.locationtech.geomesa.features.SerializationOption;
import org.locationtech.geomesa.features.SerializationOption$;

public class SerializationOptions {
    public static scala.collection.immutable.Set<SerializationOption$.Value> withUserData() {
        return SerializationOption.SerializationOptions$.MODULE$.withUserData();
    }

    public static scala.collection.immutable.Set<SerializationOption$.Value> withoutId() {
        return SerializationOption.SerializationOptions$.MODULE$.withoutId();
    }
}
