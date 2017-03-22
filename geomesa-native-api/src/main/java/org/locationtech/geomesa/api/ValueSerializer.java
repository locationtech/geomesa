/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.api;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * Required by GeoMesaIndex in order to serialize
 * a value object into the index
 * @param <T>
 */
@InterfaceStability.Unstable
public interface ValueSerializer<T> {

    byte[] toBytes(T t);

    T fromBytes(byte[] bytes);
}
