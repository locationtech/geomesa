/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector.util;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.locationtech.geomesa.arrow.vector.GeometryVector.GeometryReader;

public abstract class BaseGeometryReader<T extends Geometry> implements GeometryReader<T> {

  protected static final GeometryFactory factory = new GeometryFactory();

  private final NullableMapVector.Accessor accessor;

  public BaseGeometryReader(NullableMapVector vector) {
    this.accessor = vector.getAccessor();
  }

  protected abstract T readGeometry(int index);

  @Override
  public T get(int index) {
    if (accessor.isNull(index)) {
      return null;
    } else {
      return readGeometry(index);
    }
  }

  @Override
  public int getValueCount() {
    return accessor.getValueCount();
  }

  @Override
  public int getNullCount() {
    return accessor.getNullCount();
  }

  @Override
  public void close() throws Exception {}
}
