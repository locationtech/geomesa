/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.apache.arrow.vector.FieldVector;

/**
 * Complex vector for geometries
 *
 * @param <T> geometry type
 * @param <V> underlying vector type
 */
public interface GeometryVector<T extends Geometry, V extends FieldVector> extends AutoCloseable {

  GeometryFactory factory = new GeometryFactory();

  V getVector();

  void set(int i, T geom);
  void setValueCount(int count);

  T get(int i);
  int getValueCount();
  int getNullCount();

  void transfer(int fromIndex, int toIndex, GeometryVector<T, V> to);

  @Deprecated
  GeometryWriter<T> getWriter();

  @Deprecated
  GeometryReader<T> getReader();

  @Deprecated
  interface GeometryWriter<T extends Geometry> {
    void set(int i, T geom);
    void setValueCount(int count);
  }

  @Deprecated
  interface GeometryReader<T extends Geometry> {
    T get(int i);
    int getValueCount();
    int getNullCount();
  }
}
