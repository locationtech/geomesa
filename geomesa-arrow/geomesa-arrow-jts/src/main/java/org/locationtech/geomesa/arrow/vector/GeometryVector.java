/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector;

import org.apache.arrow.vector.FieldVector;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

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
}
