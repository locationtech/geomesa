/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import org.apache.arrow.vector.FieldVector;

/**
 * Complex vector for geometries
 *
 * @param <T> geometry type
 * @param <V> underlying vector type
 */
public interface GeometryVector<T extends Geometry, V extends FieldVector> extends AutoCloseable {

  static final GeometryFactory factory = new GeometryFactory();

  GeometryWriter<T> getWriter();
  GeometryReader<T> getReader();
  V getVector();

  interface GeometryWriter<T extends Geometry> {
    void set(int i, T geom);
    void setValueCount(int count);
  }

  interface GeometryReader<T extends Geometry> {
    T get(int i);
    int getValueCount();
    int getNullCount();
  }
}
