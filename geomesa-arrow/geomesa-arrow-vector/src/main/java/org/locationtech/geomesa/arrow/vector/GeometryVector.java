/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;

public interface GeometryVector<T extends Geometry> extends AutoCloseable {

  GeometryWriter<T> getWriter();
  GeometryReader<T> getReader();
  NullableMapVector getVector();

  @SuppressWarnings("unchecked")
  static <U extends Geometry> Class<U> typeOf(Field field) {
    List<Field> children = field.getChildren();
    if (PointVector.fields.equals(children)) {
      return (Class<U>) Point.class;
    } else if (LineStringVector.fields.equals(children)) {
      return (Class<U>) LineString.class;
    } else if (PolygonVector.fields.equals(children)) {
      return (Class<U>) Polygon.class;
    } else if (MultiLineStringVector.fields.equals(children)) {
      return (Class<U>) MultiLineString.class;
    } else if (MultiPointVector.fields.equals(children)) {
      return (Class<U>) MultiPoint.class;
    } else {
      return null;
    }
  }

  interface GeometryWriter<T extends Geometry> extends AutoCloseable {
    void set(T geom);
    void set(int i, T geom);
    void setValueCount(int count);
  }

  interface GeometryReader<T extends Geometry> extends AutoCloseable {
    T get(int i);
    int getValueCount();
    int getNullCount();
  }
}
