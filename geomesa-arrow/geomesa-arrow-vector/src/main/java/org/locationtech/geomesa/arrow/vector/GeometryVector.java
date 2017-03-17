/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.vector.reader.GeometryReader;
import org.locationtech.geomesa.arrow.vector.writer.GeometryWriter;

import java.util.List;

public interface GeometryVector<T extends Geometry> extends AutoCloseable {
  public GeometryWriter<T> getWriter();
  public GeometryReader<T> getReader();
  public NullableMapVector getVector();

  public static <U extends Geometry> Class<U> typeOf(Field field) {
    if (field.getType() instanceof ArrowType.Struct) {
      List<Field> children = field.getChildren();
      // TODO better checks
      if (children.size() == 2 && "x".equals(children.get(0).getName()) && "y".equals(children.get(1).getName())) {
        return (Class<U>) Point.class;
      } else {
        return null;
      }
    } else {
      return null;
    }
  }
}
