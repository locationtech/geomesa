/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector.reader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NullableMapVector;

import java.util.List;

public class LineStringReader implements GeometryReader<LineString> {

  private final NullableMapVector.Accessor accessor;
  private final ListVector.Accessor xAccessor;
  private final ListVector.Accessor yAccessor;

  public LineStringReader(NullableMapVector vector, String xField, String yField) {
    this.accessor = vector.getAccessor();
    this.xAccessor = (ListVector.Accessor) vector.getChild(xField).getAccessor();
    this.yAccessor = (ListVector.Accessor) vector.getChild(yField).getAccessor();
  }

  @Override
  @SuppressWarnings("unchecked")
  public LineString get(int index) {
    if (accessor.isNull(index)) {
      return null;
    } else {
      List<Double> x = (List<Double>) xAccessor.getObject(index);
      List<Double> y = (List<Double>) yAccessor.getObject(index);
      if (x.size() != y.size()) {
        throw new IllegalArgumentException("Invalid point vectors: x: " + x.size() + " y: " + y.size());
      }
      Coordinate[] coordinates = new Coordinate[x.size()];
      for (int i = 0; i < coordinates.length; i++) {
        coordinates[i] = new Coordinate(x.get(i), y.get(i));
      }
      return factory.createLineString(coordinates);
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
  public void close() throws Exception {
  }
}
