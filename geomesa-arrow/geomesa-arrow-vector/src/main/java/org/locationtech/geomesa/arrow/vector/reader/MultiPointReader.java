/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector.reader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.MultiPoint;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NullableMapVector;

import java.util.List;

public class MultiPointReader implements GeometryReader<MultiPoint> {

  private final NullableMapVector.Accessor accessor;
  private final ListVector.Accessor xAccessor;
  private final ListVector.Accessor yAccessor;

  public MultiPointReader(NullableMapVector vector, String xField, String yField) {
    this.accessor = vector.getAccessor();
    this.xAccessor = (ListVector.Accessor) vector.getChild(xField).getAccessor();
    this.yAccessor = (ListVector.Accessor) vector.getChild(yField).getAccessor();
  }

  @Override
  @SuppressWarnings("unchecked")
  public MultiPoint get(int i) {
    if (accessor.isNull(i)) {
      return null;
    } else {
      List<Double> xs = (List<Double>) xAccessor.getObject(i);
      List<Double> ys = (List<Double>) yAccessor.getObject(i);
      if (xs.size() != ys.size()) {
        throw new IllegalArgumentException("Invalid point vectors: x: " + xs.size() + " y: " + ys.size());
      }
      Coordinate[] coordinates = new Coordinate[xs.size()];
      for (int j = 0; j < coordinates.length; j++) {
        coordinates[j] = new Coordinate(xs.get(j), ys.get(j));
      }
      return factory.createMultiPoint(coordinates);
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
