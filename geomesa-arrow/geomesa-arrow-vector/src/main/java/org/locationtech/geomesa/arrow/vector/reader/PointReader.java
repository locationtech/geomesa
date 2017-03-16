/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector.reader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Point;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.complex.NullableMapVector;

public class PointReader implements GeometryReader<Point> {

  private final NullableMapVector vector;
  private final NullableFloat8Vector.Accessor xAccessor;
  private final NullableFloat8Vector.Accessor yAccessor;

  public PointReader(NullableMapVector vector) {
    this.vector = vector;
    this.xAccessor = (NullableFloat8Vector.Accessor) vector.getChild("x").getAccessor();
    this.yAccessor = (NullableFloat8Vector.Accessor) vector.getChild("y").getAccessor();
  }

  @Override
  public Point get(int i) {
    if (vector.getAccessor().isNull(i)) {
      return null;
    } else {
      double x = xAccessor.getObject(i);
      double y = yAccessor.getObject(i);
      return factory.createPoint(new Coordinate(x, y));
    }
  }

  @Override
  public int getValueCount() {
    return vector.getAccessor().getValueCount();
  }

  @Override
  public int getNullCount() {
    return vector.getAccessor().getNullCount();
  }

  @Override
  public void close() throws Exception {
  }
}
