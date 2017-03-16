/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.complex.impl.NullableMapWriter;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.apache.arrow.vector.types.Types.MinorType;

public class PointVector implements AutoCloseable {

  private static final GeometryFactory factory = new GeometryFactory();

  private final NullableMapVector vector;
  private final NullableMapWriter writer;

  private final NullableFloat8Vector xVector;
  private final NullableFloat8Vector yVector;
  private final Float8Writer xWriter;
  private final Float8Writer yWriter;

  public static PointVector wrap(NullableMapVector vector) {
    return new PointVector(vector);
  }

  public PointVector(String name, BufferAllocator allocator) {
    this(new NullableMapVector(name, allocator, null, null));
    vector.allocateNew();
  }

  private PointVector(NullableMapVector vector) {
    this.vector = vector;
    this.xVector = vector.addOrGet("x", MinorType.FLOAT8, NullableFloat8Vector.class, null);
    this.yVector = vector.addOrGet("y", MinorType.FLOAT8, NullableFloat8Vector.class, null);
    this.writer = new NullableMapWriter(vector);
    this.xWriter = writer.float8("x");
    this.yWriter = writer.float8("y");
  }

  public void set(int i, Point p) {
    if (p == null) {
      vector.getMutator().setNull(i);
    } else {
      vector.getMutator().setIndexDefined(i);
      writer.setPosition(i);
      xWriter.writeFloat8(p.getX());
      yWriter.writeFloat8(p.getY());
    }
  }

  public Point get(int i) {
    if (vector.getAccessor().isNull(i)) {
      return null;
    } else {
      double x = xVector.getAccessor().getObject(i);
      double y = yVector.getAccessor().getObject(i);
      return factory.createPoint(new Coordinate(x, y));
    }
  }

  public void setValueCount(int count) {
    vector.getMutator().setValueCount(count);
  }

  public int getValueCount() {
    return vector.getAccessor().getValueCount();
  }

  public FieldVector getVector() {
    return vector;
  }

  @Override
  public void close() throws Exception {
    writer.close();
    vector.close();
  }
}
