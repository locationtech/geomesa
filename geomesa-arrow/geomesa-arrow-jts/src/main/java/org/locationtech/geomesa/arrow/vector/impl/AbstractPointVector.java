/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.vector.impl;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.locationtech.geomesa.arrow.vector.GeometryVector;

import java.util.List;

public abstract class AbstractPointVector implements GeometryVector<Point, FixedSizeListVector> {

  private final FixedSizeListVector vector;
  private final PointWriter writer;
  private final PointReader reader;

  protected AbstractPointVector(String name, BufferAllocator allocator) {
    this(new FixedSizeListVector(name, allocator, 2, null, null));
  }

  protected AbstractPointVector(String name, AbstractContainerVector container) {
    this(container.addOrGet(name, new FieldType(true, new ArrowType.FixedSizeList(2), null), FixedSizeListVector.class));
  }

  protected AbstractPointVector(FixedSizeListVector vector) {
    this.vector = vector;
    // create the fields we will write to up front
    if (vector.getDataVector().equals(ZeroVector.INSTANCE)) {
      vector.initializeChildrenFromFields(getFields());
      vector.allocateNew();
    }
    this.writer = createWriter(vector);
    this.reader = createReader(vector);
  }

  protected abstract List<Field> getFields();
  protected abstract PointWriter createWriter(FixedSizeListVector vector);
  protected abstract PointReader createReader(FixedSizeListVector vector);

  @Override
  public PointWriter getWriter() {
    return writer;
  }

  @Override
  public PointReader getReader() {
    return reader;
  }

  @Override
  public FixedSizeListVector getVector() {
    return vector;
  }

  @Override
  public void close() throws Exception {
    vector.close();
  }

  public static abstract class PointWriter implements GeometryWriter<Point> {

    private final FixedSizeListVector.Mutator mutator;
    private final FieldVector.Mutator pointMutator;

    protected PointWriter(FixedSizeListVector vector) {
      this.mutator = vector.getMutator();
      this.pointMutator = vector.getChildrenFromFields().get(0).getMutator();
    }

    @Override
    public void set(int index, Point geom) {
      if (geom != null) {
        mutator.setNotNull(index);
        writeOrdinal(pointMutator, index * 2, geom.getY());
        writeOrdinal(pointMutator, index * 2 + 1, geom.getX());
      }
    }

    protected abstract void writeOrdinal(FieldVector.Mutator mutator, int index, double ordinal);

    @Override
    public void setValueCount(int count) {
      mutator.setValueCount(count);
      pointMutator.setValueCount(count * 2);
    }
  }

  public static abstract class PointReader implements GeometryReader<Point> {

    private static final GeometryFactory factory = new GeometryFactory();

    private final UnionFixedSizeListReader reader;
    private final FieldReader subReader;
    private final FixedSizeListVector.Accessor accessor;

    protected PointReader(FixedSizeListVector vector) {
      this.reader = vector.getReader();
      this.subReader = reader.reader();
      this.accessor = vector.getAccessor();
    }

    @Override
    public Point get(int index) {
      reader.setPosition(index);
      if (reader.isSet()) {
        reader.next();
        double y = readOrdinal(subReader);
        reader.next();
        double x = readOrdinal(subReader);
        return factory.createPoint(new Coordinate(x, y));
      } else {
        return null;
      }
    }

    protected abstract double readOrdinal(FieldReader reader);

    @Override
    public int getValueCount() {
      return accessor.getValueCount();
    }

    @Override
    public int getNullCount() {
      return accessor.getNullCount();
    }
  }
}
