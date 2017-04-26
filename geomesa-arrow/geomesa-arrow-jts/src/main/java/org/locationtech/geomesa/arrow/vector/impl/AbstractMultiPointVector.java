/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.vector.impl;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListReader;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.locationtech.geomesa.arrow.vector.GeometryVector;

import java.util.List;

public abstract class AbstractMultiPointVector implements GeometryVector<MultiPoint, ListVector> {

  private final ListVector vector;
  private final MultiPointWriter writer;
  private final MultiPointReader reader;

  protected AbstractMultiPointVector(String name, BufferAllocator allocator) {
    this(new ListVector(name, allocator, null, null));
  }

  protected AbstractMultiPointVector(String name, AbstractContainerVector container) {
    this(container.addOrGet(name, new FieldType(true, ArrowType.List.INSTANCE, null), ListVector.class));
  }

  protected AbstractMultiPointVector(ListVector vector) {
    this.vector = vector;
    // create the fields we will write to up front
    if (vector.getDataVector().equals(BaseRepeatedValueVector.DEFAULT_DATA_VECTOR)) {
      vector.initializeChildrenFromFields(getFields());
      this.vector.allocateNew();
    }
    this.writer = createWriter(vector);
    this.reader = createReader(vector);
  }

  protected abstract List<Field> getFields();
  protected abstract MultiPointWriter createWriter(ListVector vector);
  protected abstract MultiPointReader createReader(ListVector vector);

  @Override
  public MultiPointWriter getWriter() {
    return writer;
  }

  @Override
  public MultiPointReader getReader() {
    return reader;
  }

  @Override
  public ListVector getVector() {
    return vector;
  }

  @Override
  public void close() throws Exception {
    vector.close();
  }

  public static abstract class MultiPointWriter implements GeometryWriter<MultiPoint> {

    private final ListVector.Mutator mutator;
    private final FixedSizeListVector.Mutator tupleMutator;
    private final FieldVector.Mutator pointMutator;

    protected MultiPointWriter(ListVector vector) {
      this.mutator = vector.getMutator();
      FixedSizeListVector tuples = (FixedSizeListVector) vector.getChildrenFromFields().get(0);
      this.tupleMutator = tuples.getMutator();
      this.pointMutator = tuples.getChildrenFromFields().get(0).getMutator();
    }

    @Override
    public void set(int index, MultiPoint geom) {
      if (geom != null) {
        int position = mutator.startNewValue(index);
        for (int i = 0; i < geom.getNumPoints(); i++) {
          Point p = (Point) geom.getGeometryN(i);
          tupleMutator.setNotNull(position + i);
          writeOrdinal(pointMutator, (position + i) * 2, p.getY());
          writeOrdinal(pointMutator, (position + i) * 2 + 1, p.getX());
        }
        mutator.endValue(index, geom.getNumPoints());
      }
    }

    protected abstract void writeOrdinal(FieldVector.Mutator mutator, int index, double ordinal);

    @Override
    public void setValueCount(int count) {
      mutator.setValueCount(count);
    }
  }

  public static abstract class MultiPointReader implements GeometryReader<MultiPoint> {

    private final ListVector.Accessor accessor;
    private final UnionListReader reader;
    private final UnionFixedSizeListReader subReader;
    private final FieldReader ordinalReader;

    public MultiPointReader(ListVector vector) {
      this.accessor = vector.getAccessor();
      this.reader = vector.getReader();
      this.subReader = ((FixedSizeListVector) vector.getChildrenFromFields().get(0)).getReader();
      this.ordinalReader = subReader.reader();
    }

    @Override
    @SuppressWarnings("unchecked")
    public MultiPoint get(int index) {
      reader.setPosition(index);
      if (reader.isSet()) {
        Coordinate[] coordinates = new Coordinate[reader.size()];
        for (int i = 0; i < coordinates.length; i++) {
          reader.next();
          subReader.next();
          double y = readOrdinal(ordinalReader);
          subReader.next();
          double x = readOrdinal(ordinalReader);
          coordinates[i] = new Coordinate(x, y);
        }
        return factory.createMultiPoint(coordinates);
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
