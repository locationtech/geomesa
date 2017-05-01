/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.vector.impl;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
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

public abstract class AbstractLineStringVector implements GeometryVector<LineString, ListVector> {

  private final ListVector vector;
  private final LineStringWriter writer;
  private final LineStringReader reader;

  protected AbstractLineStringVector(String name, BufferAllocator allocator) {
    this(new ListVector(name, allocator, null, null));
  }

  protected AbstractLineStringVector(String name, AbstractContainerVector container) {
    this(container.addOrGet(name, new FieldType(true, ArrowType.List.INSTANCE, null), ListVector.class));
  }

  protected AbstractLineStringVector(ListVector vector) {
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
  protected abstract LineStringWriter createWriter(ListVector vector);
  protected abstract LineStringReader createReader(ListVector vector);

  @Override
  public LineStringWriter getWriter() {
    return writer;
  }

  @Override
  public LineStringReader getReader() {
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

  public static abstract class LineStringWriter implements GeometryWriter<LineString> {

    private final ListVector.Mutator mutator;
    private final FixedSizeListVector.Mutator tupleMutator;
    private final FieldVector.Mutator pointMutator;

    protected LineStringWriter(ListVector vector) {
      this.mutator = vector.getMutator();
      FixedSizeListVector tuples = (FixedSizeListVector) vector.getChildrenFromFields().get(0);
      this.tupleMutator = tuples.getMutator();
      this.pointMutator = tuples.getChildrenFromFields().get(0).getMutator();
    }

    @Override
    public void set(int index, LineString geom) {
      if (geom != null) {
        int position = mutator.startNewValue(index);
        for (int i = 0; i < geom.getNumPoints(); i++) {
          Coordinate p = geom.getCoordinateN(i);
          tupleMutator.setNotNull(position + i);
          writeOrdinal(pointMutator, (position + i) * 2, p.y);
          writeOrdinal(pointMutator, (position + i) * 2 + 1, p.x);
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

  public static abstract class LineStringReader implements GeometryReader<LineString> {

    private final ListVector.Accessor accessor;
    private final UnionListReader reader;
    private final UnionFixedSizeListReader subReader;
    private final FieldReader ordinalReader;

    public LineStringReader(ListVector vector) {
      this.accessor = vector.getAccessor();
      this.reader = vector.getReader();
      this.subReader = ((FixedSizeListVector) vector.getChildrenFromFields().get(0)).getReader();
      this.ordinalReader = subReader.reader();
    }

    @Override
    @SuppressWarnings("unchecked")
    public LineString get(int index) {
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
        return factory.createLineString(coordinates);
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
