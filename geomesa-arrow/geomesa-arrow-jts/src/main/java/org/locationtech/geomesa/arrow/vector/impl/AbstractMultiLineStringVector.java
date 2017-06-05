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
import com.vividsolutions.jts.geom.MultiLineString;
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

public abstract class AbstractMultiLineStringVector implements GeometryVector<MultiLineString, ListVector> {

  private final ListVector vector;
  private final MultiLineStringWriter writer;
  private final MultiLineStringReader reader;

  protected AbstractMultiLineStringVector(String name, BufferAllocator allocator) {
    this(new ListVector(name, allocator, null, null));
  }

  protected AbstractMultiLineStringVector(String name, AbstractContainerVector container) {
    this(container.addOrGet(name, new FieldType(true, ArrowType.List.INSTANCE, null), ListVector.class));
  }

  protected AbstractMultiLineStringVector(ListVector vector) {
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
  protected abstract MultiLineStringWriter createWriter(ListVector vector);
  protected abstract MultiLineStringReader createReader(ListVector vector);

  @Override
  public MultiLineStringWriter getWriter() {
    return writer;
  }

  @Override
  public MultiLineStringReader getReader() {
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

  public static abstract class MultiLineStringWriter implements GeometryWriter<MultiLineString> {

    private final ListVector.Mutator mutator;
    private final ListVector.Mutator innerMutator;
    private final FixedSizeListVector.Mutator tupleMutator;
    private final FieldVector.Mutator pointMutator;

    protected MultiLineStringWriter(ListVector vector) {
      ListVector innerList = (ListVector) vector.getChildrenFromFields().get(0);
      FixedSizeListVector tuples = (FixedSizeListVector) innerList.getChildrenFromFields().get(0);
      this.mutator = vector.getMutator();
      this.innerMutator = innerList.getMutator();
      this.tupleMutator = tuples.getMutator();
      this.pointMutator = tuples.getChildrenFromFields().get(0).getMutator();
    }

    @Override
    public void set(int index, MultiLineString geom) {
      if (geom != null) {
        int innerIndex = mutator.startNewValue(index);
        for (int i = 0; i < geom.getNumGeometries(); i++) {
          LineString line = (LineString) geom.getGeometryN(i);
          int position = innerMutator.startNewValue(innerIndex + i);
          for (int j = 0; j < line.getNumPoints(); j++) {
            Coordinate p = line.getCoordinateN(j);
            tupleMutator.setNotNull(position + j);
            writeOrdinal(pointMutator, (position + j) * 2, p.y);
            writeOrdinal(pointMutator, (position + j) * 2 + 1, p.x);
          }
          innerMutator.endValue(innerIndex + i, line.getNumPoints());
        }
        mutator.endValue(index, geom.getNumGeometries());
      }
    }

    protected abstract void writeOrdinal(FieldVector.Mutator mutator, int index, double ordinal);

    @Override
    public void setValueCount(int count) {
      mutator.setValueCount(count);
    }
  }

  public static abstract class MultiLineStringReader implements GeometryReader<MultiLineString> {

    private final ListVector.Accessor accessor;
    private final UnionListReader reader;
    private final UnionListReader innerReader;
    private final UnionFixedSizeListReader innerInnerReader;
    private final FieldReader ordinalReader;

    public MultiLineStringReader(ListVector vector) {
      ListVector innerVector = (ListVector) vector.getChildrenFromFields().get(0);
      this.accessor = vector.getAccessor();
      this.reader = vector.getReader();
      this.innerReader = innerVector.getReader();
      this.innerInnerReader = ((FixedSizeListVector) innerVector.getChildrenFromFields().get(0)).getReader();
      this.ordinalReader = innerInnerReader.reader();
    }

    @Override
    @SuppressWarnings("unchecked")
    public MultiLineString get(int index) {
      reader.setPosition(index);
      if (reader.isSet()) {
        LineString[] lines = new LineString[reader.size()];
        for (int i = 0; i < lines.length; i++) {
          reader.next();
          Coordinate[] coordinates = new Coordinate[innerReader.size()];
          for (int j = 0; j < coordinates.length; j++) {
            innerReader.next();
            innerInnerReader.next();
            double y = readOrdinal(ordinalReader);
            innerInnerReader.next();
            double x = readOrdinal(ordinalReader);
            coordinates[j] = new Coordinate(x, y);
          }
          lines[i] = factory.createLineString(coordinates);
        }
        return factory.createMultiLineString(lines);
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
