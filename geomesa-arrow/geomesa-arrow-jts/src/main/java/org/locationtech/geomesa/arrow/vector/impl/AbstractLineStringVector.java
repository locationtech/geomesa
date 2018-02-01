/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector.impl;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.locationtech.geomesa.arrow.vector.GeometryVector;

import java.util.List;
import java.util.Map;

public abstract class AbstractLineStringVector implements GeometryVector<LineString, ListVector> {

  public static FieldType createFieldType(Map<String, String> metadata) {
    return new FieldType(true, ArrowType.List.INSTANCE, null, metadata);
  }

  private final ListVector vector;
  private final LineStringWriter writer;
  private final LineStringReader reader;

  protected AbstractLineStringVector(String name, BufferAllocator allocator, Map<String, String> metadata) {
    this(new ListVector(name, allocator, createFieldType(metadata), null));
  }

  protected AbstractLineStringVector(String name, AbstractContainerVector container, Map<String, String> metadata) {
    this(container.addOrGet(name, createFieldType(metadata), ListVector.class));
  }

  protected AbstractLineStringVector(ListVector vector) {
    // create the fields we will write to up front
    if (vector.getDataVector().equals(BaseRepeatedValueVector.DEFAULT_DATA_VECTOR)) {
      vector.initializeChildrenFromFields(getFields());
      vector.allocateNew();
    }
    this.vector = vector;
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

  @Override
  public void transfer(int fromIndex, int toIndex, GeometryVector<LineString, ListVector> to) {
    to.getWriter().set(toIndex, reader.get(fromIndex));
  }

  public static abstract class LineStringWriter extends AbstractGeometryWriter<LineString> {

    private final BitVector.Mutator nullSet;
    private final ListVector.Mutator mutator;
    private final FixedSizeListVector.Mutator tupleMutator;

    protected LineStringWriter(ListVector vector) {
      // the only way to access the bit vectors and set an index as null
      this.nullSet = ((BitVector)vector.getFieldInnerVectors().get(0)).getMutator();
      this.mutator = vector.getMutator();
      FixedSizeListVector tuples = (FixedSizeListVector) vector.getChildrenFromFields().get(0);
      this.tupleMutator = tuples.getMutator();
      setOrdinalMutator(tuples.getChildrenFromFields().get(0).getMutator());
    }

    @Override
    public void set(int index, LineString geom) {
      if (index == 0) {
        // need to do this to avoid issues with re-setting the value at index 0
        mutator.setLastSet(0);
      }
      int position = mutator.startNewValue(index);
      if (geom == null) {
        mutator.endValue(index, 0);
        nullSet.setSafe(index, 0);
      } else {
        for (int i = 0; i < geom.getNumPoints(); i++) {
          Coordinate p = geom.getCoordinateN(i);
          tupleMutator.setNotNull(position + i);
          writeOrdinal((position + i) * 2, p.y);
          writeOrdinal((position + i) * 2 + 1, p.x);
        }
        mutator.endValue(index, geom.getNumPoints());
      }
    }

    @Override
    public void setValueCount(int count) {
      mutator.setValueCount(count);
    }
  }

  public static abstract class LineStringReader extends AbstractGeometryReader<LineString> {

    private final ListVector.Accessor accessor;
    private final UInt4Vector.Accessor offsets;

    public LineStringReader(ListVector vector) {
      this.accessor = vector.getAccessor();
      this.offsets = ((UInt4Vector) vector.getFieldInnerVectors().get(1)).getAccessor();
      setOrdinalAccessor(vector.getChildrenFromFields().get(0).getChildrenFromFields().get(0).getAccessor());
    }

    @Override
    @SuppressWarnings("unchecked")
    public LineString get(int index) {
      if (accessor.isNull(index)) {
        return null;
      } else {
        int offsetStart = offsets.get(index);
        Coordinate[] coordinates = new Coordinate[offsets.get(index + 1) - offsetStart];
        for (int i = 0; i < coordinates.length; i++) {
          double y = readOrdinal((offsetStart + i) * 2);
          double x = readOrdinal((offsetStart + i) * 2 + 1);
          coordinates[i] = new Coordinate(x, y);
        }
        return factory.createLineString(coordinates);
      }
    }

    public int getStartOffset(int index) {
      return offsets.get(index);
    }

    public int getEndOffset(int index) {
      return offsets.get(index + 1);
    }

    public double getCoordinateY(int offset) {
      return readOrdinal(offset * 2);
    }

    public double getCoordinateX(int offset) {
      return readOrdinal(offset * 2 + 1);
    }

    @Override
    public int getValueCount() {
      return accessor.getValueCount();
    }

    @Override
    public int getNullCount() {
      int count = accessor.getNullCount();
      if (count < 0) {
        return 0;
      } else {
        return count;
      }
    }
  }
}
