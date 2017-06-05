/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector.impl;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
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

public abstract class AbstractPolygonVector implements GeometryVector<Polygon, ListVector> {

  private static FieldType createFieldType(Map<String, String> metadata) {
    return new FieldType(true, ArrowType.List.INSTANCE, null, metadata);
  }

  private final ListVector vector;
  private final PolygonWriter writer;
  private final PolygonReader reader;

  protected AbstractPolygonVector(String name, BufferAllocator allocator, Map<String, String> metadata) {
    this(new ListVector(name, allocator, createFieldType(metadata), null));
  }

  protected AbstractPolygonVector(String name, AbstractContainerVector container, Map<String, String> metadata) {
    this(container.addOrGet(name, createFieldType(metadata), ListVector.class));
  }

  protected AbstractPolygonVector(ListVector vector) {
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
  protected abstract PolygonWriter createWriter(ListVector vector);
  protected abstract PolygonReader createReader(ListVector vector);

  @Override
  public PolygonWriter getWriter() {
    return writer;
  }

  @Override
  public PolygonReader getReader() {
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

  public static abstract class PolygonWriter extends AbstractGeometryWriter<Polygon> {

    private final ListVector.Mutator mutator;
    private final ListVector.Mutator innerMutator;
    private final FixedSizeListVector.Mutator tupleMutator;

    protected PolygonWriter(ListVector vector) {
      ListVector innerList = (ListVector) vector.getChildrenFromFields().get(0);
      FixedSizeListVector tuples = (FixedSizeListVector) innerList.getChildrenFromFields().get(0);
      this.mutator = vector.getMutator();
      this.innerMutator = innerList.getMutator();
      this.tupleMutator = tuples.getMutator();
      setOrdinalMutator(tuples.getChildrenFromFields().get(0).getMutator());
    }

    @Override
    public void set(int index, Polygon geom) {
      if (geom != null) {
        int innerIndex = mutator.startNewValue(index);
        for (int i = 0; i < geom.getNumInteriorRing() + 1; i++) {
          LineString line = i == 0 ? geom.getExteriorRing() : geom.getInteriorRingN(i - 1);
          int position = innerMutator.startNewValue(innerIndex + i);
          for (int j = 0; j < line.getNumPoints(); j++) {
            Coordinate p = line.getCoordinateN(j);
            tupleMutator.setNotNull(position + j);
            writeOrdinal((position + j) * 2, p.y);
            writeOrdinal((position + j) * 2 + 1, p.x);
          }
          innerMutator.endValue(innerIndex + i, line.getNumPoints());
        }
        mutator.endValue(index, geom.getNumInteriorRing() + 1);
      }
    }

    @Override
    public void setValueCount(int count) {
      mutator.setValueCount(count);
    }
  }

  public static abstract class PolygonReader extends AbstractGeometryReader<Polygon> {

    private final ListVector.Accessor accessor;
    private final UInt4Vector.Accessor outerOffsets;
    private final UInt4Vector.Accessor offsets;

    public PolygonReader(ListVector vector) {
      this.accessor = vector.getAccessor();
      this.outerOffsets = ((UInt4Vector) vector.getFieldInnerVectors().get(1)).getAccessor();
      FieldVector innerList = vector.getChildrenFromFields().get(0);
      this.offsets = ((UInt4Vector) innerList.getFieldInnerVectors().get(1)).getAccessor();
      setOrdinalAccessor(innerList.getChildrenFromFields().get(0).getChildrenFromFields().get(0).getAccessor());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Polygon get(int index) {
      if (accessor.isNull(index)) {
        return null;
      } else {
        int outerOffsetStart = outerOffsets.get(index);
        LinearRing shell = null;
        LinearRing[] holes = new LinearRing[outerOffsets.get(index + 1) - outerOffsetStart - 1];
        for (int j = 0; j < holes.length + 1; j++) {
          int offsetStart = offsets.get(outerOffsetStart + j);
          Coordinate[] coordinates = new Coordinate[offsets.get(outerOffsetStart + j + 1) - offsetStart];
          for (int i = 0; i < coordinates.length; i++) {
            double y = readOrdinal((offsetStart + i) * 2);
            double x = readOrdinal((offsetStart + i) * 2 + 1);
            coordinates[i] = new Coordinate(x, y);
          }
          LinearRing ring = factory.createLinearRing(coordinates);
          if (j == 0) {
            shell = ring;
          } else {
            holes[j - 1] = ring;
          }
        }
        return factory.createPolygon(shell, holes);
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
  }
}
