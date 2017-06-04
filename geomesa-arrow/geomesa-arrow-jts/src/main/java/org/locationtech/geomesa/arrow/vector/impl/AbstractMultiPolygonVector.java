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
import com.vividsolutions.jts.geom.MultiPolygon;
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

public abstract class AbstractMultiPolygonVector implements GeometryVector<MultiPolygon, ListVector> {

  private static FieldType createFieldType(Map<String, String> metadata) {
    return new FieldType(true, ArrowType.List.INSTANCE, null, metadata);
  }

  private final ListVector vector;
  private final MultiPolygonWriter writer;
  private final MultiPolygonReader reader;

  protected AbstractMultiPolygonVector(String name, BufferAllocator allocator, Map<String, String> metadata) {
    this(new ListVector(name, allocator, createFieldType(metadata), null));
  }

  protected AbstractMultiPolygonVector(String name, AbstractContainerVector container, Map<String, String> metadata) {
    this(container.addOrGet(name, createFieldType(metadata), ListVector.class));
  }

  protected AbstractMultiPolygonVector(ListVector vector) {
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
  protected abstract MultiPolygonWriter createWriter(ListVector vector);
  protected abstract MultiPolygonReader createReader(ListVector vector);

  @Override
  public MultiPolygonWriter getWriter() {
    return writer;
  }

  @Override
  public MultiPolygonReader getReader() {
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

  public static abstract class MultiPolygonWriter extends AbstractGeometryWriter<MultiPolygon> {

    private final ListVector.Mutator mutator;
    private final ListVector.Mutator innerMutator;
    private final ListVector.Mutator innerInnerMutator;
    private final FixedSizeListVector.Mutator tupleMutator;

    protected MultiPolygonWriter(ListVector vector) {
      ListVector innerList = (ListVector) vector.getChildrenFromFields().get(0);
      ListVector innerInnerList = (ListVector) innerList.getChildrenFromFields().get(0);
      FixedSizeListVector tuples = (FixedSizeListVector) innerInnerList.getChildrenFromFields().get(0);
      this.mutator = vector.getMutator();
      this.innerMutator = innerList.getMutator();
      this.innerInnerMutator = innerInnerList.getMutator();
      this.tupleMutator = tuples.getMutator();
      setOrdinalMutator(tuples.getChildrenFromFields().get(0).getMutator());
    }

    @Override
    public void set(int index, MultiPolygon geom) {
      if (geom != null) {
        int innerIndex = mutator.startNewValue(index);
        for (int i = 0; i < geom.getNumGeometries(); i++) {
          Polygon poly = (Polygon) geom.getGeometryN(i);
          int innerInnerIndex = innerMutator.startNewValue(innerIndex + i);
          for (int j = 0; j < poly.getNumInteriorRing() + 1; j++) {
            LineString line = j == 0 ? poly.getExteriorRing() : poly.getInteriorRingN(j - 1);
            int position = innerInnerMutator.startNewValue(innerInnerIndex + j);
            for (int k = 0; k < line.getNumPoints(); k++) {
              Coordinate p = line.getCoordinateN(k);
              tupleMutator.setNotNull(position + k);
              writeOrdinal((position + k) * 2, p.y);
              writeOrdinal((position + k) * 2 + 1, p.x);
            }
            innerInnerMutator.endValue(innerInnerIndex + j, line.getNumPoints());
          }
          innerMutator.endValue(innerIndex + i, poly.getNumInteriorRing() + 1);
        }
        mutator.endValue(index, geom.getNumGeometries());
      }
    }

    @Override
    public void setValueCount(int count) {
      mutator.setValueCount(count);
    }
  }

  public static abstract class MultiPolygonReader extends AbstractGeometryReader<MultiPolygon> {

    private final ListVector.Accessor accessor;
    private final UInt4Vector.Accessor outerOuterOffsets;
    private final UInt4Vector.Accessor outerOffsets;
    private final UInt4Vector.Accessor offsets;

    public MultiPolygonReader(ListVector vector) {
      this.accessor = vector.getAccessor();
      this.outerOuterOffsets = ((UInt4Vector) vector.getFieldInnerVectors().get(1)).getAccessor();
      FieldVector innerList = vector.getChildrenFromFields().get(0);
      this.outerOffsets = ((UInt4Vector) innerList.getFieldInnerVectors().get(1)).getAccessor();
      FieldVector innerInnerList = innerList.getChildrenFromFields().get(0);
      this.offsets = ((UInt4Vector) innerInnerList.getFieldInnerVectors().get(1)).getAccessor();
      setOrdinalAccessor(innerInnerList.getChildrenFromFields().get(0).getChildrenFromFields().get(0).getAccessor());
    }

    @Override
    @SuppressWarnings("unchecked")
    public MultiPolygon get(int index) {
      if (accessor.isNull(index)) {
        return null;
      } else {
        int outerOuterOffsetStart = outerOuterOffsets.get(index);
        Polygon[] polygons = new Polygon[outerOuterOffsets.get(index + 1) - outerOuterOffsetStart];
        for (int k = 0; k < polygons.length; k++) {
          int outerOffsetStart = outerOffsets.get(outerOuterOffsetStart + k);
          LinearRing shell = null;
          LinearRing[] holes = new LinearRing[outerOffsets.get(outerOuterOffsetStart + k + 1) - outerOffsetStart - 1];
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
          polygons[k] = factory.createPolygon(shell, holes);
        }
        return factory.createMultiPolygon(polygons);
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
