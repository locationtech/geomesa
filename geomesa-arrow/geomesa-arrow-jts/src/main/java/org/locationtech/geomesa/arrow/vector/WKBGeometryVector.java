/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullableVarBinaryVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Catch-all for storing instances of Geometry as WKB
 */
public class WKBGeometryVector implements GeometryVector<Geometry, NullableVarBinaryVector> {
  private final NullableVarBinaryVector vector;

  private WKBGeometryWriter writer = null;
  private WKBGeometryReader reader = null;

  public static final Field field = Field.nullablePrimitive("wkb", ArrowType.Binary.INSTANCE);

  public static FieldType createFieldType(Map<String, String> metadata) {
    return new FieldType(true, ArrowType.Binary.INSTANCE, null, metadata);
  }

  public WKBGeometryVector(String name, BufferAllocator allocator, @Nullable Map<String, String> metadata) {
    this(new NullableVarBinaryVector(name, allocator));
  }

  public WKBGeometryVector(NullableVarBinaryVector vector) {
    this.vector = vector;
  }

  @Override
  public GeometryWriter<Geometry> getWriter() {
    if (writer == null) {
      writer = new WKBGeometryWriter(vector);
    }
    return writer;
  }

  @Override
  public GeometryReader<Geometry> getReader() {
    if (reader == null) {
      reader = new WKBGeometryReader(vector);
    }
    return reader;
  }

  public static class WKBGeometryWriter implements GeometryWriter<Geometry> {

    private final NullableVarBinaryVector.Mutator mutator;
    private final WKBWriter writer = new WKBWriter();

    public WKBGeometryWriter(NullableVarBinaryVector vector) {
      this.mutator = vector.getMutator();
    }

    @Override
    public void set(int i, Geometry geom) {
      if (geom == null) {
        mutator.setNull(i);
      } else {
        byte[] wkb = writer.write(geom);
        mutator.setSafe(i, wkb, 0, wkb.length);
      }
    }

    @Override
    public void setValueCount(int count) {
       mutator.setValueCount(count);
    }
  }

  public static class WKBGeometryReader implements GeometryReader<Geometry> {

    private final NullableVarBinaryVector.Accessor accessor;
    private final WKBReader reader = new WKBReader();

    public WKBGeometryReader(NullableVarBinaryVector vector) {
      this.accessor  = vector.getAccessor();
    }

    @Override
    public Geometry get(int i) {
      if (accessor.isNull(i)) {
        return null;
      } else {
        Geometry geometry;
        try {
          geometry = reader.read(accessor.get(i));
        } catch (ParseException exception) {
          throw new RuntimeException(exception);
        }
        return geometry;
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

  @Override
  public NullableVarBinaryVector getVector() {
    return vector;
  }

  @Override
  public void transfer(int fromIndex, int toIndex, GeometryVector<Geometry, NullableVarBinaryVector> to) {
    NullableVarBinaryVector toVector = to.getVector();
    if (vector.getAccessor().isNull(fromIndex)) {
      toVector.getMutator().setNull(toIndex);
    } else {
      byte[] wkb = vector.getAccessor().get(fromIndex);
      toVector.getMutator().setSafe(toIndex, wkb, 0, wkb.length);
    }
  }

  @Override
  public void close() throws Exception {
    vector.close();
  }
}
