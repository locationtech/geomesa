/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> f586fec5a3 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 847c6dae88 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.jts;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Catch-all for storing instances of Geometry as WKB
 */
public class WKBGeometryVector implements GeometryVector<Geometry, VarBinaryVector> {
  private VarBinaryVector vector;
  private WKBWriter writer = null;
  private WKBReader reader = null;

  public static final Field field = Field.nullablePrimitive("wkb", ArrowType.Binary.INSTANCE);

  public static FieldType createFieldType(Map<String, String> metadata) {
    return new FieldType(true, ArrowType.Binary.INSTANCE, null, metadata);
  }

  public WKBGeometryVector(String name, BufferAllocator allocator, @Nullable Map<String, String> metadata) {
    this.vector = new VarBinaryVector(name, createFieldType(metadata), allocator);
  }

  public WKBGeometryVector(String name, AbstractContainerVector container, @Nullable Map<String, String> metadata) {
    this.vector = container.addOrGet(name, createFieldType(metadata), VarBinaryVector.class);
  }

  public WKBGeometryVector(VarBinaryVector vector) {
    this.vector = vector;
  }

  @Override
  public void set(int i, Geometry geom) {
    if (geom == null) {
      vector.setNull(i);
    } else {
      if (writer == null) {
        writer = new WKBWriter();
      }
      vector.setSafe(i, writer.write(geom));
    }
  }

  @Override
  public Geometry get(int i) {
    if (vector.isNull(i)) {
      return null;
    } else {
      Geometry geometry = null;
      try {
        if (reader == null) {
          reader = new WKBReader();
        }
        geometry = reader.read(vector.get(i));
      } catch (ParseException exception) {
        throw new RuntimeException(exception);
      }
      return geometry;
    }
  }

  @Override
  public VarBinaryVector getVector() {
    return vector;
  }

  @Override
  public void setValueCount(int count) {
    vector.setValueCount(count);
  }

  @Override
  public int getValueCount() {
    return vector.getValueCount();
  }

  @Override
  public int getNullCount() {
    int count = vector.getNullCount();
    return Math.max(count, 0);
  }

  @Override
  public void transfer(int fromIndex, int toIndex, GeometryVector<Geometry, VarBinaryVector> to) {
    to.set(toIndex, get(fromIndex));
  }

  @Override
  public void close() throws Exception {
    vector.close();
  }
}
