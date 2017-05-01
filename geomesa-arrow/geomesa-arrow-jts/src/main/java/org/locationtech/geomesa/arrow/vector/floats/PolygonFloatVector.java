/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.vector.floats;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableFloat4Vector;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.vector.GeometryFields;
import org.locationtech.geomesa.arrow.vector.impl.AbstractPolygonVector;

import java.util.List;

public class PolygonFloatVector extends AbstractPolygonVector {

  // fields created by this vector
  public static final List<Field> fields = GeometryFields.XY_FLOAT_LIST_2;

  public PolygonFloatVector(String name, BufferAllocator allocator) {
    super(name, allocator);
  }

  public PolygonFloatVector(String name, AbstractContainerVector container) {
    super(name, container);
  }

  public PolygonFloatVector(ListVector vector) {
    super(vector);
  }

  @Override
  protected List<Field> getFields() {
    return fields;
  }

  @Override
  protected PolygonWriter createWriter(ListVector vector) {
    return new PolygonFloatWriter(vector);
  }

  @Override
  protected PolygonReader createReader(ListVector vector) {
    return new PolygonFloatReader(vector);
  }

  public static class PolygonFloatWriter extends PolygonWriter {

    public PolygonFloatWriter(ListVector vector) {
      super(vector);
    }

    @Override
    protected void writeOrdinal(FieldVector.Mutator mutator, int index, double ordinal) {
      ((NullableFloat4Vector.Mutator) mutator).set(index, (float) ordinal);
    }
  }

  public static class PolygonFloatReader extends PolygonReader {

    public PolygonFloatReader(ListVector vector) {
      super(vector);
    }

    @Override
    protected double readOrdinal(FieldReader reader) {
      return reader.readFloat();
    }
  }
}
