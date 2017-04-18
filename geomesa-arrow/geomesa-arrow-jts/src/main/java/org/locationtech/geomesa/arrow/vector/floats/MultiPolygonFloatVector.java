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
import org.locationtech.geomesa.arrow.vector.impl.AbstractMultiPolygonVector;

import java.util.List;

public class MultiPolygonFloatVector extends AbstractMultiPolygonVector {

  // fields created by this vector
  public static final List<Field> fields = GeometryFields.XY_FLOAT_LIST_3;

  public MultiPolygonFloatVector(String name, BufferAllocator allocator) {
    super(name, allocator);
  }

  public MultiPolygonFloatVector(String name, AbstractContainerVector container) {
    super(name, container);
  }

  public MultiPolygonFloatVector(ListVector vector) {
    super(vector);
  }

  @Override
  protected List<Field> getFields() {
    return fields;
  }

  @Override
  protected MultiPolygonWriter createWriter(ListVector vector) {
    return new MultiPolygonFloatWriter(vector);
  }

  @Override
  protected MultiPolygonReader createReader(ListVector vector) {
    return new MultiPolygonFloatReader(vector);
  }

  public static class MultiPolygonFloatWriter extends MultiPolygonWriter {

    public MultiPolygonFloatWriter(ListVector vector) {
      super(vector);
    }

    @Override
    protected void writeOrdinal(FieldVector.Mutator mutator, int index, double ordinal) {
      ((NullableFloat4Vector.Mutator) mutator).set(index, (float) ordinal);
    }
  }

  public static class MultiPolygonFloatReader extends MultiPolygonReader {

    public MultiPolygonFloatReader(ListVector vector) {
      super(vector);
    }

    @Override
    protected double readOrdinal(FieldReader reader) {
      return reader.readFloat();
    }
  }
}
