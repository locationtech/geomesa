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
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.vector.GeometryFields;
import org.locationtech.geomesa.arrow.vector.impl.AbstractPointVector;

import java.util.List;

public class PointFloatVector extends AbstractPointVector {

  // fields created by this vector
  public static final List<Field> fields = GeometryFields.XY_FLOAT;

  public PointFloatVector(String name, BufferAllocator allocator) {
    super(name, allocator);
  }

  public PointFloatVector(String name, AbstractContainerVector container) {
    super(name, container);
  }

  public PointFloatVector(FixedSizeListVector vector) {
    super(vector);
  }

  @Override
  protected List<Field> getFields() {
    return fields;
  }

  @Override
  protected PointWriter createWriter(FixedSizeListVector vector) {
    return new PointFloatWriter(vector);
  }

  @Override
  protected PointReader createReader(FixedSizeListVector vector) {
    return new PointFloatReader(vector);
  }

  public static class PointFloatWriter extends PointWriter {

    public PointFloatWriter(FixedSizeListVector vector) {
      super(vector);
    }

    @Override
    protected void writeOrdinal(FieldVector.Mutator mutator, int index, double ordinal) {
      ((NullableFloat4Vector.Mutator) mutator).set(index, (float) ordinal);
    }
  }

  public static class PointFloatReader extends PointReader {

    public PointFloatReader(FixedSizeListVector vector) {
      super(vector);
    }

    @Override
    protected double readOrdinal(FieldReader reader) {
      return reader.readFloat();
    }
  }
}
