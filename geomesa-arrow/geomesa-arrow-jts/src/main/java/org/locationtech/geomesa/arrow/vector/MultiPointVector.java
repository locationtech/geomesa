/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.arrow.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableFloat8Vector;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.vector.impl.AbstractMultiPointVector;

import java.util.List;

/**
 * Double-precision vector for multi-points
 */
public class MultiPointVector extends AbstractMultiPointVector {

  // fields created by this vector
  public static final List<Field> fields = GeometryFields.XY_DOUBLE_LIST;

  public MultiPointVector(String name, BufferAllocator allocator) {
    super(name, allocator);
  }

  public MultiPointVector(String name, AbstractContainerVector container) {
    super(name, container);
  }

  public MultiPointVector(ListVector vector) {
    super(vector);
  }

  @Override
  protected List<Field> getFields() {
    return fields;
  }

  @Override
  protected MultiPointWriter createWriter(ListVector vector) {
    return new MultiPointDoubleWriter(vector);
  }

  @Override
  protected MultiPointReader createReader(ListVector vector) {
    return new MultiPointDoubleReader(vector);
  }

  public static class MultiPointDoubleWriter extends MultiPointWriter {

    public MultiPointDoubleWriter(ListVector vector) {
      super(vector);
    }

    @Override
    protected void writeOrdinal(FieldVector.Mutator mutator, int index, double ordinal) {
      ((NullableFloat8Vector.Mutator) mutator).set(index, ordinal);
    }
  }

  public static class MultiPointDoubleReader extends MultiPointReader {

    public MultiPointDoubleReader(ListVector vector) {
      super(vector);
    }

    @Override
    protected double readOrdinal(FieldReader reader) {
      return reader.readDouble();
    }
  }
}
