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
import org.locationtech.geomesa.arrow.vector.impl.AbstractLineStringVector;

import java.util.List;

public class LineStringFloatVector extends AbstractLineStringVector {

  // fields created by this vector
  public static final List<Field> fields = GeometryFields.XY_FLOAT_LIST;

  public LineStringFloatVector(String name, BufferAllocator allocator) {
    super(name, allocator);
  }

  public LineStringFloatVector(String name, AbstractContainerVector container) {
    super(name, container);
  }

  public LineStringFloatVector(ListVector vector) {
    super(vector);
  }

  @Override
  protected List<Field> getFields() {
    return fields;
  }

  @Override
  protected LineStringWriter createWriter(ListVector vector) {
    return new LineStringFloatWriter(vector);
  }

  @Override
  protected LineStringReader createReader(ListVector vector) {
    return new LineStringFloatReader(vector);
  }

  public static class LineStringFloatWriter extends LineStringWriter {

    public LineStringFloatWriter(ListVector vector) {
      super(vector);
    }

    @Override
    protected void writeOrdinal(FieldVector.Mutator mutator, int index, double ordinal) {
      ((NullableFloat4Vector.Mutator) mutator).set(index, (float) ordinal);
    }
  }

  public static class LineStringFloatReader extends LineStringReader {

    public LineStringFloatReader(ListVector vector) {
      super(vector);
    }

    @Override
    protected double readOrdinal(FieldReader reader) {
      return reader.readFloat();
    }
  }
}
