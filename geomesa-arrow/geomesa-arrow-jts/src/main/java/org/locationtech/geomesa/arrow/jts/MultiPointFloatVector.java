/***********************************************************************
 * Copyright (c) 2013-2025 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.jts;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.locationtech.geomesa.arrow.jts.impl.AbstractMultiPointVector;

import java.util.List;
import java.util.Map;

public class MultiPointFloatVector extends AbstractMultiPointVector<Float4Vector>  {

  // fields created by this vector
  public static final List<Field> fields = GeometryFields.XY_FLOAT_LIST;

  /**
   * Constructor
   *
   * @param name name of the vector
   * @param allocator allocator for the vector
   * @param metadata metadata (may be null)
   */
  public MultiPointFloatVector(String name, BufferAllocator allocator,Map<String, String> metadata) {
    super(name, allocator, metadata);
  }

  /**
   * Constructor
   *
   * @param name name of the vector
   * @param container parent container
   * @param metadata metadata (may be null)
   */
  public MultiPointFloatVector(String name, AbstractContainerVector container, Map<String, String> metadata) {
    super(name, container, metadata);
  }

  public MultiPointFloatVector(ListVector vector) {
    super(vector);
  }

  @Override
  protected List<Field> getFields() {
    return fields;
  }

  @Override
  protected void writeOrdinal(int index, double ordinal) {
    getOrdinalVector().setSafe(index, (float) ordinal);
  }

  @Override
  protected double readOrdinal(int index) {
    return getOrdinalVector().get(index);
  }
}
