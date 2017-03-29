/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector.util;

import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ArrowHelper {

  private ArrowHelper() {}

  public static ArrowType DOUBLE_TYPE = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

  public static List<Field> DOUBLE_FIELD = Collections.singletonList(
      new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME, true, DOUBLE_TYPE, null));

  public static List<Field> DOUBLE_DOUBLE_FIELD = Collections.singletonList(
      new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME, true, ArrowType.List.INSTANCE, DOUBLE_FIELD));

  public static List<Field> TRIPLE_DOUBLE_FIELD = Collections.singletonList(
      new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME, true, ArrowType.List.INSTANCE, DOUBLE_DOUBLE_FIELD));
}
