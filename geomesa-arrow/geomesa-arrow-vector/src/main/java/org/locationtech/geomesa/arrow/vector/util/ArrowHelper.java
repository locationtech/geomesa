/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector.util;

import com.vividsolutions.jts.geom.Coordinate;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ArrowHelper {

  private ArrowHelper() {}

  private static ThreadLocal<ByteBuffer> buffers = new ThreadLocal<ByteBuffer>() {
    public ByteBuffer initialValue() {
      return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
    }
  };

  public static ArrowType LONG_TYPE = new ArrowType.Int(64, true);

  public static List<Field> LONG_FIELD = Collections.singletonList(
      new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME, true, LONG_TYPE, null));

  public static ArrowType FLOAT_TYPE = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);

  public static ArrowType DOUBLE_TYPE = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

  public static List<Field> XY_FIELD = Collections.singletonList(
      new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME, true, ArrowHelper.FLOAT_TYPE, null));

  public static List<Field> XY_LIST_FIELD = Collections.singletonList(
      new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME, true, new ArrowType.FixedSizeList(2), XY_FIELD));

  public static List<Field> DOUBLE_FIELD = Collections.singletonList(
      new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME, true, DOUBLE_TYPE, null));

  public static List<Field> DOUBLE_DOUBLE_FIELD = Collections.singletonList(
      new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME, true, ArrowType.List.INSTANCE, DOUBLE_FIELD));

  public static List<Field> TRIPLE_DOUBLE_FIELD = Collections.singletonList(
      new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME, true, ArrowType.List.INSTANCE, DOUBLE_DOUBLE_FIELD));

  public static long encode64(Coordinate c) {
    ByteBuffer buf = buffers.get();
    buf.clear();
    buf.putFloat((float) c.y);
    buf.putFloat((float) c.x);
    buf.flip();
    return buf.getLong();
  }

  public static Coordinate decode64(long encoded) {
    ByteBuffer buf = buffers.get();
    buf.clear();
    buf.putLong(encoded);
    buf.flip();
    float y = buf.getFloat();
    float x = buf.getFloat();
    return new Coordinate(x, y);
  }
}
