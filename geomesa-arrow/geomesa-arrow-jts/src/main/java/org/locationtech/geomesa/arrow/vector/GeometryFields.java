/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.vector;

import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.Collections;
import java.util.List;

/**
 * Defines the fields (schema) used by geometry vectors
 */
public class GeometryFields {

  private GeometryFields() {}

  /**
   * Determines the geometry precision of a vector based on its field
   *
   * @param field field
   * @return precision, or null if not an expected geometry field
   */
  public static FloatingPointPrecision precisionFromField(Field field) {
    Field toCheck = field;
    while (true) {
      ArrowType type = toCheck.getType();
      if (type instanceof ArrowType.FloatingPoint) {
        return ((ArrowType.FloatingPoint) type).getPrecision();
      } else if (type instanceof ArrowType.FixedSizeList || type instanceof ArrowType.List) {
        toCheck = field.getChildren().get(0);
      } else {
        return null;
      }
    }
  }

  public static ArrowType FLOAT_TYPE = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);

  public static ArrowType DOUBLE_TYPE = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

  /**
   * Single float vector, appropriate for storing Points
   */
  public static List<Field> XY_FLOAT = Collections.singletonList(
      new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME, FieldType.nullable(FLOAT_TYPE), null));

  /**
   * Single double vector, appropriate for storing Points
   */
  public static List<Field> XY_DOUBLE = Collections.singletonList(
      new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME, FieldType.nullable(DOUBLE_TYPE), null));

  /**
   * Nested list of floats, appropriate for storing MultiPoints or LineStrings
   */
  public static List<Field> XY_FLOAT_LIST = Collections.singletonList(
      new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME, FieldType.nullable(new ArrowType.FixedSizeList(2)), XY_FLOAT));

  /**
   * Nested list of doubles, appropriate for storing MultiPoints or LineStrings
   */
  public static List<Field> XY_DOUBLE_LIST = Collections.singletonList(
      new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME, FieldType.nullable(new ArrowType.FixedSizeList(2)), XY_DOUBLE));

  /**
   * Doubly-nested list of floats, appropriate for storing MultiLineStrings or Polygons
   */
  public static List<Field> XY_FLOAT_LIST_2 = Collections.singletonList(
      new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME, FieldType.nullable(ArrowType.List.INSTANCE), XY_FLOAT_LIST));

  /**
   * Doubly-nested list of doubles, appropriate for storing MultiLineStrings or Polygons
   */
  public static List<Field> XY_DOUBLE_LIST_2 = Collections.singletonList(
      new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME, FieldType.nullable(ArrowType.List.INSTANCE), XY_DOUBLE_LIST));

  /**
   * Triply-nested list of floats, appropriate for storing MultiPolygons
   */
  public static List<Field> XY_FLOAT_LIST_3 = Collections.singletonList(
      new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME, FieldType.nullable(ArrowType.List.INSTANCE), XY_FLOAT_LIST_2));

  /**
   * Triply-nested list of doubles, appropriate for storing MultiPolygons
   */
  public static List<Field> XY_DOUBLE_LIST_3 = Collections.singletonList(
      new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME, FieldType.nullable(ArrowType.List.INSTANCE), XY_DOUBLE_LIST_2));
}
