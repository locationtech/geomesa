/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.jts;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.util.Collections;
import java.util.List;

/**
 * Defines the fields (schema) used by geometry vectors
 */
public class GeometryFields {

  private GeometryFields() {}

  public static GeometryVector<?, ?> wrap(FieldVector vector, Class<?> binding) {
    List<Field> fields = vector.getField().getChildren();
    if (fields.isEmpty()) {
      return new WKBGeometryVector((VarBinaryVector) vector);
    } else if (Point.class.equals(binding)) {
      if (fields.equals(PointFloatVector.fields)) {
        return new PointFloatVector((FixedSizeListVector) vector);
      } else if (fields.equals(PointVector.fields)) {
        return new PointVector((FixedSizeListVector) vector);
      }
    } else if (LineString.class.equals(binding)) {
      if (fields.equals(LineStringFloatVector.fields)) {
        return new LineStringFloatVector((ListVector) vector);
      } else if (fields.equals(LineStringVector.fields)) {
        return new LineStringVector((ListVector) vector);
      }
    } else if (Polygon.class.equals(binding)) {
      if (fields.equals(PolygonFloatVector.fields)) {
        return new PolygonFloatVector((ListVector) vector);
      } else if (fields.equals(PolygonVector.fields)) {
        return new PolygonVector((ListVector) vector);
      }
    } else if (MultiPolygon.class.equals(binding)) {
      if (fields.equals(MultiPolygonFloatVector.fields)) {
        return new MultiPolygonFloatVector((ListVector) vector);
      } else if (fields.equals(MultiPolygonVector.fields)) {
        return new MultiPolygonVector((ListVector) vector);
      }
    } else if (MultiLineString.class.equals(binding)) {
      if (fields.equals(MultiLineStringFloatVector.fields)) {
        return new MultiLineStringFloatVector((ListVector) vector);
      } else if (fields.equals(MultiLineStringVector.fields)) {
        return new MultiLineStringVector((ListVector) vector);
      }
    } else if (MultiPoint.class.equals(binding)) {
      if (fields.equals(MultiPointFloatVector.fields)) {
        return new MultiPointFloatVector((ListVector) vector);
      } else if (fields.equals(MultiPointVector.fields)) {
        return new MultiPointVector((ListVector) vector);
      }
    }

    throw new IllegalArgumentException("Vector " + vector + " does not match any geometry type");
  }

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
        toCheck = toCheck.getChildren().get(0);
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
