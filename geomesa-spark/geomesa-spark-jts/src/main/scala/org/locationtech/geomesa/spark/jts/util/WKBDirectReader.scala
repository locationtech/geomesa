/***********************************************************************
 * Copyright (c) 2018 Astraea, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.util

import java.nio.{ByteBuffer, ByteOrder}

import com.vividsolutions.jts.io.WKBConstants
import org.locationtech.geomesa.spark.jts.util.WKBUtils.WKBData

/**
 *
 *
 *
 * @since 4/15/18
 */
object WKBDirectReader {
  implicit class ByteBufferBuddies(val self: ByteBuffer) extends AnyVal {
    @inline
    def getUInt(index: Int) = self.getInt(index) & 0xFF
  }

  def getNumPoints(wkb: WKBData): Int = {
    getNumPoints(ByteBuffer.wrap(wkb))
  }
  private def getNumPoints(buff: ByteBuffer): Int = {
    buff.get(0) match {
      case WKBConstants.wkbXDR =>
        buff.order(ByteOrder.BIG_ENDIAN)
      case WKBConstants.wkbNDR =>
        buff.order(ByteOrder.LITTLE_ENDIAN)
    }

    buff.getUInt(1) match {
      case WKBConstants.wkbPoint =>
        1
      case WKBConstants.wkbLineString =>
        buff.getUInt(2)
      case WKBConstants.wkbPolygon =>
        val numRings = buff.getUInt(2)
        ???
      case WKBConstants.wkbMultiPoint =>
        buff.getUInt(2)
      case WKBConstants.wkbMultiLineString =>
        val numLines = buff.getUInt(2)
        ???
      case WKBConstants.wkbMultiPolygon =>
        val numPolys = buff.getUInt(2)
        ???
      case WKBConstants.wkbGeometryCollection =>
        ???
    }
  }
}

/*
 * WKB Schema
 * ----------
 *
 * Basic Type definitions
 * byte : 1 byte
 * uint32 : 32 bit unsigned integer  (4 bytes)
 * double : double precision number (8 bytes)
 *
 * Building Blocks : Point, LinearRing
 *
 *   Point {
 *     double x;
 *     double y;
 *   };
 *   LinearRing   {
 *     uint32  numPoints;
 *     Point   points[numPoints];
 *   };
 *   enum wkbGeometryType {
 *     wkbPoint = 1,
 *     wkbLineString = 2,
 *     wkbPolygon = 3,
 *     wkbMultiPoint = 4,
 *     wkbMultiLineString = 5,
 *     wkbMultiPolygon = 6
 *   };
 *   enum wkbByteOrder {
 *     wkbXDR = 0,     // Big Endian
 *     wkbNDR = 1     // Little Endian
 *   };
 *   WKBPoint {
 *     byte     byteOrder;
 *     uint32   wkbType;     // 1=wkbPoint
 *     Point    point;
 *   };
 *   WKBLineString {
 *     byte     byteOrder;
 *     uint32   wkbType;     // 2=wkbLineString
 *     uint32   numPoints;
 *     Point    points[numPoints];
 *   };
 *
 *   WKBPolygon    {
 *     byte                byteOrder;
 *     uint32            wkbType;     // 3=wkbPolygon
 *     uint32            numRings;
 *     LinearRing        rings[numRings];
 *   };
 *   WKBMultiPoint    {
 *     byte                byteOrder;
 *     uint32            wkbType;     // 4=wkbMultipoint
 *     uint32            num_wkbPoints;
 *     WKBPoint            WKBPoints[num_wkbPoints];
 *   };
 *   WKBMultiLineString    {
 *     byte              byteOrder;
 *     uint32            wkbType;     // 5=wkbMultiLineString
 *     uint32            num_wkbLineStrings;
 *     WKBLineString     WKBLineStrings[num_wkbLineStrings];
 *   };
 *
 *   wkbMultiPolygon {
 *     byte              byteOrder;
 *     uint32            wkbType;     // 6=wkbMultiPolygon
 *     uint32            num_wkbPolygons;
 *     WKBPolygon        wkbPolygons[num_wkbPolygons];
 *   };
 *
 *   WKBGeometry  {
 *     union {
 *       WKBPoint                 point;
 *       WKBLineString            linestring;
 *       WKBPolygon               polygon;
 *       WKBMultiPoint            mpoint;
 *       WKBMultiLineString       mlinestring;
 *       WKBMultiPolygon          mpolygon;
 *     }
 *   };
 */