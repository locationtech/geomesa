/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.serialization

import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.Hints

// noinspection LanguageFeature
trait GenericMapSerialization[T <: PrimitiveWriter, V <: PrimitiveReader] extends LazyLogging {

  def serialize(out: T, map: java.util.Map[AnyRef, AnyRef]): Unit

  def deserialize(in: V): java.util.Map[AnyRef, AnyRef]

  protected def write(out: T, value: AnyRef) = value match {
    case v: String            => out.writeString(v)
    case v: java.lang.Integer => out.writeInt(v)
    case v: java.lang.Long    => out.writeLong(v)
    case v: java.lang.Float   => out.writeFloat(v)
    case v: java.lang.Double  => out.writeDouble(v)
    case v: java.lang.Boolean => out.writeBoolean(v)
    case v: java.util.Date    => out.writeLong(v.getTime)
    case v: Hints.Key         => out.writeString(HintKeySerialization.keyToId(v))
    case _ => throw new IllegalArgumentException(s"Unsupported value: $value (${value.getClass})")
  }

  protected def read(in: V, clas: Class[_]): AnyRef = clas match {
    case c if classOf[java.lang.String].isAssignableFrom(c)  => in.readString()
    case c if classOf[java.lang.Integer].isAssignableFrom(c) => Int.box(in.readInt())
    case c if classOf[java.lang.Long].isAssignableFrom(c)    => Long.box(in.readLong())
    case c if classOf[java.lang.Float].isAssignableFrom(c)   => Float.box(in.readFloat())
    case c if classOf[java.lang.Double].isAssignableFrom(c)  => Double.box(in.readDouble())
    case c if classOf[java.lang.Boolean].isAssignableFrom(c) => Boolean.box(in.readBoolean())
    case c if classOf[java.util.Date].isAssignableFrom(c)    => new java.util.Date(in.readLong())
    case c if classOf[Hints.Key].isAssignableFrom(c)         => HintKeySerialization.idToKey(in.readString())
    case _ => throw new IllegalArgumentException(s"Unsupported value class: $clas")
  }

  protected def canSerialize(obj: AnyRef): Boolean = obj match {
    case key: Hints.Key => HintKeySerialization.canSerialize(key)
    case _    => true
  }
}
