/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.conversions

import java.util.Optional

// noinspection LanguageFeature
object JavaConverters {

  implicit class OptionToJava[T](val option: Option[T]) extends AnyRef {
    def asJava[V >: T]: Optional[V] = option match {
      case Some(v) => Optional.of(v)
      case None    => Optional.empty()
    }
  }

  implicit class OptionalToScala[T](val optional: Optional[T]) extends AnyRef {
    def asScala[V >: T]: Option[V] = if (optional.isPresent) { Some(optional.get()) } else { None }
  }

  implicit def ScalaFunction1ToJava[T, U, V <: T, X >: U](fn: Function[T, U]): java.util.function.Function[V, X] =
    new java.util.function.Function[V, X] { override def apply(t: V): X = fn.apply(t) }
}
