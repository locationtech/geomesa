/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features

/**
 * Options to be applied when encoding.  The same options must be specified when decoding.
 */
object SerializationOption extends Enumeration {

  type SerializationOption = Value

  val WithUserData      :Value = Value
  val WithoutFidHints   :Value = Value
  val WithoutId         :Value = Value
  val Immutable         :Value = Value
  val Lazy              :Value = Value
  val NativeCollections :Value = Value

  implicit class SerializationOptions(val options: Set[SerializationOption]) extends AnyVal {

    /**
     * @param value the value to search for
     * @return true iff ``this`` contains the given ``value``
     */
    def contains(value: SerializationOption): Boolean = options.contains(value)

    def withUserData: Boolean = options.contains(WithUserData)

    /**
     * In conjunction with `withUserData`, skip Hints.USE_PROVIDED_FID and Hints.PROVIDED_FID
     *
     * Note that currently we don't serialize those fields anyway, but this makes it explicit and will
     * suppress any warnings
     *
     * @return
     */
    def withoutFidHints: Boolean = options.contains(WithoutFidHints)

    def withoutId: Boolean = options.contains(WithoutId)

    def immutable: Boolean = options.contains(Immutable)

    def isLazy: Boolean = options.contains(Lazy)

    def useNativeCollections: Boolean = options.contains(NativeCollections)
  }

  object SerializationOptions {

    val none: Set[SerializationOption] = Set.empty[SerializationOption]

    val withUserData: Set[SerializationOption] = Set(WithUserData)

    val withoutId: Set[SerializationOption] = Set(WithoutId)

    val immutable: Set[SerializationOption] = Set(Immutable)

    val nativeCollections: Set[SerializationOption] = Set(NativeCollections)

    def builder: Builder = new Builder()

    class Builder {

      private val options = scala.collection.mutable.Set.empty[SerializationOption]

      def immutable: Builder = { options.add(Immutable); this }

      def withUserData: Builder = { options.add(WithUserData); this }

      /**
       * In conjunction with `withUserData`, skip Hints.USE_PROVIDED_FID and Hints.PROVIDED_FID
       *
       * Note that currently we don't serialize those fields anyway, but this makes it explicit and will
       * suppress any warnings
       *
       * @return
       */
      def withoutFidHints: Builder = { options.add(WithoutFidHints); this }

      def withoutId: Builder = { options.add(WithoutId); this }

      def `lazy`: Builder = { options.add(Lazy); this }

      def withNativeCollections: Builder = { options.add(NativeCollections); this }

      def build: Set[SerializationOption] = options.toSet
    }
  }
}
