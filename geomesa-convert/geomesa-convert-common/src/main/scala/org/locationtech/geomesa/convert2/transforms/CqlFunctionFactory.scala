/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert2.transforms

import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.filter.expression.{PropertyAccessor, PropertyAccessorFactory}
import org.locationtech.geomesa.utils.geotools.converters.FastConverter


class CqlFunctionFactory extends TransformerFunctionFactory with LazyLogging {

  import scala.collection.JavaConverters._

  override def functions: Seq[TransformerFunction] = cqlFunctions

  private val cqlFunctions = {
    val ff = CommonFactoryFinder.getFilterFactory2
    val builder = Seq.newBuilder[TransformerFunction]

    CommonFactoryFinder.getFunctionFactories(null).asScala.toSeq.foreach { factory =>
      val names = try {
        // only import default cql functions (without namespaces)
        // exclude 'categorize', as it doesn't parse into a function correctly
        factory.getFunctionNames.asScala.filter(f => f.getFunctionName.getNamespaceURI == null && f.getName != "Categorize")
      } catch {
        // if CQL classes aren't on the classpath, these functions won't be available
        case e: NoClassDefFoundError =>
          logger.warn(s"Couldn't create cql function factory '${factory.getClass.getName}': ${e.toString}")
          Seq.empty
      }
      names.foreach { f =>
        val name = f.getFunctionName.toString
        // use alphas for the array indices, as used by the ArrayPropertyAccessor, below
        val expressions = Array.tabulate(f.getArguments.size()) { i =>
          ff.property(('a' + i).toChar.toString)
        }
        try {
          val fn = ff.function(name, expressions: _*)
          builder += TransformerFunction(s"cql:$name") { args => fn.evaluate(args) }
        } catch {
          case e: Exception => logger.warn(s"Couldn't create cql function '$name': ${e.toString}")
        }
      }
    }
    builder.result()
  }
}

object CqlFunctionFactory {

  /**
    * For accessing 'properties' of arrays (instead of simple features)
    */
  class ArrayPropertyAccessorFactory extends PropertyAccessorFactory {
    override def createPropertyAccessor(typ: Class[_],
                                        xpath: String,
                                        target: Class[_],
                                        hints: Hints): PropertyAccessor = {
      if (typ.isArray) { new ArrayPropertyAccessor } else { null }
    }
  }

  /**
    * Accessing properties of an array. Indices are expected to be lower-case letters,
    * where 'a' indicates the first element, b the second, etc. We use alphas because
    * integers are interpreted as literals, not properties.
    */
  class ArrayPropertyAccessor extends PropertyAccessor {

    override def canHandle(obj: Any, xpath: String, target: Class[_]): Boolean = {
      val i = toIndex(xpath)
      i >= 0 && i < obj.asInstanceOf[Array[Any]].length
    }

    override def set[T](obj: Any, xpath: String, value: T, target: Class[T]): Unit =
      obj.asInstanceOf[Array[Any]].update(toIndex(xpath), FastConverter.convert(value, target))

    override def get[T](obj: Any, xpath: String, target: Class[T]): T =
      FastConverter.convert(obj.asInstanceOf[Array[Any]].apply(toIndex(xpath)), target)

    private def toIndex(xpath: String): Int = {
      if (xpath.length == 1) {
        xpath.charAt(0) - 'a'
      } else {
        -1
      }
    }
  }
}
