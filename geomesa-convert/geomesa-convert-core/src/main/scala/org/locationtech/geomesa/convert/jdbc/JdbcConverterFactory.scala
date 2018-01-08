/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.jdbc

import java.sql.{Connection, DriverManager}

import com.typesafe.config.Config
import org.locationtech.geomesa.convert.Transformers.Expr
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.immutable.IndexedSeq

class JdbcConverterFactory extends AbstractSimpleFeatureConverterFactory[String] {

  import JdbcConverterFactory._

  override protected val typeToProcess = "jdbc"

  override protected def buildConverter(sft: SimpleFeatureType,
                                        conf: Config,
                                        idBuilder: Expr,
                                        fields: IndexedSeq[Field],
                                        userDataBuilder: Map[String, Expr],
                                        cacheServices: Map[String, EnrichmentCache],
                                        parseOpts: ConvertParseOpts): JdbcConverter = {
    val connection = DriverManager.getConnection(conf.getString("connection"))
    new JdbcConverter(connection, sft, idBuilder, fields, userDataBuilder, cacheServices, parseOpts)
  }
}

object JdbcConverterFactory {

  class JdbcConverter(connection: Connection,
                      val targetSFT: SimpleFeatureType,
                      val idBuilder: Expr,
                      val inputFields: IndexedSeq[Field],
                      val userDataBuilder: Map[String, Expr],
                      val caches: Map[String, EnrichmentCache],
                      val parseOpts: ConvertParseOpts)
      extends LinesToSimpleFeatureConverter {

    override def fromInputType(string: String, ec: EvaluationContext): Iterator[Array[Any]] = {
      if (string == null || string.isEmpty) {
        throw new IllegalArgumentException("Invalid input (empty)")
      }

      // track line counts separately from input strings, which are usually a single select statement
      ec.counter.setLineCount(ec.counter.getLineCount - 1)

      new Iterator[Array[Any]] {
        private val statement = connection.prepareCall(if (string.endsWith(";")) { string } else { s"$string;" })
        private val results = statement.executeQuery()
        private val array = Array.ofDim[Any](1 + results.getMetaData.getColumnCount)
        private var _next = results.next()

        override def hasNext: Boolean = _next

        override def next(): Array[Any] = {
          ec.counter.incLineCount()

          array(0) = "" // leave 0 empty for the entire row
          var i = 1
          while (i < array.length) {
            array(i) = results.getObject(i) // results are 1-indexed
            i += 1
          }
          array(0) = array.mkString // set the whole row value for reference

          _next = results.next()

          if (!_next) {
            CloseWithLogging(results)
            CloseWithLogging(statement)
          }

          array
        }
      }
    }

    override def close(): Unit = {
      CloseWithLogging(connection)
      super.close()
    }
  }
}
