/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.jdbc

import java.io.InputStream
import java.sql.{DriverManager, PreparedStatement, ResultSet}

import com.typesafe.config.Config
import org.apache.commons.io.IOUtils
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.convert.jdbc.JdbcConverter.JdbcConfig
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.{AbstractConverter, ConverterConfig}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.SimpleFeatureType

import scala.annotation.tailrec

class JdbcConverter(targetSft: SimpleFeatureType,
                    config: JdbcConfig,
                    fields: Seq[BasicField],
                    options: BasicOptions)
    extends AbstractConverter(targetSft, config, fields, options) {

  private val connection = DriverManager.getConnection(config.connection)

  override def close(): Unit = {
    CloseWithLogging(connection)
    super.close()
  }

  override protected def read(is: InputStream, ec: EvaluationContext): CloseableIterator[Array[Any]] =
    new StatementIterator(is, ec.counter)

  class StatementIterator private [JdbcConverter] (is: InputStream, counter: Counter)
      extends CloseableIterator[Array[Any]] {

    private val statements = IOUtils.lineIterator(is, options.encoding) // TODO split on ; ?

    private var statement: PreparedStatement = _
    private var results: ResultSet = _
    private var _next = false
    private var array: Array[Any] = _

    @tailrec
    override final def hasNext: Boolean = _next || {
      Option(results).foreach(CloseWithLogging.apply)
      Option(statement).foreach(CloseWithLogging.apply)
      if (!statements.hasNext) {
        false
      } else {
        val sql = statements.next.trim()
        statement = connection.prepareCall(if (sql.endsWith(";")) { sql } else { s"$sql;" })
        results = statement.executeQuery()
        array = Array.ofDim[Any](1 + results.getMetaData.getColumnCount)
        _next = results.next()
        hasNext
      }
    }

    override def next(): Array[Any] = {
      counter.incLineCount()

      array(0) = "" // leave 0 empty for the entire row
      var i = 1
      while (i < array.length) {
        array(i) = results.getObject(i) // results are 1-indexed
        i += 1
      }
      array(0) = array.mkString // set the whole row value for reference

      _next = results.next()

      array
    }

    override def close(): Unit = {
      Option(results).foreach(CloseWithLogging.apply)
      Option(statement).foreach(CloseWithLogging.apply)
      CloseWithLogging(is)
    }
  }
}

object JdbcConverter {

  case class JdbcConfig(`type`: String,
                        connection: String,
                        idField: Option[Expression],
                        caches: Map[String, Config],
                        userData: Map[String, Expression]) extends ConverterConfig
}
