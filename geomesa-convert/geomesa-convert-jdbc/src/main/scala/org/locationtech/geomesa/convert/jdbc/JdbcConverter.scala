/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.jdbc

import java.io.InputStream
import java.nio.charset.Charset
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.typesafe.config.Config
import org.apache.commons.io.IOUtils
import org.locationtech.geomesa.convert._
import org.locationtech.geomesa.convert.jdbc.JdbcConverter.{JdbcConfig, ResultSetIterator, StatementIterator}
import org.locationtech.geomesa.convert2.AbstractConverter.{BasicField, BasicOptions}
import org.locationtech.geomesa.convert2.transforms.Expression
import org.locationtech.geomesa.convert2.{AbstractConverter, ConverterConfig}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.SimpleFeatureType

import scala.annotation.tailrec

class JdbcConverter(sft: SimpleFeatureType, config: JdbcConfig, fields: Seq[BasicField], options: BasicOptions)
    extends AbstractConverter[ResultSet, JdbcConfig, BasicField, BasicOptions](sft, config, fields, options) {

  private val connection = DriverManager.getConnection(config.connection)

  override protected def parse(is: InputStream, ec: EvaluationContext): CloseableIterator[ResultSet] =
    new StatementIterator(connection, is, options.encoding)

  override protected def values(parsed: CloseableIterator[ResultSet],
                                ec: EvaluationContext): CloseableIterator[Array[Any]] = {
    new ResultSetIterator(parsed, ec.counter)
  }

  override def close(): Unit = {
    CloseWithLogging(connection)
    super.close()
  }
}

object JdbcConverter {

  case class JdbcConfig(`type`: String,
                        connection: String,
                        idField: Option[Expression],
                        caches: Map[String, Config],
                        userData: Map[String, Expression]) extends ConverterConfig

  /**
    * Converts the input to statements and executes them.
    *
    * Note: the ResultSets are not closed, this should be done by the caller
    *
    * @param connection connection
    * @param is input
    * @param encoding input encoding
    */
  class StatementIterator private [JdbcConverter] (connection: Connection, is: InputStream, encoding: Charset)
      extends CloseableIterator[ResultSet] {

    private val statements = IOUtils.lineIterator(is, encoding) // TODO split on ; ?

    private var statement: PreparedStatement = _
    private var results: ResultSet = _

    override final def hasNext: Boolean = results != null || {
      Option(statement).foreach(CloseWithLogging.apply)
      if (!statements.hasNext) {
        statement = null
        results = null
        false
      } else {
        val sql = statements.next.trim()
        statement = connection.prepareCall(if (sql.endsWith(";")) { sql } else { s"$sql;" })
        results = statement.executeQuery()
        true
      }
    }

    override def next(): ResultSet = {
      if (!hasNext) { Iterator.empty.next() } else {
        val res = results
        results = null
        res
      }
    }

    override def close(): Unit = {
      Option(statement).foreach(CloseWithLogging.apply)
      CloseWithLogging(statements)
    }
  }

  /**
    * Converts result sets into values
    *
    * @param iter result sets
    * @param counter counter
    */
  class ResultSetIterator private [JdbcConverter] (iter: CloseableIterator[ResultSet], counter: Counter)
      extends CloseableIterator[Array[Any]] {

    private var results: ResultSet = _
    private var array: Array[Any] = _
    private var hasNextResult = false

    @tailrec
    override final def hasNext: Boolean = hasNextResult || {
      Option(results).foreach(CloseWithLogging.apply)
      if (!iter.hasNext) {
        results = null
        false
      } else {
        results = iter.next
        array = Array.ofDim[Any](results.getMetaData.getColumnCount + 1)
        hasNextResult = results.next()
        hasNext
      }
    }

    override def next(): Array[Any] = {
      if (!hasNext) { Iterator.empty.next() } else {
        counter.incLineCount()
        // the first column will hold the entire row, but set it empty here to
        // avoid the previous row being captured in mkString, below
        array(0) = ""
        var i = 1
        while (i < array.length) {
          array(i) = results.getObject(i) // note: results are 1-indexed
          i += 1
        }
        array(0) = array.mkString // set the whole row value for reference
        hasNextResult = results.next()
        array
      }
    }

    override def close(): Unit = {
      Option(results).foreach(CloseWithLogging.apply)
      CloseWithLogging(iter)
    }
  }
}
