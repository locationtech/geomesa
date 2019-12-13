/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.utils

import java.io.{Closeable, Flushable, IOException}
import java.util.Collections

import org.apache.kudu.client.{KuduScanner, KuduSession, OperationResponse, RowResult}
import org.locationtech.geomesa.utils.collection.CloseableIterator

import scala.annotation.tailrec

object RichKuduClient {

  implicit class RichScanner(val scanner: KuduScanner) extends AnyVal {

    // note: RowResult instances are re-used by the underlying scanner, so don't cache the values of this scanner
    def iterator: CloseableIterator[RowResult] = new CloseableIterator[RowResult] {

      private var iter = Collections.emptyIterator[RowResult]()

      @tailrec
      override def hasNext: Boolean = iter.hasNext || {
        if (!scanner.hasMoreRows) { false } else {
          iter = scanner.nextRows()
          hasNext
        }
      }

      override def next(): RowResult = iter.next

      override def close(): Unit = scanner.close()
    }
  }

  /**
    * Wrapper for kudu session that eases closing and flushing.
    *
    * Kudu session doesn't implement closeable or flushable, and doesn't match AnyCloseable/AnyFlushable
    * because the close and flush methods return values
    *
    * @param session session
    */
  case class SessionHolder(session: KuduSession) extends Closeable with Flushable {

    override def close(): Unit = handleErrors(session.close())
    override def flush(): Unit = handleErrors(session.flush())

    private def handleErrors(response: java.util.List[OperationResponse]): Unit = {
      import scala.collection.JavaConverters._

      val errors = response.asScala.collect { case row if row.hasRowError => row.getRowError }
      if (errors.nonEmpty) {
        val e = new RuntimeException("Error closing session")
        errors.foreach(error => e.addSuppressed(new IOException(error.toString)))
        throw e
      }
    }
  }
}
