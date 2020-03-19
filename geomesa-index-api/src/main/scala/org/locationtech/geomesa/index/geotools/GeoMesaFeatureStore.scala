/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import java.util.{List => jList}

import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.feature._
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.`type`.{AttributeDescriptor, Name}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.identity.FeatureId

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class GeoMesaFeatureStore(ds: DataStore with HasGeoMesaStats, sft: SimpleFeatureType, runner: QueryRunner)
    extends GeoMesaFeatureSource(ds, sft, runner) with SimpleFeatureStore {

  private var transaction: Transaction = Transaction.AUTO_COMMIT

  override def addFeatures(collection: FeatureCollection[SimpleFeatureType, SimpleFeature]): jList[FeatureId] = {
    if (collection.isEmpty) {
      return List.empty[FeatureId]
    }

    val fids = new java.util.ArrayList[FeatureId](collection.size())
    val errors = ArrayBuffer.empty[Throwable]

    WithClose(collection.features, writer(None)) { case (features, writer) =>
      while (features.hasNext) {
        try { fids.add(FeatureUtils.write(writer, features.next()).getIdentifier) } catch {
          // validation errors in indexing will throw an IllegalArgumentException
          // make the caller handle other errors, which are likely related to the underlying database,
          // as we wouldn't know which features were actually written or not due to write buffering
          case e: IllegalArgumentException => errors.append(e)
        }
      }
    }

    if (errors.isEmpty) { fids } else {
      val e = new IllegalArgumentException("Some features were not written:")
      // suppressed exceptions should contain feature ids and attributes
      errors.foreach(e.addSuppressed)
      throw e
    }
  }

  override def setFeatures(reader: FeatureReader[SimpleFeatureType, SimpleFeature]): Unit = {
    removeFeatures(Filter.INCLUDE)
    try {
      WithClose(writer(None)) { writer =>
        while (reader.hasNext) {
          FeatureUtils.write(writer, reader.next())
        }
      }
    } finally {
      reader.close()
    }
  }

  override def modifyFeatures(attributes: Array[String], values: Array[AnyRef], filter: Filter): Unit =
    modifyFeatures(attributes.map(new NameImpl(_).asInstanceOf[Name]), values, filter)

  override def modifyFeatures(attribute: String, value: AnyRef, filter: Filter): Unit =
    modifyFeatures(Array[Name](new NameImpl(attribute)), Array(value), filter)

  override def modifyFeatures(attribute: Name, value: AnyRef, filter: Filter): Unit =
    modifyFeatures(Array(attribute), Array(value), filter)

  override def modifyFeatures(attributes: Array[Name], values: Array[AnyRef], filter: Filter): Unit = {
    val sft = getSchema

    require(filter != null, "Filter must not be null")
    attributes.foreach(a => require(sft.getDescriptor(a) != null, s"$a is not an attribute of ${sft.getName}"))
    require(attributes.length == values.length, "Modified names and values don't match")

    WithClose(writer(Some(filter))) { writer =>
      while (writer.hasNext) {
        val sf = writer.next()
        var i = 0
        while (i < attributes.length) {
          try {
            sf.setAttribute(attributes(i), values(i))
          } catch {
            case e: Exception =>
              throw new DataSourceException("Error updating feature " +
                  s"'${sf.getID}' with ${attributes(i)}=${values(i)}", e)
          }
          i += 1
        }
        writer.write()
      }
    }
  }

  override def removeFeatures(filter: Filter): Unit = {
    ds match {
      case gm: GeoMesaDataStore[_] if filter == Filter.INCLUDE =>
        if (TablePartition.partitioned(sft)) {
          gm.manager.indices(sft).par.foreach(index => gm.adapter.deleteTables(index.deleteTableNames(None)))
        } else {
          gm.manager.indices(sft).par.foreach { index =>
            val prefix = Some(index.keySpace.sharing).filterNot(_.isEmpty)
            gm.adapter.clearTables(index.getTableNames(None), prefix)
          }
        }
        gm.stats.writer.clear(sft)

      case _ =>
        WithClose(writer(Some(filter))) { writer =>
          while (writer.hasNext) {
            writer.next()
            writer.remove()
          }
        }
    }
  }

  override def setTransaction(transaction: Transaction): Unit = {
    require(transaction != null, "Transaction can't be null - did you mean Transaction.AUTO_COMMIT?")
    if (ds.isInstanceOf[GeoMesaDataStore[_]] && transaction != Transaction.AUTO_COMMIT) {
      logger.warn("Ignoring transaction - not supported")
    }
    this.transaction = transaction
  }

  override def getTransaction: Transaction = transaction

  // removed in gt-23, but keep around for compatibility with older versions
  def modifyFeatures(attribute: AttributeDescriptor, value: AnyRef, filter: Filter): Unit =
    modifyFeatures(attribute.getName, value, filter)
  def modifyFeatures(attributes: Array[AttributeDescriptor], values: Array[AnyRef], filter: Filter): Unit =
    modifyFeatures(attributes.map(_.getName), values, filter)

  private def writer(filter: Option[Filter]): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    ds match {
      case gm: GeoMesaDataStore[_] => gm.getFeatureWriter(sft, filter)
      case _ if filter.isEmpty => ds.getFeatureWriterAppend(sft.getTypeName, transaction)
      case _ => ds.getFeatureWriter(sft.getTypeName, filter.get, transaction)
    }
  }
}
