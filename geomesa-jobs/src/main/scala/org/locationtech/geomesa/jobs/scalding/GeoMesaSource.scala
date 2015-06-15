/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs.scalding

import cascading.tuple._
import com.twitter.scalding._
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.jobs.scalding.taps.{GeoMesaLocalScheme, GeoMesaLocalTap, GeoMesaScheme, GeoMesaTap}
import org.opengis.feature.simple.SimpleFeature

/**
 * Source or sink for accessing GeoMesa
 */
case class GeoMesaSource(options: GeoMesaSourceOptions)
    extends Source with TypedSource[(Text, SimpleFeature)] with TypedSink[(Text, SimpleFeature)] {

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): GenericTap =
    mode match {
      case _: Hdfs  => GeoMesaTap(readOrWrite, GeoMesaScheme(options))
      case _: Local => GeoMesaLocalTap(readOrWrite, GeoMesaLocalScheme(options))
      case _: Test  => TestTapFactory(this, GeoMesaScheme(options).asInstanceOf[GenericScheme]).createTap(readOrWrite)
      case _        => throw new NotImplementedError()
    }
  override def sourceFields: Fields = GeoMesaSource.fields

  override def converter[U >: (Text, SimpleFeature)]: TupleConverter[U] = new TupleConverter[U] {
    override val arity: Int = 2
    override def apply(te: TupleEntry): (Text, SimpleFeature) =
      (te.getObject(0).asInstanceOf[Text], te.getObject(1).asInstanceOf[SimpleFeature])
  }

  override def sinkFields: Fields = GeoMesaSource.fields

  override def setter[U <: (Text, SimpleFeature)]:  TupleSetter[U] = new TupleSetter[U] {
    override def arity: Int = 2
    override def apply(arg: U): Tuple = new Tuple(arg._1, arg._2)
  }
}

object GeoMesaSource {
  def fields: Fields = new Fields("id", "sf")
}

/**
 * Common trait for source/sink options
 */
sealed trait GeoMesaSourceOptions {
  def dsParams: Map[String, String]
}

/**
 * Options for configuring GeoMesa as a source
 */
case class GeoMesaInputOptions(dsParams: Map[String, String],
                               feature: String,
                               filter: Option[String] = None,
                               transform: Option[Array[String]] = None) extends GeoMesaSourceOptions {
  override val toString = s"GeoMesaInputOptions[${dsParams.getOrElse("instanceId", "None")}," +
      s"${dsParams.getOrElse("tableName", "None")},$feature,${filter.getOrElse("INCLUDE")}]"
}

/**
 * Options for configuring GeoMesa as a sink
 */
case class GeoMesaOutputOptions(dsParams: Map[String, String]) extends GeoMesaSourceOptions {
  override val toString = s"GeoMesaOutputOptions[${dsParams.getOrElse("instanceId", "None")}]"
}