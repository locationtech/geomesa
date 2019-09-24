/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.apache.parquet.filter2.predicate.FilterPredicate
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.process.vector.TransformProcess
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.parquet.FilterConverter.reduce
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.filter.expression.{Expression, PropertyName}

package object parquet {

  import scala.collection.JavaConverters._

  /**
    * Schema to read and schema to return.
    *
    * If we have to return a different feature than we read, we need to apply a secondary transform.
    * Otherwise, we can just do the transform on read and skip the secondary transform
    *
    * @param read read schema, includes fields to filter on
    * @param transform return schema, if different from read schema
    */
  case class ReadSchema(read: SimpleFeatureType, transform: Option[(String, SimpleFeatureType)])

  /**
    * Filter to read files
    *
    * @param parquet parquet filter that we can push down to the file format
    * @param residual residual geotools filter that we have to apply after read
    */
  case class ReadFilter(parquet: Option[FilterPredicate], residual: Option[Filter])

  object ReadSchema {

    /**
      * Calculates the read schema
      *
      * @param sft simple feature type
      * @param filter query filter
      * @param transform query transform
      * @return
      */
    def apply(
        sft: SimpleFeatureType,
        filter: Option[Filter],
        transform: Option[(String, SimpleFeatureType)]): ReadSchema = {
      transform match {
        case None => ReadSchema(sft, None)
        case Some((tdefs, _)) =>
          var secondary = false
          // note: update `secondary` in our .flatMap to avoid a second pass over the expressions
          val transformCols = {
            val all = TransformProcess.toDefinition(tdefs).asScala.map(_.expression).flatMap {
              case p: PropertyName => Seq(p.getPropertyName)
              case e: Expression => secondary = true; FilterHelper.propertyNames(e, sft)
            }
            all.distinct
          }
          val filterCols = filter match {
            case None => Seq.empty
            case Some(f) => FilterHelper.propertyNames(f, sft).filterNot(transformCols.contains)
          }

          val projectedSft = {
            val builder = new SimpleFeatureTypeBuilder()
            builder.setName(sft.getName)
            transformCols.foreach(a => builder.add(sft.getDescriptor(a)))
            filterCols.foreach(a => builder.add(sft.getDescriptor(a)))
            builder.buildFeatureType()
          }
          projectedSft.getUserData.putAll(sft.getUserData)

          ReadSchema(projectedSft, if (secondary || filterCols.nonEmpty) { transform } else { None })
      }
    }
  }

  object ReadFilter {

    /**
      * Create a read filter
      *
      * @param sft simple feature type
      * @param filter query filter
      * @return
      */
    def apply(sft: SimpleFeatureType, filter: Option[Filter]): ReadFilter = {
      val (parquet, residual) = filter match {
        case None | Some(Filter.INCLUDE) => (None, None)
        case Some(f) => FilterConverter.convert(sft, f)
      }
      ReadFilter(parquet, residual)
    }
  }
}
