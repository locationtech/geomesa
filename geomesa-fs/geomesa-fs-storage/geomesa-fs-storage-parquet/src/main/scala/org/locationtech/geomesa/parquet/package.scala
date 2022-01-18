/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.apache.parquet.filter2.predicate.FilterPredicate
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.utils.geotools.Transform.{ExpressionTransform, PropertyTransform, RenameTransform, Transforms}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

package object parquet {

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

    import org.locationtech.geomesa.filter.RichTransform.RichTransform

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
          val definitions = Transforms(sft, tdefs)
          val secondary = definitions.exists {
            case _: PropertyTransform   => false
            case _: RenameTransform     => false
            case _: ExpressionTransform => true
          }
          val transformCols = definitions.flatMap(_.properties).distinct
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
