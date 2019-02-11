/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.util

import java.util.Locale

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.accumulo.util.AccumuloSchemaBuilder.{AccumuloAttributeBuilder, AccumuloUserDataBuilder}
import org.locationtech.geomesa.utils.geotools.SchemaBuilder.{AbstractAttributeBuilder, AbstractSchemaBuilder, AbstractUserDataBuilder}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats.Cardinality.Cardinality
import org.locationtech.geomesa.utils.stats.IndexCoverage

class AccumuloSchemaBuilder extends AbstractSchemaBuilder[AccumuloAttributeBuilder, AccumuloUserDataBuilder] {
  override protected def createAttributeBuilder(spec: StringBuilder): AccumuloAttributeBuilder =
    new AccumuloAttributeBuilder(this, spec)
  override protected def createUserDataBuilder(userData: StringBuilder): AccumuloUserDataBuilder =
    new AccumuloUserDataBuilder(this, userData)
}

object AccumuloSchemaBuilder {

  def builder(): AccumuloSchemaBuilder = new AccumuloSchemaBuilder()

  /**
    * Implicit function to return from an attribute builder to a schema builder for chaining calls
    *
    * @param b field builder
    * @return attribute builder
    */
  // noinspection LanguageFeature
  implicit def toSchemaBuilder(b: AccumuloAttributeBuilder): AccumuloSchemaBuilder = b.end()

  /**
    * Implicit function to return from a user data builder to a schema builder for chaining calls
    *
    * @param b user data builder
    * @return schema builder
    */
  // noinspection LanguageFeature
  implicit def toSchemaBuilder(b: AccumuloUserDataBuilder): AccumuloSchemaBuilder = b.end()

  class AccumuloAttributeBuilder(parent: AccumuloSchemaBuilder, specification: StringBuilder)
      extends AbstractAttributeBuilder[AccumuloAttributeBuilder](parent, specification) {

    import SimpleFeatureTypes.AttributeOptions

    /**
      * Add a join index on the current attribute. This uses a reduced format to save disk space,
      * but queries may need to 'join' against the record table if they return attributes not in the reduced
      * format
      *
      * @return attribute builder for chaining calls
      */
    def withJoinIndex(): AccumuloAttributeBuilder =
      withOption(AttributeOptions.OPT_INDEX, IndexCoverage.JOIN.toString.toLowerCase(Locale.US))

    /**
      * Add a join index on the current attribute. This uses a reduced format to save disk space,
      * but queries may need to 'join' against the record table if they return attributes not in the reduced
      * format
      *
      * @param cardinality a cardinality hint for the attribute - will be considered when picking an index
      *                    during query planning
      * @return attribute builder for chaining calls
      */
    def withJoinIndex(cardinality: Cardinality): AccumuloAttributeBuilder =
      withJoinIndex().withOption(AttributeOptions.OPT_CARDINALITY, cardinality.toString)

    /**
      * Adds a full (covering) index. Equivalent to `addIndex`
      *
      * @return attribute builder for chaining calls
      */
    def withFullIndex(): AccumuloAttributeBuilder = withIndex()

    /**
      * Adds a full (covering) index. Equivalent to `addIndex`
      *
      * @param cardinality a cardinality hint for the attribute - will be considered when picking an index
      *                    during query planning
      * @return attribute builder for chaining calls
      */
    def withFullIndex(cardinality: Cardinality): AccumuloAttributeBuilder = withIndex(cardinality)

    /**
      * Keep this attribute as an value in the reduced join attribute index format
      *
      * @return attribute builder for chaining calls
      */
    def withIndexValue(): AccumuloAttributeBuilder = withOption(AttributeOptions.OPT_INDEX_VALUE, "true")

    /**
      * Enable tracking summary statistics for this attribute during ingest
      *
      * @return attribute builder for chaining calls
      */
    def withStats(): AccumuloAttributeBuilder = withOption(AttributeOptions.OPT_STATS, "true")
  }

  class AccumuloUserDataBuilder(parent: AccumuloSchemaBuilder, userData: StringBuilder)
      extends AbstractUserDataBuilder[AccumuloUserDataBuilder](parent, userData) with LazyLogging {

    /**
      * Sets table sharing for this schema
      *
      * @param sharing table sharing
      * @return user data builder for call chaining
      */
    @deprecated("table sharing is no longer supported")
    def tableSharing(sharing: Boolean): AccumuloUserDataBuilder = {
      if (sharing) {
        logger.warn("Ignoring table sharing hint - table sharing is no longer supported")
      }
      this
    }

    /**
      * Set logical timestamps for the Accumulo tables
      *
      * @param logical logical time
      * @return user data builder for call chaining
      */
    def logicalTime(logical: Boolean): AccumuloUserDataBuilder =
      userData(SimpleFeatureTypes.Configs.LOGICAL_TIME_KEY, logical.toString)
  }
}
