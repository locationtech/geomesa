/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import com.google.common.primitives.Primitives
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.utils.geotools.SchemaBuilder.{AbstractSchemaBuilder, AttributeBuilder, UserDataBuilder}
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpec
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpec.{ListAttributeSpec, MapAttributeSpec}
import org.locationtech.geomesa.utils.stats.Cardinality.Cardinality
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

import scala.reflect.{ClassTag, classTag}

/**
  * Builder class for creating simple feature types
  *
  * Adding an attribute returns an AttributeBuilder class with additional options for that attribute.
  * There is an implicit conversion back to SchemaBuilder to allow for operation chaining.
  *
  * Example usage:
  *
  *   SchemaBuilder.builder().addString("foo").withIndex().addInt("bar").build("baz")
  *
  */
class SchemaBuilder extends AbstractSchemaBuilder[AttributeBuilder, UserDataBuilder] {
  override protected def createAttributeBuilder(spec: StringBuilder): AttributeBuilder =
    new AttributeBuilder(this, spec)
  override protected def createUserDataBuilder(userData: StringBuilder): UserDataBuilder =
    new UserDataBuilder(this, userData)
}

object SchemaBuilder {

  def builder(): SchemaBuilder = new SchemaBuilder

  /**
    * Implicit function to return from an attribute builder to a schema builder for chaining calls
    *
    * @param b attribute builder
    * @return schema builder
    */
  // noinspection LanguageFeature
  implicit def toSchemaBuilder(b: AttributeBuilder): SchemaBuilder = b.end()

  /**
    * Implicit function to return from a user data builder to a schema builder for chaining calls
    *
    * @param b user data builder
    * @return schema builder
    */
  // noinspection LanguageFeature
  implicit def toSchemaBuilder(b: UserDataBuilder): SchemaBuilder = b.end()

  class AttributeBuilder(parent: SchemaBuilder, spec: StringBuilder) extends
      AbstractAttributeBuilder[AttributeBuilder](parent, spec)

  class UserDataBuilder(parent: SchemaBuilder, spec: StringBuilder) extends
      AbstractUserDataBuilder[UserDataBuilder](parent, spec)

  /**
    * Base parameterized schema builder trait to allow subclassing with correct return type chaining
    */
  trait AbstractSchemaBuilder[A <: AbstractAttributeBuilder[A], U <: AbstractUserDataBuilder[U]] {

    private val specString = new StringBuilder()
    private val userDataString = new StringBuilder(";")

    private val attributeBuilder = createAttributeBuilder(specString)
    private val userDataBuilder = createUserDataBuilder(userDataString)

    /**
      * Add a string-type attribute
      *
      * @param name name of the attribute to add
      * @return attribute builder forchaining additional options
      */
    def addString(name: String): A = add(s"$name:String")

    /**
      * Add an integer-type attribute
      *
      * @param name name of the attribute to add
      * @return attribute builder forchaining additional options
      */
    def addInt(name: String): A = add(s"$name:Int")

    /**
      * Add a long-type attribute
      *
      * @param name name of the attribute to add
      * @return attribute builder forchaining additional options
      */
    def addLong(name: String): A = add(s"$name:Long")

    /**
      * Add a float-type attribute
      *
      * @param name name of the attribute to add
      * @return attribute builder forchaining additional options
      */
    def addFloat(name: String): A = add(s"$name:Float")

    /**
      * Add a double-type attribute
      *
      * @param name name of the attribute to add
      * @return attribute builder forchaining additional options
      */
    def addDouble(name: String): A = add(s"$name:Double")

    /**
      * Add a boolean-type attribute
      *
      * @param name name of the attribute to add
      * @return attribute builder forchaining additional options
      */
    def addBoolean(name: String): A = add(s"$name:Boolean")

    /**
      * Add a UUID-type attribute
      *
      * @param name name of the attribute to add
      * @return attribute builder forchaining additional options
      */
    def addUuid(name: String): A = add(s"$name:UUID")

    /**
      * Add a binary-type (byte array) attribute
      *
      * @param name name of the attribute to add
      * @return attribute builder forchaining additional options
      */
    def addBytes(name: String): A = add(s"$name:Bytes")

    /**
      * Add a json-formatted string-type attribute
      *
      * @param name name of the attribute to add
      * @return attribute builder forchaining additional options
      */
    def addJson(name: String): A = add(s"$name:String:json=true")

    /**
      * Add a date-type attribute
      *
      * @param name name of the attribute to add
      * @param default if this is the default (primary) date field, which will be automatically indexed
      * @return attribute builder forchaining additional options
      */
    def addDate(name: String, default: Boolean = false): A = {
      if (default) {
        userData(SimpleFeatureTypes.Configs.DEFAULT_DATE_KEY, name)
      }
      add(s"$name:Date")
    }

    /**
      * Add a list-type attribute
      *
      * @param name name of the attribute to add
      * @tparam V value type for the list - this must be one of the normal supported types (string, int, etc)
      * @return attribute builder forchaining additional options
      */
    def addList[V: ClassTag](name: String): A =
      add(s"$name:${ListAttributeSpec("", classy[V], Map.empty).getClassSpec}")

    /**
      * Add a map-type attribute
      *
      * @param name name of the attribute to add
      * @tparam K key type of the map - this must be one of the normal supported types (string, int, etc)
      * @tparam V value type of the map - this must be one of the normal supported types (string, int, etc)
      * @return attribute builder forchaining additional options
      */
    def addMap[K: ClassTag, V: ClassTag](name: String): A =
      add(s"$name:${MapAttributeSpec("", classy[K], classy[V], Map.empty).getClassSpec}")

    /**
      * Add a point-type geometry attribute
      *
      * @param name name of the attribute to add
      * @param default if this is the default geometry, which will automatically be indexed
      * @return attribute builder forchaining additional options
      */
    def addPoint(name: String, default: Boolean = false): A =
      add(geom(name, "Point", default))

    /**
      * Add a linestring-type geometry attribute
      *
      * @param name name of the attribute to add
      * @param default if this is the default geometry, which will automatically be indexed
      * @return attribute builder forchaining additional options
      */
    def addLineString(name: String, default: Boolean = false): A =
      add(geom(name, "LineString", default))

    /**
      * Add a polygon-type geometry attribute
      *
      * @param name name of the attribute to add
      * @param default if this is the default geometry, which will automatically be indexed
      * @return attribute builder forchaining additional options
      */
    def addPolygon(name: String, default: Boolean = false): A =
      add(geom(name, "Polygon", default))

    /**
      * Add a multi-point-type geometry attribute
      *
      * @param name name of the attribute to add
      * @param default if this is the default geometry, which will automatically be indexed
      * @return attribute builder forchaining additional options
      */
    def addMultiPoint(name: String, default: Boolean = false): A =
      add(geom(name, "MultiPoint", default))

    /**
      * Add a multi-linestring-type geometry attribute
      *
      * @param name name of the attribute to add
      * @param default if this is the default geometry, which will automatically be indexed
      * @return attribute builder forchaining additional options
      */
    def addMultiLineString(name: String, default: Boolean = false): A =
      add(geom(name, "MultiLineString", default))

    /**
      * Add a multi-polygon-type geometry attribute
      *
      * @param name name of the attribute to add
      * @param default if this is the default geometry, which will automatically be indexed
      * @return attribute builder forchaining additional options
      */
    def addMultiPolygon(name: String, default: Boolean = false): A =
      add(geom(name, "MultiPolygon", default))

    /**
      * Add a geometry-collection-type geometry attribute
      *
      * @param name name of the attribute to add
      * @param default if this is the default geometry, which will automatically be indexed
      * @return attribute builder forchaining additional options
      */
    def addGeometryCollection(name: String, default: Boolean = false): A =
      add(geom(name, "GeometryCollection", default))

    /**
      * Add a mixed-geometry-type geometry attribute. This should be used if you want multiple types of geometries
      * in the same attribute - otherwise prefer one of the explicit geometry types
      *
      * @param name name of the attribute to add
      * @param default if this is the default geometry, which will automatically be indexed
      * @return attribute builder forchaining additional options
      */
    def addMixedGeometry(name: String, default: Boolean = false): A = {
      if (default) {
        userData(SimpleFeatureTypes.Configs.MIXED_GEOMETRIES, "true")
      }
      add(geom(name, "Geometry", default))
    }

    /**
      * Add an attribute based on an attribute descriptor
      *
      * @param ad attribute descriptor
      * @return schema builder for chaining additional calls
      */
    def addAttribute(ad: AttributeDescriptor): A = add(SimpleFeatureSpec.attribute(null, ad).toSpec)

    /**
      * Add feature-level user data
      *
      * @param key user data key
      * @param value user data value
      * @return schema builder for chaining additional calls
      */
    def userData(key: String, value: String): U = userDataBuilder.userData(key, value)

    /**
      * Get a user data builder for additional user data operations
      *
      * @return user data builder for chaining additional calls
      */
    def userData: U = userDataBuilder

    /**
      * Get the current simple feature type specification string
      *
      * @return spec
      */
    def spec: String = specString.toString + userDataString.substring(0, userDataString.length - 1)

    /**
      * Create a new simple feature type using the current attributes
      *
      * @param name simple feature type name
      * @return simple feature type
      */
    def build(name: String): SimpleFeatureType = SimpleFeatureTypes.createType(name, spec)

    /**
      * Create a new simple feature type using the current attributes
      *
      * @param namespace simple feature type namespace
      * @param name simple feature type name
      * @return simple feature type
      */
    def build(namespace: String, name: String): SimpleFeatureType =
      SimpleFeatureTypes.createType(namespace, name, spec)

    protected def add(spec: String): A = {
      if (specString.nonEmpty) {
        specString.append(",")
      }
      specString.append(spec)
      attributeBuilder
    }

    protected def createAttributeBuilder(spec: StringBuilder): A
    protected def createUserDataBuilder(userData: StringBuilder): U

    private def classy[T: ClassTag]: Class[_] = Primitives.wrap(classTag[T].runtimeClass)

    private def geom(name: String, binding: String, default: Boolean): String =
      if (default) { s"*$name:$binding:srid=4326" } else { s"$name:$binding:srid=4326" }
  }

  /**
    * Builder class for configuring per-attribute options
    */
  class AbstractAttributeBuilder[A <: AbstractAttributeBuilder[A]](parent: AbstractSchemaBuilder[_, _],
                                                                   specification: StringBuilder) {

    this: A =>

    import SimpleFeatureTypes.AttributeOptions

    /**
      * Add an index on the current attribute, to facilitate  querying on that attribute
      *
      * @return attribute builder for chaining calls
      */
    def withIndex(): A = withOption(AttributeOptions.OPT_INDEX, "true")

    /**
      * Add an index on the current attribute, to facilitate querying on that attribute
      *
      * @param cardinality a cardinality hint for the attribute - will be considered when picking an index
      *                    during query planning
      * @return attribute builder for chaining calls
      */
    def withIndex(cardinality: Cardinality): A =
      withOptions(AttributeOptions.OPT_INDEX -> "true", AttributeOptions.OPT_CARDINALITY -> cardinality.toString)

    /**
      * Specify column groups for a particular attribute, to speed up querying for subsets of attributes
      *
      * @param groups column groups - preferably short strings (one character is best), case sensitive
      * @return
      */
    def withColumnGroups(groups: String*): A = withOptions(AttributeOptions.OPT_COL_GROUPS -> groups.mkString(","))

    /**
      * Add any attribute-level option
      *
      * @param key option key
      * @param value option value
      * @return attribute builder for chaining calls
      */
    def withOption(key: String, value: String): A = { specification.append(s":$key=$value"); this }

    /**
      * Add multiple attribute-level options at once
      *
      * @param options key/value attribute-level options
      * @return attribute builder for chaining calls
      */
    def withOptions(options: (String, String)*): A = {
      options.foreach { case (k, v) => withOption(k, v) }
      this
    }

    /**
      * End the current attribute and return to the schema builder. Useful for chaining calls. Note that
      * there is an implicit conversion back to SchemaBuilder, so this method does not normally need to be invoked
      *
      * @return schema builder for chaining calls
      */
    def end[B <: AbstractSchemaBuilder[A, _ <: AbstractUserDataBuilder[_]]](): B = parent.asInstanceOf[B]
  }

  /**
    * Builder class for configuring schema-level user data
    */
  class AbstractUserDataBuilder[U <: AbstractUserDataBuilder[U]](parent: AbstractSchemaBuilder[_, _],
                                                                 userData: StringBuilder) {

    this: U =>

    import SimpleFeatureTypes.Configs

    /**
      * Configure the enabled indices for a schema
      *
      * @param names names of the indices to enable (e.g. "z3", "id", etc)
      * @return user data builder for chaining calls
      */
    def indices(names: List[String]): U = userData(Configs.ENABLED_INDICES, names.mkString(","))

    /**
      * Disable indexing on the default date field. If the default date field has not been specified,
      * this will override the default behavior of using the first date-type attribute
      *
      * @return user data builder for chaining calls
      */
    def disableDefaultDate(): U = userData(Configs.IGNORE_INDEX_DTG, "true")

    /**
      * Configure table splits for a schema
      *
      * @param options table splitter options
      * @return user data builder for chaining calls
      */
    def splits(options: Map[String,String]): U =
      userData(Configs.TABLE_SPLITTER_OPTS, options.map { case (k, v) => s"$k:$v" }.mkString(","))

    /**
      * Specify the number of shards to use for the Z indices. Shards can provide distribution
      * across nodes in a cluster, but also require more nodes to be scanned when querying.
      *
      * Default value is 4
      *
      * @param shards number of shards
      * @return user data builder for chaining calls
      */
    def zShards(shards: Int): U = userData(Configs.Z_SPLITS_KEY, shards.toString)

    /**
      * Specify the number of shards to use for the attribute index. Shards can provide distribution
      * across nodes in a cluster, but also require more nodes to be scanned when querying.
      *
      * Default value is 4
      *
      * @param shards number of shards
      * @return user data builder for chaining calls
      */
    def attributeShards(shards: Int): U = userData(Configs.ATTR_SPLITS_KEY, shards.toString)

    /**
      * Specifies that feature IDs are UUIDs. This can save space on disk, as a UUID can be serialized more
      * efficiently than its string representation. Note that if enabled, all feature IDs **must** be
      * valid UUIDs, in the format '28a12c18-e5ae-4c04-ae7b-bf7cdbfaf234'
      *
      * @return user data builder for chaining calls
      */
    def uuidFeatureIds(): U = userData(Configs.FID_UUID_KEY, "true")

    /**
      * Sets the time interval used for binning dates in the Z3 and XZ3 indices
      *
      * @param interval time interval to use
      * @return user data builder for chaining calls
      */
    def z3Interval(interval: TimePeriod): U = userData(Configs.Z3_INTERVAL_KEY, interval.toString)

    /**
      * Set the precision of the XZ index (if used).
      *
      * Default value is 12
      *
      * @param precision precision
      * @return user data builder for chaining calls
      */
    def xzPrecision(precision: Int): U = userData(Configs.XZ_PRECISION_KEY, precision.toString)

    /**
      * Enable date-based table partitioning
      *
      * @return user data builder for chainging calls
      */
    def partitioned(): U = userData(Configs.TABLE_PARTITIONING, "time")

    /**
      * Add arbitrary user data values to the schema
      *
      * @param key user data key
      * @param value user data value
      * @return user data builder for chaining calls
      */
    def userData(key: String, value: String): U = {
      userData.append(SimpleFeatureTypes.encodeUserData(key, value)).append(",")
      this
    }

    /**
      * Add multiple user data values to the schema
      *
      * @param data user data key values
      * @return
      */
    def userData(data: Map[String, String]): U = {
      data.foreach { case (k, v) => userData(k, v) }
      this
    }

    /**
      * End the current user data and return to the schema builder. Useful for chaining calls. Note that
      * this method does not normally need to be invoked explicitly, as there is an implicit conversion back
      * to SchemaBuilder
      *
      * @return schema builder for chaining calls
      */
    def end[B <: AbstractSchemaBuilder[_ <: AbstractAttributeBuilder[_], U]](): B = parent.asInstanceOf[B]
  }
}
