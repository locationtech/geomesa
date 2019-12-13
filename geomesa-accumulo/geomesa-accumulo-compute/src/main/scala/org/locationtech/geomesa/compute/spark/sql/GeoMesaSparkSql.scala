/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.compute.spark.sql

import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Date, List => jList, Map => jMap, UUID}

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Geometry
import org.apache.hadoop.conf.Configuration
import org.apache.metamodel.query.FilterClause
import org.apache.metamodel.{DataContext, query}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, _}
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStoreFinder, DataUtilities, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.DuplicatingFilterVisitor
import org.locationtech.geomesa.compute.spark.GeoMesaSpark
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.filter.expression.PropertyName

import scala.collection.JavaConversions._

/**
 * Class to manage running sql queries against geomesa using spark.
 *
 * There can only be a single spark context running in a given jvm, so it has to be managed using the
 * start/stop/register methods.
 */
@deprecated
object GeoMesaSparkSql extends LazyLogging {

  private val ff = CommonFactoryFinder.getFilterFactory2

  // state to keep track of our sfts and data store connection parameters
  private val dsParams = scala.collection.mutable.Set.empty[Map[String, String]]
  private val sfts = scala.collection.mutable.Set.empty[SimpleFeatureType]

  // singleton spark context
  private var sc: SparkContext = null
  private var sparkSql: GeoMesaSparkSql = null
  private var running = false
  private val executing = new AtomicInteger(0)

  /**
   * Register a data store. This makes all schemas in the data store available for querying.
   * Synchronized to ensure it's only called when the spark context is not running.
   */
  def registerDataStore(params: Map[String, String]): Unit = synchronized {
    require(!running, "Can't register a data store in a running instance")
    val ds = DataStoreFinder.getDataStore(params)
    require(ds != null, "No data store found using provided parameters")
    dsParams += params
    sfts ++= ds.getTypeNames.map(ds.getSchema)
  }

  /**
   * Starts the spark context, if not already running.
   */
  def start(configs: Map[String, String] = Map.empty,
            distributedJars: Seq[String] = Seq.empty): Boolean = synchronized {
    if (running) {
      logger.debug("Trying to start an already started instance")
      false
    } else {
      val conf = GeoMesaSpark.init(new SparkConf(), sfts.toSeq)
      conf.setAppName("GeoMesaSql")
      conf.setMaster("yarn-client")
      conf.setJars(distributedJars)
      configs.foreach { case (k, v) => conf.set(k, v) }
      sc = new SparkContext(conf)
      sparkSql = new GeoMesaSparkSql(sc, dsParams.toSeq)
      running = true
      true
    }
  }

  /**
   * Stops the spark context, if running. Blocks until all current processes have finished executing.
   * Note that the synchronization on this method will prevent new tasks from executing.
   *
   * @param wait
   *             if < 0, will block indefinitely
   *             if >= 0, will return after that many millis
   * @return true if successfully stopped, else false
   */
  def stop(wait: Long = -1): Boolean = synchronized {
    if (running) {
      val start = System.currentTimeMillis()
      // wait for current queries to stop
      while (executing.get() > 0 && (wait == -1 || System.currentTimeMillis() - start < wait)) {
        Thread.sleep(1000)
      }
      if (executing.get() > 0) {
        return false
      }
      sc.stop()
      sc = null
      sparkSql = null
      running = false
    } else {
      logger.debug("Trying to stop an already stopped instance")
    }
    true
  }

  /**
   * Execute a sql query against geomesa. Where clause is interpreted as CQL.
   */
  def execute(sql: String): (StructType, Array[Row]) = {
    val canStart = synchronized {
      // we need to compare and modify the state inside the synchronized block
      if (running) {
        executing.incrementAndGet()
      }
      running
    }
    require(canStart, "Can only execute in a running instance")

    try {
      val results = sparkSql.query(sql)
      // return the result schema and rows
      (results.schema, results.collect())
    } finally {
      executing.decrementAndGet()
    }
  }

  /**
   * Extracts CQL from the SQL query.
   */
  private def extractCql(where: FilterClause,
                         context: DataContext,
                         sftNames: Seq[String]): Map[String, Filter] = {
    val sqlVisitor = new SqlVisitor(context, sftNames)
    val result = scala.collection.mutable.Map.empty[String, Filter]
    // items should have an expression if they can't be parsed as SQL
    // we interpret that to mean that they are CQL instead
    where.getItems.flatMap(i => Option(i.getExpression)).map(ECQL.toFilter).foreach { filter =>
      sqlVisitor.referencedSfts.clear()
      val updated = filter.accept(sqlVisitor, null).asInstanceOf[Filter]
      require(sqlVisitor.referencedSfts.size == 1, "CQL filters across multiple tables are not supported")
      val typeName = sqlVisitor.referencedSfts.head
      result.put(typeName, result.get(typeName).map(c => ff.and(updated, c)).getOrElse(updated))
    }
    result.toMap
  }

  /**
   * Get the attribute names referenced in the query - used to select a subset of attributes from geomesa
   */
  def extractAttributeNames(sql: query.Query, cql: Map[String, Filter]): Map[String, Set[String]] = {
    val namesFromCql = cql.mapValues(DataUtilities.attributeNames(_).toSet)
    val namesFromSql = scala.collection.mutable.Map.empty[String, Set[String]]

    // we ignore the 'having' clause as it should always reference something from the select
    val selects = sql.getSelectClause.getItems ++
        sql.getWhereClause.getEvaluatedSelectItems ++
        sql.getGroupByClause.getEvaluatedSelectItems ++
        sql.getOrderByClause.getEvaluatedSelectItems
    selects.flatMap(s => Option(s.getColumn)).foreach { c =>
      val table = c.getTable.getName
      namesFromSql.put(table, namesFromSql.get(table).map(_ ++ Set(c.getName)).getOrElse(Set(c.getName)))
    }

    // combine the two maps
    namesFromSql.toMap ++ namesFromCql.map { case (k,v) =>
      k -> namesFromSql.get(k).map(_ ++ v.toSet).getOrElse(v.toSet)
    }
  }

  /**
   * Converts a simple feature attribute into a SQL data type
   */
  private def types(d: AttributeDescriptor): DataType = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
    if (d.isList) {
      DataTypes.createArrayType(types(d.getListType()))
    } else if (d.isMap) {
      val (keyClass, valueClass) = d.getMapTypes()
      DataTypes.createMapType(types(keyClass), types(valueClass))
    } else {
      types(d.getType.getBinding)
    }
  }

  /**
   * Converts a simple class type into a SQL data type
   */
  private def types(clas: Class[_]): DataType = {
    if (classOf[java.lang.String].isAssignableFrom(clas)) {
      StringType
    } else if (classOf[java.lang.Integer].isAssignableFrom(clas)) {
      IntegerType
    } else if (classOf[java.lang.Long].isAssignableFrom(clas)) {
      LongType
    } else if (classOf[java.lang.Float].isAssignableFrom(clas)) {
      FloatType
    } else if (classOf[java.lang.Double].isAssignableFrom(clas)) {
      DoubleType
    } else if (classOf[java.lang.Boolean].isAssignableFrom(clas)) {
      BooleanType
    } else if (classOf[java.util.Date].isAssignableFrom(clas)) {
      TimestampType
    } else if (classOf[UUID].isAssignableFrom(clas)) {
      StringType
    } else if (classOf[Geometry].isAssignableFrom(clas)) {
      StringType
    } else {
      throw new NotImplementedError(s"Binding $clas is not supported")
    }
  }
}

@deprecated
class GeoMesaSparkSql(sc: SparkContext, dsParams: Seq[Map[String, String]]) {

  // load up our sfts
  val sftsByName = dsParams.flatMap { params =>
    val ds = DataStoreFinder.getDataStore(params)
    require(ds != null, "No data store found using provided parameters")
    ds.getTypeNames.map { name =>
      val schema = ds.getSchema(name)
      name -> (schema,  params)
    }
  }.foldLeft(Map.empty[String, (SimpleFeatureType, Map[String, String])])(_ + _)

  private val dataContext = new GeoMesaDataContext(sftsByName.mapValues(_._1))

  /**
   * Execute a sql query against geomesa. Where clause is interpreted as CQL.
   */
  def query(sql: String): DataFrame = {
    val parsedSql = dataContext.parseQuery(sql)

    // extract the feature types from the from clause
    val typeNames = parsedSql.getFromClause.getItems.map(_.getTable.getName)
    val sftsWithParams = typeNames.map(sftsByName.apply)

    // extract the cql from the where clause
    val where = parsedSql.getWhereClause
    val cql = GeoMesaSparkSql.extractCql(where, dataContext, typeNames)
    // clear out the cql from the where clause so spark doesn't try to parse it
    // if it' a sql expression, the expression field will be null
    // otherwise it has the raw expression, which we assume is cql
    where.getItems.filter(_.getExpression != null).foreach(where.removeItem)
    val sqlWithoutCql = parsedSql.toSql

    // restrict the attributes coming back to speed up the query
    val attributesByType = GeoMesaSparkSql.extractAttributeNames(parsedSql, cql)

    val sqlContext = new SQLContext(sc)

    // for each input sft, set up the sql table with the results from querying geomesa with the cql filter
    sftsWithParams.foreach { case (sft, params) =>
      val typeName = sft.getTypeName
      val allAttributes = sft.getAttributeDescriptors.map(_.getLocalName)
      val attributes = {
        val extracted = attributesByType(typeName).toList
        if (extracted.sorted == allAttributes.sorted) {
          None // if we've got all attributes, we don't need a transform
        } else {
          Some(extracted.toArray)
        }
      }
      val filter = cql.getOrElse(typeName, Filter.INCLUDE)
      val query = new Query(typeName, filter)
      attributes.foreach(query.setPropertyNames)

      // generate the sql schema based on the sft/query attributes
      val fields = attributes.getOrElse(allAttributes.toArray).map { field =>
        StructField(field, GeoMesaSparkSql.types(sft.getDescriptor(field)), nullable = true)
      }
      val schema = StructType(fields)

      // create an rdd from the query
      val features = GeoMesaSpark.rdd(new Configuration(), sc, params, query)

      // convert records to rows - convert the values to sql-compatible ones
      val rowRdd = features.map { f =>
        val sqlAttributes = f.getAttributes.map {
          case g: Geometry => WKTUtils.write(g) // text
          case d: Date     => new Timestamp(d.getTime) // sql timestamp
          case u: UUID     => u.toString // text
          case a           => a // others should map natively without explict conversion
        }
        Row(sqlAttributes: _*)
      }

      // apply the schema to the rdd
      val featuresDataFrame = sqlContext.createDataFrame(rowRdd, schema)

      // register the data frame as a table, so that it's available to the sql engine
      featuresDataFrame.registerTempTable(typeName)
    }

    // run the sql statement against our registered tables
    sqlContext.sql(sqlWithoutCql)
  }
}

/**
 * Extracts property names from a filter. Names are expected to either be qualified with the
 * feature type name (e.g. mysft.myattr), or be unambiguous among the feature types being queried.
 */
@deprecated
class SqlVisitor(context: DataContext, sftNames: Seq[String]) extends DuplicatingFilterVisitor {

  val referencedSfts = scala.collection.mutable.Set.empty[String]

  override def visit(expression: PropertyName, extraData: AnyRef): AnyRef = {
    val name = expression.getPropertyName
    require(name != null && !name.isEmpty, "Property name is ambiguous: 'null'")

    val parts = name.split("\\.|/") // ECQL converts '.' into '/' in properties, so we have to match both
    require(parts.length < 3, s"Ambiguous property name in filter: '$name")

    if (parts.length == 2) {
      // qualified by sft name
      val matching = sftNames.filter(_ == parts.head)
      require(matching.nonEmpty, s"Property name does not match a table in from clause: '$name")
      referencedSfts.add(matching.head)
      getFactory(extraData).property(parts(1), expression.getNamespaceContext)
    } else {
      // not qualified - see if it unambiguously matches any of the tables
      val matching = sftNames.map(context.getTableByQualifiedLabel).flatMap(_.getColumns.find(_.getName == name))
      require(matching.nonEmpty, s"Property name does not match a table in from clause: '$name")
      require(matching.length == 1, s"Property name is ambiguous: '$name'")
      referencedSfts.add(matching.head.getTable.getName)
      expression
    }
  }
}
