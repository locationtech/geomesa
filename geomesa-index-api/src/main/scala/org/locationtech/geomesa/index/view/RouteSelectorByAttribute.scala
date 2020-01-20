/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import org.geotools.data.{DataStore, Query}
import org.locationtech.geomesa.filter.FilterHelper
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

/**
  * Selects routes based on the attributes present in the query filter
  */
class RouteSelectorByAttribute extends RouteSelector {

  import RouteSelectorByAttribute.RouteAttributes

  import scala.collection.JavaConverters._

  private var mappings: Seq[(Set[String], DataStore)] = Seq.empty
  private var id: Option[DataStore] = None
  private var include: Option[DataStore] = None

  override def init(stores: Seq[(DataStore, java.util.Map[String, _ <: AnyRef])]): Unit = {
    val builder = Seq.newBuilder[(Set[String], DataStore)]

    stores.foreach { case (ds, config) =>
      Option(config.get(RouteAttributes)).foreach { attributes =>
        try {
          attributes.asInstanceOf[java.util.List[AnyRef]].asScala.foreach {
            case s: String if s.equalsIgnoreCase("id") => setId(ds)
            case s: String => builder += Set(s) -> ds
            case list: java.util.List[String] if list.isEmpty => setInclude(ds)
            case list: java.util.List[String] if list.size() == 1 && list.get(0).equalsIgnoreCase("id") => setId(ds)
            case list: java.util.List[String] => builder += list.asScala.toSet -> ds
            case i => throw new IllegalArgumentException(s"Expected List[String] bug got: $i")
          }
        } catch {
          case NonFatal(e) => throw new IllegalArgumentException(s"Invalid store routing: $attributes", e)
        }
      }
    }
    mappings = builder.result

    val check = scala.collection.mutable.Set.empty[Set[String]]
    mappings.foreach { case (m, _) =>
      if (!check.add(m)) {
        throw new IllegalArgumentException("Invalid store routing: " +
            s"route [${m.mkString(", ")}] is defined more than once")
      }
    }
  }

  override def route(sft: SimpleFeatureType, query: Query): Option[DataStore] = {
    def attributes: Option[DataStore] = {
      val names = FilterHelper.propertyNames(query.getFilter, sft).toSet
      if (names.isEmpty) { None } else {
        mappings.collectFirst { case (routes, ds) if routes.forall(names.contains) => ds }
      }
    }

    if (FilterHelper.hasIdFilter(query.getFilter)) {
      id.orElse(attributes.orElse(include))
    } else {
      attributes.orElse(include)
    }
  }

  private def setId(ds: DataStore): Unit = {
    if (id.isDefined) {
      throw new IllegalArgumentException("Invalid store routing: 'id' route is defined more than once")
    }
    id = Some(ds)
  }

  private def setInclude(ds: DataStore): Unit = {
    if (include.isDefined) {
      throw new IllegalArgumentException("Invalid store routing: 'include' route is defined more than once")
    }
    include = Some(ds)
  }
}

object RouteSelectorByAttribute {
  val RouteAttributes = "geomesa.route.attributes"
}
