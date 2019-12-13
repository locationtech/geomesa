/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.io.{File, FileWriter}

import com.typesafe.config.ConfigFactory
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType


case class AttributeDetails(unsafeName: String, index: Int, clazz: String) {
  val name = unsafeName.replaceAll("\\W", "_")
  def getter: String = s"def $name(): $clazz = sf.getAttribute($index).asInstanceOf[$clazz]"
  def optionGetter: String = s"def ${name}Opt(): Option[$clazz] = Option($name())"
  def setter: String = s"def set${name.capitalize}(x: $clazz): Unit = sf.setAttribute($index, x)"
}

object AttributeDetails {
  import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors._

  def apply(ad: AttributeDescriptor, sft: SimpleFeatureType): AttributeDetails = {
    val majorBinding = classToString(Some(ad.getType.getBinding))
    val binding = if (ad.isList) {
      val subtype = classToString(Option(ad.getListType()))
      s"$majorBinding[$subtype]"
    } else if (ad.isMap) {
      val types = ad.getMapTypes()
      val keyType = classToString(Option(types._1))
      val valueType = classToString(Option(types._2))
      s"$majorBinding[$keyType,$valueType]"
    } else {
      majorBinding
    }
    AttributeDetails(ad.getLocalName, sft.indexOf(ad.getLocalName), binding)
  }

  private def classToString(clas: Option[Class[_]]) = clas.map(_.getCanonicalName).getOrElse("String")
}

object GenerateFeatureWrappers {

  val className = "SimpleFeatureWrappers"

  /**
   * Builds all implicit classes in a wrapper object
   *
   * @param sfts
   * @param pkg
   * @return
   */
  def buildAllClasses(sfts: Seq[SimpleFeatureType], pkg: String): String = {
    val sb = new StringBuilder()
    sb.append(s"package $pkg\n\n")
    sb.append(s"object $className {")
    sfts.foreach(sft => sb.append("\n\n").append(buildClass(sft, "  ")))
    sb.append("\n}")
    sb.toString()
  }

  /**
   * Builds a single implicit class
   *
   * @param sft
   * @param tab
   * @return
   */
  def buildClass(sft: SimpleFeatureType, tab: String): String = {
    import scala.collection.JavaConversions._

    val attrs = sft.getAttributeDescriptors.map(AttributeDetails(_, sft))

    val sb = new StringBuilder()
    sb.append(s"${tab}implicit class ${sft.getTypeName}")
    sb.append("(val sf: org.opengis.feature.simple.SimpleFeature) extends AnyVal {\n")
    attrs.foreach { a =>
      sb.append("\n")
      sb.append(s"$tab  ${a.getter}\n")
      sb.append(s"$tab  ${a.optionGetter}\n")
      sb.append(s"$tab  ${a.setter}\n")
    }
    sb.append(
      s"""
        |$tab  def debug(): String = {
        |$tab    import scala.collection.JavaConversions._
        |$tab    val sb = new StringBuilder(s"$${sf.getType.getTypeName}:$${sf.getID}")
        |$tab    sf.getProperties.foreach(p => sb.append(s"|$${p.getName.getLocalPart}=$${p.getValue}"))
        |$tab    sb.toString()
        |$tab  }
        |""".stripMargin)
    sb.append(s"$tab}")

    sb.toString()
  }

  /**
   * Recursively looks for configuration files of the pattern 'format-*.conf'
   *
   * @param file
   * @return
   */
  def findFormatFiles(file: File): Seq[File] = {
    if (!file.isDirectory) {
      val name = file.getName
      if (name.startsWith("format-") && name.endsWith(".conf")) {
        Seq(file)
      } else {
        Seq.empty
      }
    } else {
      file.listFiles().flatMap(findFormatFiles)
    }
  }

  /**
   * Creates implicit wrappers for any typesafe config format files found under src/main/resources
   *
   * @param args (0) - base directory for the maven project
   *             (1) - package to place the implicit classes
   */
  def main(args: Array[String]): Unit = {
    val basedir = args(0)
    val packageName = args(1)
    assert(basedir != null)
    assert(packageName != null)

    val folder = new File(basedir + "/src/main/resources")
    val resources = Some(folder).filter(_.isDirectory).map(findFormatFiles).getOrElse(Seq.empty).sortBy(_.getName)
    val sfts = resources.map(r => SimpleFeatureTypes.createType(ConfigFactory.parseFile(r)))

    if (sfts.isEmpty) {
      println("No formats found")
    } else {
      val classFilePath = s"$basedir/src/main/scala/${packageName.replaceAll("\\.", "/")}/$className.scala"
      val classFile = new File(classFilePath)
      println(s"Writing class file $packageName.$className with formats ${sfts.map(_.getTypeName).mkString(", ")}")
      val fw = new FileWriter(classFile)
      fw.write(buildAllClasses(sfts, packageName))
      fw.flush()
      fw.close()
    }
  }
}

/* Sample output

package com.foo

import org.opengis.feature.simple.SimpleFeature

object SimpleFeatureWrappers {

  implicit class mySft(sf: SimpleFeature) extends AnyVal {

    def foo(): java.lang.String = sf.getAttribute(0).asInstanceOf[java.lang.String]
    def fooOpt(): Option[java.lang.String] = Option(foo())
    def setFoo(x: java.lang.String): Unit = sf.setAttribute(0, x)

    def lat(): java.lang.Double = sf.getAttribute(1).asInstanceOf[java.lang.Double]
    def latOpt(): Option[java.lang.Double] = Option(lat())
    def setLat(x: java.lang.Double): Unit = sf.setAttribute(1, x)

    def lon(): java.lang.Double = sf.getAttribute(2).asInstanceOf[java.lang.Double]
    def lonOpt(): Option[java.lang.Double] = Option(lon())
    def setLon(x: java.lang.Double): Unit = sf.setAttribute(2, x)

    def geom(): org.locationtech.jts.geom.Point = sf.getAttribute(3).asInstanceOf[org.locationtech.jts.geom.Point]
    def geomOpt(): Option[org.locationtech.jts.geom.Point] = Option(geom())
    def setGeom(x: org.locationtech.jts.geom.Point): Unit = sf.setAttribute(3, x)
  }
}
*/

