/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import java.io.IOException
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.util.Base64

import com.google.gson._
import javax.net.ssl.{HttpsURLConnection, SSLSocketFactory}
import org.apache.avro.Schema
import org.apache.commons.io.IOUtils
import org.locationtech.geomesa.utils.io.WithClose

import scala.util.{Failure, Success, Try}

class SchemaRegistryClient(val url: URL,
                           sslSocketFactory: Option[SSLSocketFactory] = None,
                           authentication: Option[String] = None) {

  private val parser = new Schema.Parser()
  private val gson = new GsonBuilder().create()

  /**
    * Register an avro schema with confluent. Note that this may fail if an incompatible schema has previously
    * been registered, depending on the confluent compatibility settings
    *
    * @param subject name to register the schema under
    * @param schema schema to register
    */
  def registerSchema(subject: String, schema: Schema): Unit = {
    val latest = Option(request(s"subjects/$subject/versions/latest", "GET", None)).map(parser.parse)
    if (!latest.contains(schema)) {
      val json = new JsonObject()
      json.add("schema", new JsonPrimitive(schema.toString()))
      request(s"subjects/$subject/versions", "POST", Some(gson.toJson(json).getBytes(StandardCharsets.UTF_8)))
      // response format: """{ "id" : <id> }"""
    }
  }

  /**
    * Make an HTTP request to the confluent server.
    *
    * Portions taken from io.confluent.kafka.schemaregistry.client.rest.RestService, copyright 2015 Confluent Inc.
    *
    * @param path relative request path
    * @param method POST, GET, PUT, etc
    * @param body request body, if any
    * @return response
    */
  private def request(path: String, method: String, body: Option[Array[Byte]]): String = {
    val connection = new URL(url, path).openConnection.asInstanceOf[HttpURLConnection]
    try {
      for { f <- sslSocketFactory; c <- Option(connection).collect { case c: HttpsURLConnection => c } } {
        c.setSSLSocketFactory(f)
      }
      authentication.foreach { auth =>
        val header = Base64.getEncoder.encodeToString(auth.getBytes(StandardCharsets.UTF_8))
        connection.setRequestProperty("Authorization", s"Basic $header")
      }
      connection.setRequestProperty("Content-Type", SchemaRegistryClient.ContentType)
      connection.setRequestMethod(method)

      // connection.getResponseCode() implicitly calls getInputStream, so always set to true.
      // On the other hand, leaving this out breaks nothing.
      connection.setDoInput(true)
      connection.setUseCaches(false)

      body.foreach { data =>
        connection.setDoOutput(true)
        WithClose(connection.getOutputStream) { out =>
          out.write(data)
          out.flush()
        }
      }

      val response = connection.getResponseCode
      if (response == HttpURLConnection.HTTP_OK) {
        WithClose(connection.getInputStream)(IOUtils.toString(_, StandardCharsets.UTF_8))
      } else if (response == HttpURLConnection.HTTP_NO_CONTENT || response == HttpURLConnection.HTTP_NOT_FOUND) {
        null
      } else {
        val message = Try(WithClose(connection.getErrorStream)(IOUtils.toString(_, StandardCharsets.UTF_8))) match {
          case Success(m) => m
          case Failure(e) => s"Error calling endpoint: $response, couldn't read error message ($e)"
        }
        throw new IOException(message)
      }
    } finally {
      connection.disconnect()
    }
  }
}

object SchemaRegistryClient {
  val ContentType = "application/vnd.schemaregistry.v1+json"
}
