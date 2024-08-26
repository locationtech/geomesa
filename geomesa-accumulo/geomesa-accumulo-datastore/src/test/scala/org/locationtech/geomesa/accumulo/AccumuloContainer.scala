/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import com.typesafe.scalalogging.StrictLogging
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.{Authorizations, NamespacePermission, SystemPermission}
import org.geomesa.testcontainers.AccumuloContainer
import org.locationtech.geomesa.utils.io.WithClose
import org.testcontainers.utility.DockerImageName

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.Try

case object AccumuloContainer extends StrictLogging {

  val ImageName =
    DockerImageName.parse("ghcr.io/geomesa/accumulo-uno")
        .withTag(sys.props.getOrElse("accumulo.docker.tag", "2.1.3"))

  val Namespace = "gm"

  lazy val instanceName = Container.getInstanceName
  lazy val zookeepers = Container.getZookeepers
  lazy val user = Container.getUsername
  lazy val password = Container.getPassword

  lazy val Container: AccumuloContainer = {
    val container = tryContainer.get
    WithClose(container.client()) { client =>
      val secOps = client.securityOperations()
      secOps.changeUserAuthorizations(Users.root.name, Users.root.auths)
      Seq(Users.admin, Users.user).foreach { case UserWithAuths(name, password, auths) =>
        secOps.createLocalUser(name, new PasswordToken(password))
        SystemPermissions.foreach(p => secOps.grantSystemPermission(name, p))
        client.securityOperations().changeUserAuthorizations(name, auths)
      }
    }
    container
  }

  private lazy val tryContainer: Try[AccumuloContainer] = Try {
    logger.info("Starting Accumulo container")
    val container = new AccumuloContainer(ImageName).withGeoMesaDistributedRuntime()
    initialized.getAndSet(true)
    container.start()
    logger.info("Started Accumulo container")
    container
  }

  private val initialized = new AtomicBoolean(false)

  sys.addShutdownHook({
    if (initialized.get) {
      logger.info("Stopping Accumulo container")
      tryContainer.foreach(_.stop())
      logger.info("Stopped Accumulo container")
    }
  })

  case class UserWithAuths(name: String, password: String, auths: Authorizations)

  object Users {
    val root  = UserWithAuths("root", "secret", new Authorizations("admin", "user", "system"))
    val admin = UserWithAuths("admin", "secret", new Authorizations("admin", "user"))
    val user  = UserWithAuths("user", "secret", new Authorizations("user"))
  }

  private val SystemPermissions = Seq(
    SystemPermission.CREATE_NAMESPACE,
    SystemPermission.ALTER_NAMESPACE,
    SystemPermission.DROP_NAMESPACE,
    SystemPermission.CREATE_TABLE,
    SystemPermission.ALTER_TABLE,
    SystemPermission.DROP_TABLE,
  )
}
