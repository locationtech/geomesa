/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo

import java.io.{File, FileWriter}
import java.nio.file.Files

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.{Authorizations, NamespacePermission, SystemPermission}
import org.apache.accumulo.minicluster.{MiniAccumuloCluster, MiniAccumuloConfig}
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}

case object MiniCluster extends LazyLogging {

  private val miniClusterTempDir = Files.createTempDirectory("gm-mini-acc-")

  private val systemPermissions = Seq(
    SystemPermission.CREATE_NAMESPACE,
    SystemPermission.ALTER_NAMESPACE,
    SystemPermission.DROP_NAMESPACE
  )

  private val namespacePermissions = Seq(
    NamespacePermission.READ,
    NamespacePermission.WRITE,
    NamespacePermission.CREATE_TABLE,
    NamespacePermission.ALTER_TABLE,
    NamespacePermission.DROP_TABLE
  )

  val namespace = "gm"

  lazy val cluster: MiniAccumuloCluster = {
    logger.info(s"Starting Accumulo minicluster at $miniClusterTempDir")
    val config = new MiniAccumuloConfig(miniClusterTempDir.toFile, Users.root.password)
    sys.props.get("geomesa.accumulo.test.tablet.servers").map(_.toInt).foreach(config.setNumTservers)
    val cluster = new MiniAccumuloCluster(config)
    // required for zookeeper 3.5
    WithClose(new FileWriter(new File(miniClusterTempDir.toFile, "conf/zoo.cfg"), true)) { writer =>
      writer.write("admin.enableServer=false\n") // disable the admin server, which tries to bind to 8080
      writer.write("4lw.commands.whitelist=*\n") // enable 'ruok', which the minicluster uses to check zk status
    }
    cluster.start()

    // set up users and authorizations
    val connector = cluster.getConnector(Users.root.name, Users.root.password)
    connector.namespaceOperations().create(namespace)
    Seq(Users.root, Users.admin, Users.user).foreach { case UserWithAuths(name, password, auths) =>
      if (name != Users.root.name) {
        connector.securityOperations().createLocalUser(name, new PasswordToken(password))
        systemPermissions.foreach(p => connector.securityOperations().grantSystemPermission(name, p))
        namespacePermissions.foreach(p => connector.securityOperations().grantNamespacePermission(name, namespace, p))
      }
      connector.securityOperations().changeUserAuthorizations(name, auths)
    }

    AccumuloVersion.close(connector)

    logger.info("Started Accmulo minicluster")

    cluster
  }

  sys.addShutdownHook({
    logger.info("Stopping Accumulo minicluster")
    try { cluster.stop() } finally {
      PathUtils.deleteRecursively(miniClusterTempDir)
    }
    logger.info("Stopped Accumulo minicluster")
  })

  case class UserWithAuths(name: String, password: String, auths: Authorizations)

  object Users {
    val root  = UserWithAuths("root", "secret", new Authorizations("admin", "user", "system"))
    val admin = UserWithAuths("admin", "secret", new Authorizations("admin", "user"))
    val user  = UserWithAuths("user", "secret", new Authorizations("user"))
  }
}
