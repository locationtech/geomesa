Contributing to GeoMesa
========================

Project Description
-------------------

GeoMesa is an open source suite of tools that enables large-scale geospatial querying and analytics on distributed
computing systems. GeoMesa provides spatio-temporal indexing on top of the Accumulo, HBase and
Cassandra databases for massive storage of point, line, and polygon data. GeoMesa also provides near real time
stream processing of spatio-temporal data by layering spatial semantics on top of Apache Kafka. Through GeoServer,
GeoMesa facilitates integration with a wide range of existing mapping clients over standard OGC (Open Geospatial
Consortium) APIs and protocols such as WFS and WMS. GeoMesa supports Apache Spark for custom distributed
geospatial analytics.

- https://www.geomesa.org/

Eclipse Contributor Agreement
-----------------------------

Before your contribution can be accepted by the project, you need to create an Eclipse Foundation 
account and electronically sign the Eclipse Contributor Agreement (ECA).

- https://www.eclipse.org/legal/ECA.php 

Developer Resources
-------------------

GeoMesa code is hosted on GitHub, and the project is hosted at locationtech:

* https://github.com/locationtech/geomesa
* https://locationtech.org/projects/technology.geomesa

Issue Tracking
--------------

GeoMesa uses JIRA to track ongoing development and issues:

* https://geomesa.atlassian.net/issues/?jql=

Building
--------

See the instructions in the main README.

Contributing
------------

GeoMesa uses git pull requests for contributions. To create a pull request, follow these steps:

* Contributions to GeoMesa must reference a valid JIRA ticket. Contact the GeoMesa developers list
  about opening a ticket, or to have a ticket assigned to you.
* Fork the GeoMesa project on GitHub - go to https://github.com/locationtech/geomesa and click 'Fork'.
* Create a branch on your forked project that contains your work. See 'Coding Standards', below.
* Use GitHub to open a pull request against the locationtech GeoMesa repository - from your branch on
  GitHub, click 'New Pull Request'.
* Respond to comments on your pull request as they are made.
* When ready, your pull request will be merged by an official GeoMesa contributor.

Coding Standards
----------------

* An initial pull request should consist of a single commit, rebased on top of the current main branch.
  * Additional commits can be added in response to code review comments.
* The commit message must consist of a JIRA ticket number followed by a short description, with additional
  information in bullets below as required.
  * See e.g. https://github.com/locationtech/geomesa/commit/1f345132a717816d5a4951f73b2b73537fce305b
* Code must be reasonably formatted. Scala does not conform well to automatic formatting, but in general
  GeoMesa tries to adhere to the official Scala style guide: https://docs.scala-lang.org/style/
* Code should include unit tests when appropriate.

License and Copyright
---------------------

GeoMesa is provided under the Apache 2 license, and any contributions must maintain this. To ensure proper
licensing, source files must contain an appropriate license header. When a file is created or modified,
the contributor should also indicate their copyright in the header. If copyright is not desired, the contributor
may delegate the copyright to CCRi, as per the default header.

GeoMesa uses the [License Maven Plugin](https://code.mycila.com/license-maven-plugin/) to help manage copyright
headers. This plugin runs as part of the default build, and will fail if any files do not contain a valid header.
To add a new copyright owner, a template file can be placed under `build/copyright/` and added to the
`<validHeaders>` block in the root pom.xml.

Contact
-------

Contact the GeoMesa developers via the developers mailing list:

* https://accounts.eclipse.org/mailing-list/geomesa-dev

For user information, use the users mailing list:

* https://accounts.eclipse.org/mailing-list/geomesa-users
