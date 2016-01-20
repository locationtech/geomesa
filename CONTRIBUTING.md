Contributing to GeoMesa
========================

Project Description
-------------------

GeoMesa is a suite of tools for working with big geo-spatial data in a distributed fashion.
GeoMesa's main focus is a GeoTools DataStore implementation backed by Apache Accumulo, but it also
supports streaming data with Apache Kafka, distributed processing with Apache Spark, tools for
converting and ingesting data, and much more.

- http://www.geomesa.org/

Contributor License Agreement
-----------------------------

Before your contribution can be accepted by the project, you need to create and 
electronically sign the Eclipse Foundation Contributor License Agreement (CLA).

- http://www.eclipse.org/legal/CLA.php

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

GeoMesa requires Maven 3.2.2 or later to build:

```
geomesa> mvn clean install
```

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

* An initial pull request should consist of a single commit, rebased on top of the current master branch.
  * Additional commits can be added in response to code review comments.
* The commit message must consist of a JIRA ticket number followed by a short description, with additional
  information in bullets below as required.
  * See e.g. https://github.com/locationtech/geomesa/commit/1f345132a717816d5a4951f73b2b73537fce305b
* The commit must be signed-off, which indicates that you are taking responsibility for all code contained
  in the commit. This can be done with the `git commit -s` flag.
* Code must be reasonably formatted. Scala does not conform well to automatic formatting, but in general
  GeoMesa tries to adhere to the official Scala style guide: http://docs.scala-lang.org/style/
* Code must contain an appropriate license header. GeoMesa is licensed under Apache 2.
* Code should include unit tests when appropriate.

Contact
-------

Contact the GeoMesa developers via the developers mailing list:

* https://www.locationtech.org/mailman/listinfo/geomesa-dev

For user information, use the users mailing list:

* https://www.locationtech.org/mailman/listinfo/geomesa-users
