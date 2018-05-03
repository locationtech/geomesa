#!/usr/bin/env bash
#
# Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License, Version 2.0 which
# accompanies this distribution and is available at
# http://www.opensource.org/licenses/apache2.0.php.
#
# Bootstrap Script to install a GeoMesa Jupyter notebook
#

projectVersion="%%project.version%%"
scalaBinVersion="%%scala.binary.version%%"

if [[ -z "${%%gmtools.dist.name%%_HOME}" ]]; then
  %%gmtools.dist.name%%_HOME="$(cd "`dirname "$0"`"/../..; pwd)"
fi
GM_TOOLS_HOME="${%%gmtools.dist.name%%_HOME}"

function log() {
  timeStamp=$(date +"%T")
  echo "${timeStamp}| ${@}" | tee -a /tmp/bootstrap.log
}

sudo ${GM_TOOLS_HOME}/bin/aws-utils/aws-bootstrap-geomesa-python.sh

# We need to install jupyter get ipython in zeppelin through the %python.ipython magic
sudo python36 -m pip install pandas jupyter grpcio folium matplotlib

# Prepare runtime (gm-hbase bootstrap may not have run)
runtimeJar="geomesa-hbase-spark-runtime_${scalaBinVersion}-${projectVersion}.jar"
linkFile="/opt/geomesa/dist/spark/geomesa-hbase-spark-runtime.jar"
[[ ! -h $linkFile ]] && sudo ln -s $runtimeJar $linkFile

# Shutdown notebook server so we can modify configuration
sudo stop zeppelin

sudo rm -r /var/lib/zeppelin/notebook/*
sudo cp -r /opt/geomesa/examples/zeppelin/notebook/* /var/lib/zeppelin/notebook/
sudo chown-R zeppelin:zeppelin /var/lib/zeppelin/notebook/

notebookConf="/etc/zeppelin/conf/interpreter.json"

sudo rm -f ${notebookConf}
sudo cat > ${notebookConf} <<EOF
{
  "interpreterSettings": {
    "2ANGGHHMQ": {
      "id": "2ANGGHHMQ",
      "name": "spark",
      "group": "spark",
      "properties": {
        "zeppelin.spark.printREPLOutput": "true",
        "spark.yarn.jar": "",
        "master": "yarn-client",
        "spark.dynamicAllocation.enabled\t": "true",
        "zeppelin.spark.maxResult": "1000",
        "zeppelin.dep.localrepo": "/usr/lib/zeppelin/local-repo",
        "spark.shuffle.service.enabled\t": "true",
        "spark.app.name": "Zeppelin",
        "zeppelin.spark.importImplicit": "true",
        "zeppelin.spark.useHiveContext": "false",
        "args": "",
        "spark.home": "/usr/lib/spark",
        "zeppelin.spark.concurrentSQL": "false",
        "zeppelin.pyspark.python": "python36"
      },
      "status": "DOWNLOADING_DEPENDENCIES",
      "interpreterGroup": [
        {
          "name": "spark",
          "class": "org.apache.zeppelin.spark.SparkInterpreter",
          "defaultInterpreter": false,
          "editor": {
            "language": "scala",
            "editOnDblClick": false
          }
        },
        {
          "name": "pyspark",
          "class": "org.apache.zeppelin.spark.PySparkInterpreter",
          "defaultInterpreter": false,
          "editor": {
            "language": "python",
            "editOnDblClick": false
          }
        },
        {
          "name": "sql",
          "class": "org.apache.zeppelin.spark.SparkSqlInterpreter",
          "defaultInterpreter": false,
          "editor": {
            "language": "sql",
            "editOnDblClick": false
          }
        }
      ],
      "dependencies": [
        {
          "groupArtifactVersion": "/etc/hbase/conf/hbase-site.xml",
          "local": false
        },
        {
          "groupArtifactVersion": "/etc/hadoop/conf/core-site.xml",
          "local": false,
          "exclusions": []
        },
        {
          "groupArtifactVersion": "${linkFile}",
          "local": false,
          "exclusions": []
        }
      ],
      "option": {
        "remote": true,
        "port": -1,
        "perNote": "isolated",
        "perUser": "",
        "isExistingProcess": false,
        "setPermission": false,
        "users": [],
        "isUserImpersonate": false
      }
    },
    "2AM1YV5CU": {
      "id": "2AM1YV5CU",
      "name": "angular",
      "group": "angular",
      "properties": {},
      "status": "READY",
      "interpreterGroup": [
        {
          "name": "angular",
          "class": "org.apache.zeppelin.angular.AngularInterpreter",
          "defaultInterpreter": false,
          "editor": {
            "editOnDblClick": true
          }
        }
      ],
      "dependencies": [],
      "option": {
        "remote": true,
        "port": -1,
        "perNote": "shared",
        "perUser": "shared",
        "isExistingProcess": false,
        "setPermission": false,
        "isUserImpersonate": false
      }
    },
    "2BRWU4WXC": {
      "id": "2BRWU4WXC",
      "name": "python",
      "group": "python",
      "properties": {
        "zeppelin.python": "python36",
        "zeppelin.python.maxResult": "1000"
      },
      "status": "READY",
      "interpreterGroup": [
        {
          "name": "python",
          "class": "org.apache.zeppelin.python.PythonInterpreter",
          "defaultInterpreter": false,
          "editor": {
            "language": "python",
            "editOnDblClick": false
          }
        }
      ],
      "dependencies": [],
      "option": {
        "remote": true,
        "port": -1,
        "perNote": "shared",
        "perUser": "shared",
        "isExistingProcess": false,
        "setPermission": false,
        "isUserImpersonate": false
      }
    },
    "2AJXGMUUJ": {
      "id": "2AJXGMUUJ",
      "name": "md",
      "group": "md",
      "properties": {},
      "status": "READY",
      "interpreterGroup": [
        {
          "name": "md",
          "class": "org.apache.zeppelin.markdown.Markdown",
          "defaultInterpreter": false,
          "editor": {
            "language": "markdown",
            "editOnDblClick": true
          }
        }
      ],
      "dependencies": [],
      "option": {
        "remote": true,
        "port": -1,
        "perNote": "shared",
        "perUser": "shared",
        "isExistingProcess": false,
        "setPermission": false,
        "isUserImpersonate": false
      }
    },
    "2AKK3QQXU": {
      "id": "2AKK3QQXU",
      "name": "sh",
      "group": "sh",
      "properties": {
        "shell.command.timeout.millisecs": "60000"
      },
      "status": "READY",
      "interpreterGroup": [
        {
          "name": "sh",
          "class": "org.apache.zeppelin.shell.ShellInterpreter",
          "defaultInterpreter": false,
          "editor": {
            "language": "sh",
            "editOnDblClick": false
          }
        }
      ],
      "dependencies": [],
      "option": {
        "remote": true,
        "port": -1,
        "perNote": "shared",
        "perUser": "shared",
        "isExistingProcess": false,
        "setPermission": false,
        "isUserImpersonate": false
      }
    }
  },
  "interpreterBindings": {
    "2A94M5J1Z": [
      "2ANGGHHMQ",
      "2AJXGMUUJ",
      "2AM1YV5CU",
      "2AKK3QQXU",
      "2BRWU4WXC"
    ]
  },
  "interpreterRepositories": [
    {
      "id": "central",
      "type": "default",
      "url": "http://repo1.maven.org/maven2/",
      "releasePolicy": {
        "enabled": true,
        "updatePolicy": "daily",
        "checksumPolicy": "warn"
      },
      "snapshotPolicy": {
        "enabled": true,
        "updatePolicy": "daily",
        "checksumPolicy": "warn"
      },
      "mirroredRepositories": [],
      "repositoryManager": false
    },
    {
      "id": "local",
      "type": "default",
      "url": "file:///var/lib/zeppelin/.m2/repository",
      "releasePolicy": {
        "enabled": true,
        "updatePolicy": "daily",
        "checksumPolicy": "warn"
      },
      "snapshotPolicy": {
        "enabled": true,
        "updatePolicy": "daily",
        "checksumPolicy": "warn"
      },
      "mirroredRepositories": [],
      "repositoryManager": false
    }
  ]
}
EOF

zeppelin_env="/etc/zeppelin/conf/zeppelin-env.sh"
notebookDir="${GM_TOOLS_HOME}/examples/zeppelin/notebook"
sudo bash -c "echo '' >> ${zeppelin_env}"
sudo bash -c "echo 'export ZEPPELIN_NOTEBOOK_DIR=${notebookDir}' >> ${zeppelin_env}"
sudo chown -R zeppelin:zeppelin $notebookDir

sudo chown -R zeppelin:zeppelin /etc/zeppelin/conf/*
sudo chmod -R 777 ${GM_TOOLS_HOME}/logs/
sudo start zeppelin
