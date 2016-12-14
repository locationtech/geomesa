#!/usr/bin/env bash
#
# Installing additional components on an EMR node depends on several config files
# controlled by the EMR framework which may affect the is_master 
# functions at some point in the future. I've grouped each unit of work into a function 
# with a descriptive name to help with understanding and maintainability
#

# You can change these but there is probably no need
ACCUMULO_INSTANCE=accumulo
ACCUMULO_HOME="${INSTALL_DIR}/accumulo"
MASTER_DNSNAME=
INTIAL_POLLING_INTERVAL=15 # This gets doubled for each attempt up to max_attempts
HDFS_USER=hdfs

# Parses a configuration file put in place by EMR to determine the role of this node
is_master() {
  if [ $(jq '.isMaster' /mnt/var/lib/info/instance.json) = 'true' ]; then
    return 0
  else
    return 1
  fi
}

# Avoid race conditions and actually poll for availability of component dependencies
# Credit: http://stackoverflow.com/questions/8350942/how-to-re-run-the-curl-command-automatically-when-the-error-occurs/8351489#8351489
with_backoff() {
  local max_attempts=${ATTEMPTS-5}
  local timeout=${INTIAL_POLLING_INTERVAL-1}
  local attempt=0
  local exitCode=0

  while (( $attempt < $max_attempts ))
  do
    set +e
    "$@"
    exitCode=$?
    set -e

    if [[ $exitCode == 0 ]]
    then
      break
    fi

    echo "Retrying $@ in $timeout.." 1>&2
    sleep $timeout
    attempt=$(( attempt + 1 ))
    timeout=$(( timeout * 2 ))
  done

  if [[ $exitCode != 0 ]]
  then
    echo "Fail: $@ failed to complete after $max_attempts attempts" 1>&2
  fi

  return $exitCode
}


# Settings recommended for Accumulo
os_tweaks() {
	echo -e "net.ipv6.conf.all.disable_ipv6 = 1" | sudo tee --append /etc/sysctl.conf
	echo -e "net.ipv6.conf.default.disable_ipv6 = 1" | sudo tee --append /etc/sysctl.conf
	echo -e "net.ipv6.conf.lo.disable_ipv6 = 1" | sudo tee --append /etc/sysctl.conf
	echo -e "vm.swappiness = 0" | sudo tee --append /etc/sysctl.conf
	sudo sysctl -w vm.swappiness=0
	echo -e "" | sudo tee --append /etc/security/limits.conf
	echo -e "*\t\tsoft\tnofile\t65536" | sudo tee --append /etc/security/limits.conf
	echo -e "*\t\thard\tnofile\t65536" | sudo tee --append /etc/security/limits.conf
}

create_accumulo_user() {
 	id $USER
	if [ $? != 0 ]; then
		sudo adduser $USER
		sudo sh -c "echo '$USERPW' | passwd $USER --stdin"
	fi
}

install_accumulo() {
	with_backoff hdfs dfsadmin -safemode wait
	ARCHIVE_FILE="accumulo-${ACCUMULO_VERSION}-bin.tar.gz"
	LOCAL_ARCHIVE="${INSTALL_DIR}/${ARCHIVE_FILE}"
	sudo sh -c "curl '${ACCUMULO_DOWNLOAD_BASE_URL}/${ACCUMULO_VERSION}/${ARCHIVE_FILE}' > $LOCAL_ARCHIVE"
	sudo sh -c "tar xzf $LOCAL_ARCHIVE -C $INSTALL_DIR"
	sudo rm -f $LOCAL_ARCHIVE
	sudo ln -s "${INSTALL_DIR}/accumulo-${ACCUMULO_VERSION}" "${INSTALL_DIR}/accumulo"
	sudo chown -R accumulo:accumulo "${INSTALL_DIR}/accumulo-${ACCUMULO_VERSION}"
	sudo sh -c "echo 'export PATH=$PATH:${INSTALL_DIR}/accumulo/bin' > /etc/profile.d/accumulo.sh"
}

configure_accumulo() {
	# Requires zookeeper bootstrapped by EMR along with hadoop
	MASTER_DNSNAME=$(hdfs getconf -confKey yarn.resourcemanager.hostname)
	sudo cp $INSTALL_DIR/accumulo/conf/examples/${ACCUMULO_TSERVER_OPTS}/native-standalone/* $INSTALL_DIR/accumulo/conf/
	sudo sed -i "s/<value>localhost:2181<\/value>/<value>${MASTER_DNSNAME}:2181<\/value>/" $INSTALL_DIR/accumulo/conf/accumulo-site.xml
	sudo sed -i '/HDP 2.0 requirements/d' $INSTALL_DIR/accumulo/conf/accumulo-site.xml
	sudo sed -i "s/\${LOG4J_JAR}/\${LOG4J_JAR}:\/usr\/lib\/hadoop\/lib\/*:\/usr\/lib\/hadoop\/client\/*/" $INSTALL_DIR/accumulo/bin/accumulo

	# Crazy escaping to get this shell to fill in values but root to write out the file
	ENV_FILE="export ACCUMULO_HOME=$INSTALL_DIR/accumulo; export HADOOP_HOME=/usr/lib/hadoop; export ACCUMULO_LOG_DIR=$INSTALL_DIR/accumulo/logs; export JAVA_HOME=/usr/lib/jvm/java; export ZOOKEEPER_HOME=/usr/lib/zookeeper; export HADOOP_PREFIX=/usr/lib/hadoop; export HADOOP_CONF_DIR=/etc/hadoop/conf"
	echo $ENV_FILE > /tmp/acc_env
	sudo sh -c "cat /tmp/acc_env > $INSTALL_DIR/accumulo/conf/accumulo-env.sh"
	source $INSTALL_DIR/accumulo/conf/accumulo-env.sh
	sudo -u $USER $INSTALL_DIR/accumulo/bin/build_native_library.sh

	if is_master ; then
		sudo -u $HDFS_USER hadoop fs -chmod 777 /user # This is more for Spark than Accumulo but put here for expediency
		sudo -u $HDFS_USER hadoop fs -mkdir /accumulo
		sudo -u $HDFS_USER hadoop fs -chown accumulo:accumulo /accumulo
		sudo sh -c "hostname > $INSTALL_DIR/accumulo/conf/monitor"
		sudo sh -c "hostname > $INSTALL_DIR/accumulo/conf/gc"
		sudo sh -c "hostname > $INSTALL_DIR/accumulo/conf/tracers"
		sudo sh -c "hostname > $INSTALL_DIR/accumulo/conf/masters"
		sudo sh -c "echo > $INSTALL_DIR/accumulo/conf/slaves"
		sudo -u $USER $INSTALL_DIR/accumulo/bin/accumulo init --clear-instance-name --instance-name $ACCUMULO_INSTANCE --password $USERPW
	else
		sudo sh -c "echo $MASTER_DNSNAME > $INSTALL_DIR/accumulo/conf/monitor"
		sudo sh -c "echo $MASTER_DNSNAME > $INSTALL_DIR/accumulo/conf/gc"
		sudo sh -c "echo $MASTER_DNSNAME > $INSTALL_DIR/accumulo/conf/tracers"
		sudo sh -c "echo $MASTER_DNSNAME > $INSTALL_DIR/accumulo/conf/masters"
		sudo sh -c "hostname > $INSTALL_DIR/accumulo/conf/slaves"
	fi

	sudo chown -R $USER:$USER $INSTALL_DIR/accumulo

	# EMR starts worker instances first so there will be timing issues
	# Test to ensure it's safe to continue before attempting to start things up
	if is_master ; then
		with_backoff is_accumulo_initialized
	else
		with_backoff is_accumulo_available
	fi

	sudo -u $USER $INSTALL_DIR/accumulo/bin/start-here.sh
}

is_accumulo_initialized() {
	hadoop fs -ls /accumulo/instance_id
	return $?
}

is_accumulo_available() {
	$INSTALL_DIR/accumulo/bin/accumulo info
	return $?
}

write_accumulo_namespace_script(){
# Create a script for accumulo to config namespace goodies.
GM_NAMESPACE="geomesa_"
GM_NAMESPACE="${GM_NAMESPACE}${GEOMESA_VERSION}"
# Remove periods 
GM_NAMESPACE=${GM_NAMESPACE//./}
namenode=$(hdfs getconf -confKey fs.defaultFS)

cat <<EOF >>/tmp/config-namespace
createnamespace ${GM_NAMESPACE}
grant NameSpace.CREATE_TABLE -ns ${GM_NAMESPACE} -u root
config -s general.vfs.context.classpath.${GM_NAMESPACE}=${namenode}/accumulo/classpath/geomesa/${GEOMESA_VERSION}/[^.].*.jar 
config -ns ${GM_NAMESPACE} -s table.classpath.context=${GM_NAMESPACE} 
bye
EOF
}

install_geomesa(){
	echo " ==== INSTALL GEOMESA ==== "
	pushd /tmp
	if [ ! -z "${GEOMESA_DIST_S3}" ]; then
		aws s3 cp ${GEOMESA_DIST_S3} . 
	else 
		wget ${GEOMESA_DIST_LT}
	fi

	tar xvfz ${GEOMESA_TARBALL} 
	tar xvfz geomesa-${GEOMESA_VERSION}/dist/tools/geomesa-tools-${GEOMESA_VERSION}-bin.tar.gz -C /home/hadoop/
	export GEOMESA_HOME="/home/hadoop/geomesa-tools-${GEOMESA_VERSION}"
	echo "export GEOMESA_HOME='/home/hadoop/geomesa-tools-${GEOMESA_VERSION}'" >> /home/hadoop/.bashrc
	echo 'export PATH=${GEOMESA_HOME}/bin:$PATH' >> /home/hadoop/.bashrc
	# java, hadoop, zoo, and accumulo homes from accumulo install
	cat /tmp/acc_env >> /home/hadoop/.bashrc

	# set up accumulo namespace
	echo Deploying geomesa accumulo runtime on hdfs for namespace ${GM_NAMESPACE}
	GM_ACCUMULO_JAR="geomesa-"${GEOMESA_VERSION}"/dist/accumulo/geomesa-accumulo-distributed-runtime-"${GEOMESA_VERSION}".jar"
	sudo chown $HDFS_USER:$HDFS_USER ${GM_ACCUMULO_JAR}
	sudo -u $HDFS_USER hadoop fs -mkdir -p /accumulo/classpath/geomesa/${GEOMESA_VERSION}
	sudo -u $HDFS_USER hadoop fs -put ${GM_ACCUMULO_JAR} /accumulo/classpath/geomesa/${GEOMESA_VERSION}/

	write_accumulo_namespace_script
	${INSTALL_DIR}/accumulo/bin/accumulo shell -u root -p ${USERPW} -f /tmp/config-namespace
	popd
	#install shapefile processing libs using geomesa tools scripts
	/home/hadoop/geomesa-tools-${GEOMESA_VERSION}/bin/install-jai > /dev/null < <(echo y)
	/home/hadoop/geomesa-tools-${GEOMESA_VERSION}/bin/install-jline > /dev/null < <(echo y)
	/home/hadoop/geomesa-tools-${GEOMESA_VERSION}/bin/install-imageio-ext > /dev/null < <(echo y)
	
}
