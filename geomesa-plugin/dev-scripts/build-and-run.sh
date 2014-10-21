pushd ../
mvn clean install -Dmaven.test.skip=true
if [ "$?" -eq 0 ]; then
  popd
  ./copy.sh $*
  ./run.sh $*
fi

