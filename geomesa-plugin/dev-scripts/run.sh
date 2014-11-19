if [ "$1" = "--debug" ]; then
  GEO_DEBUG="jpda"
fi
pushd $CATALINA_HOME/bin
./catalina.sh jpda run
popd

