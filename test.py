#!/bin/bash
#Invoke tests here
set -x
sudo apt-get update
sudo apt-get install bc

kubectl get service,pod -o wide

echo ""
echo "-----------------------------------------"
echo " ENSURE the test service points to GREEN "
echo "-----------------------------------------"
echo ""
./scripts/kubectl_patch.sh astro-test-api 1.1

./scripts/ping_astro.sh -n ${TEST_URL} -t ${ASTRO_API_KEY} -a 3 -k

if [ "$?" -ne 0 ]; then
  echo "### GREEN TEST FAILED! ###"
  echo ""
  echo "---------------------------------"
  echo "State of the current deployments"
  echo "---------------------------------"
  kubectl get deployments
  echo ""
  echo "---------------------------------" 
  echo "kubectl get pod -o wide | grep astro"
  echo "---------------------------------"
  kubectl get ing,service,pod -o wide 
  rc=1
else
  echo "### TEST PASSED!! ###"
  rc=0
fi

kubectl get service -o wide

exit $rc

