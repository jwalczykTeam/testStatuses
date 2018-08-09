#!/usr/bin/env bash

image_tag="1.0.0"
bluemix_docker_registry=$(cf ic info | grep -i 'registry host' | tr -s ' ' | cut -f 3 -d ' ')
namespace=$(cf ic namespace get)
github_ibm_token="319922559f31d9bc589a00befcf7eba18033bf55"

image_name="$bluemix_docker_registry/$namespace/astro:$image_tag"

echo "Image name.........: $image_name"


if [ ! -d "./astro" ]; then
  echo git clone "https://$github_ibm_token@github.ibm.com/ibmdevopsadtech/astro.git"
fi

cd astro

cf ic build -t $image_name .
