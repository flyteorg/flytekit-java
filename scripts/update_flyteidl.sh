#!/usr/bin/env bash

# WARNING: THIS FILE IS MANAGED IN THE 'BOILERPLATE' REPO AND COPIED TO OTHER REPOSITORIES.
# ONLY EDIT THIS FILE FROM WITHIN THE 'FLYTEORG/BOILERPLATE' REPOSITORY:
# 
# TO OPT OUT OF UPDATES, SEE https://github.com/flyteorg/boilerplate/blob/master/Readme.rst

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

OUT="$(mktemp -d)"
trap 'rm -fr $OUT' EXIT


FLYTEIDL_VERSION=$(curl --silent "https://api.github.com/repos/flyteorg/flyteidl/releases/latest" | jq -r .tag_name)
git clone https://github.com/flyteorg/flyteidl.git "${OUT}" --branch "${FLYTEIDL_VERSION}"

mv ${OUT}/proto/flyteidl/admin  jflyte/src/main/proto/flyteidl
mv ${OUT}/proto/flyteidl/core  jflyte/src/main/proto/flyteidl
mv ${OUT}/proto/flyteidl/events  jflyte/src/main/proto/flyteidl
mv ${OUT}/proto/flyteidl/services  jflyte/src/main/proto/flyteidl