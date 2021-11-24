#copying latest proto from flyteidl to flytekit-java

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

OUT="$(mktemp -d)"
trap 'rm -fr $OUT' EXIT


FLYTEIDL_VERSION=$(curl --silent "https://api.github.com/repos/flyteorg/flyteidl/releases/latest" | jq -r .tag_name)
git clone https://github.com/flyteorg/flyteidl.git "${OUT}" --branch "${FLYTEIDL_VERSION}"

for dir in "admin" "core" "event" "service"
do
    mv ${OUT}/protos/flyteidl/${dir}/*  flyteidl-protos/src/main/proto/flyteidl/${dir}/
done