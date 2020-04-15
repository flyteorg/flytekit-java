FLYTEIDL_VERSION="0.17.7"

out=src/main/proto

mkdir -p src/main/proto

curl -L "https://github.com/lyft/flyteidl/archive/v${FLYTEIDL_VERSION}.tar.gz" | \
  tar xvf - \
    --strip-components=2 \
    -C "$out/" \
    "flyteidl-${FLYTEIDL_VERSION}/protos/flyteidl/admin/*.proto" \
    "flyteidl-${FLYTEIDL_VERSION}/protos/flyteidl/core/*.proto" \
    "flyteidl-${FLYTEIDL_VERSION}/protos/flyteidl/event/*.proto" \
    "flyteidl-${FLYTEIDL_VERSION}/protos/flyteidl/service/admin.proto"

# remove trailing whitespace for better OCR
find "$out" -type f -exec sed -i 's/ *$//' '{}' ';'

# FIXME manually stripped grpc-gateway annotations from service/admin.proto
#
# it was easier to strip because annotations.proto file is licensed under BSD-3
# and requires copyright notice so we don't add it for now

