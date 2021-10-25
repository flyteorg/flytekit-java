export REPOSITORY=flytekit-java

GIT_VERSION := $(shell git describe --always --tags)
GIT_HASH := $(shell git rev-parse --short HEAD)
TIMESTAMP := $(shell date '+%Y-%m-%d')

.PHONY: update_boilerplate
update_boilerplate:
	@curl https://raw.githubusercontent.com/flyteorg/boilerplate/master/boilerplate/update.sh -o boilerplate/update.sh
	@boilerplate/update.sh

# It will copy the latest proto from flyteidl to flytekit-java
.PHONY: update_flyteidl
update_flyteidl:
	@scripts/update_flyteidl.sh