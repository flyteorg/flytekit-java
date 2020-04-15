# flytekit-java


## How to run examples

Create `.env.local` with:

```
FLYTE_PLATFORM_URL=flyte.local:81
FLYTE_STAGING_LOCATION=gs://yourbucket
FLYTE_PLATFORM_INSECURE=True
```

Package and run:

```
$ mvn package -Ddocker.image=<image-name>
$ scripts/jflyte register workflows \
  -d=development \
  -p=flytetester \
  -v=$(git describe --always) \
  -cp=flytekit-examples/target/lib
```
