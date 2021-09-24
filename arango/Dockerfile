FROM arangodb:latest

RUN apk add bash
CMD bash -c "set -a && source /run/secrets/arango_secrets && env && arangod"
