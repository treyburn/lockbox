# Lockbox

[![codecov](https://codecov.io/gh/treyburn/lockbox/graph/badge.svg?token=2ALGABLQ80)](https://codecov.io/gh/treyburn/lockbox)

A simple, distributed key-value store.

## Usage
Stand up the service using:

```sh
docker compose up -d api
```

Then you can hit it with curl:
```sh
# add a key
curl -X PUT -d 'testing' https://localhost:443/v1/abc --insecure

# get the value for a key
curl -X GET https://localhost:443/v1/abc --insecure

# delete a key
curl -X DELETE https://localhost:443/v1/abc --insecure
```

## API
### http
### grpc

## Setup

## TODO
Various items for cleanup.

### Project
- [ ] Improve README
  - [x] Create a TODO section
  - [ ] Provide a thorough project description
  - [ ] Provide useful code snippets for cli execution
  - [ ] Document the API(s)
  - [ ] Document prerequisites and general dev setup
- [X] Set up CI
  - [X] Linting w/ golangci-lint
  - [x] Testing
  - [X] Report test coverage in PR and on project README
  - [x] Build docker container
- [ ] Bring in Just for encapsulating typical project commands

### Service
- [x] Update Go + Deps
- [ ] Utilize multi-stage docker build
- [ ] Swap over to Postgres by default
- [ ] Add in 
- [ ] Improve testing
- [ ] Refactor to use standard Go project layouts
- [ ] Drop gorilla/mux for chi (https://github.com/go-chi/chi)
- [ ] Drop lib/pq for pgx (https://github.com/jackc/pgx)
- [ ] Improve logging and utilize context logging
- [ ] Swap over to sqlc instead of raw SQL (https://github.com/sqlc-dev/sqlc)
- [ ] Create an OpenAPI specification w/ validation
  - [ ] Use codegen tooling to create your Chi router and service (https://github.com/oapi-codegen/oapi-codegen)
- [ ] Add a gRPC API
- [ ] Instrument for OpenTelemetry
  - [ ] Utilize decorator patterns
  - [ ] Utilize telemetry middleware
- [ ] Add healthz endpoints for service health checking

### Deployment
- [ ] Generate a valid cert via LetsEncrypt
- [ ] Utilize mtls termination between traefik and api services
- [ ] Enable SSL with Postgres
- [ ] Set up OpenTelemetry collectors + Grafana
- [ ] Utilize healthcheck endpoints