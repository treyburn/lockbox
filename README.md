# Lockbox

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
- [ ] Set up CI
  - [ ] Linting w/ golangci-lint
  - [ ] Testing
  - [ ] Report test coverage in PR and on project README
- [ ] Bring in Just for encapsulating typical project commands

### Service
- [ ] Update Go + Deps
- [ ] Utilize multi-stage docker build
- [ ] Swap over to Postgres by default
- [ ] Add in 
- [ ] Improve testing
- [ ] Refactor to use standard Go project layouts
- [ ] Drop gorilla/mux for chi
- [ ] Swap over to sqlc instead of raw SQL
- [ ] Create an OpenAPI specification w/ validation
- [ ] Add a gRPC API
- [ ] Instrument for OpenTelemetry
  - [ ] Utilize decorator pattern 

### Deployment
- [ ] Generate a valid cert via LetsEncrypt
- [ ] Utilize mtls termination between traefik and api services
- [ ] Enable SSL with Postgres
- [ ] Set up OpenTelemetry collectors + Grafana