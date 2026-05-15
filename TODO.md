## TODO
Various items for cleanup.

### Project
- [ ] Improve README
    - [x] Create a TODO section
    - [ ] Provide a thorough project description
    - [ ] Provide useful code snippets for cli execution
    - [ ] Document the API(s)
    - [x] Document prerequisites and general dev setup
- [x] Set up CI
    - [x] Linting w/ golangci-lint
    - [x] Testing
    - [x] Report test coverage in PR and on project README
    - [x] Build docker container
- [x] Bring in Just for encapsulating typical project commands

### Service
- [x] Update Go + Deps
- [ ] Utilize multi-stage docker build
- [ ] Swap over to Postgres logger by default
- [ ] Restructure main for better configurability
    - [ ] Add in env var/cli configuration setting
    - [ ] Reconsider defaults
- [ ] Improve Tests
    - [ ] Improve unit tests
        - [x] Better coverage for file logger
        - [x] Better coverage for PG logger
        - [x] Better coverage for service layer
        - [ ] main pkg unit tests
    - [ ] Add integration testing
        - [ ] File logger
        - [ ] Postgres logger
    - [ ] Add end-to-end testing (hitting http endpoints from outside the service)
    - [ ] Smoke test main (using github.com/rogpeppe/go-internal/testscript)
    - [ ] Handling combining test coverage in CI (unit + integration + smoke)
- [X] Refactor to use standard Go project layouts
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
- [x] Add postgres to docker compose setup for local dev
- [ ] Generate a valid cert via LetsEncrypt
- [ ] Utilize mtls termination between traefik and api services
- [ ] Enable SSL with Postgres
- [ ] Set up OpenTelemetry collectors + Grafana
- [ ] Utilize healthcheck endpoints