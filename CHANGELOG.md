# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 1.0.0 (2026-02-16)


### Features

* add application entry point ([6d66f53](https://github.com/openjobspec/ojs-backend-redis/commit/6d66f53e3a53a76ad384d8f856c950b2f47f7f62))
* **admin:** add admin API handlers and embedded UI ([54a983b](https://github.com/openjobspec/ojs-backend-redis/commit/54a983b189a88990241baff07b6b82645ac82cef))
* **api:** add HTTP handlers and middleware ([4b97dc2](https://github.com/openjobspec/ojs-backend-redis/commit/4b97dc239670df85033efd4ba31a518739bf3f3a))
* **api:** add request body size limit middleware ([18b4334](https://github.com/openjobspec/ojs-backend-redis/commit/18b4334bab722ad7708948b6db58ca67b5244f6d))
* **api:** update system manifest and improve handler error handling ([b318282](https://github.com/openjobspec/ojs-backend-redis/commit/b3182826e94bf5483affc29afd192f6f61e4471a))
* **core:** add domain types, interfaces, and validation ([096360c](https://github.com/openjobspec/ojs-backend-redis/commit/096360c1f2865e206b1222a393373d9133abc8b0))
* **core:** add max-length validation for type and queue fields ([3a71df5](https://github.com/openjobspec/ojs-backend-redis/commit/3a71df541fe36515554dd5d9ebba0d92acc08f33))
* **core:** add resource-aware worker capability matching ([ff3a7a6](https://github.com/openjobspec/ojs-backend-redis/commit/ff3a7a6f8b6c113416e4e6c6d9a9e6f36f0cc650))
* **core:** add worker_id to Job and improve marshaling ([da1af71](https://github.com/openjobspec/ojs-backend-redis/commit/da1af71dcedc4af357225ff66a5fe1fb64cad7ad))
* **grpc:** implement gRPC protocol binding ([84944a9](https://github.com/openjobspec/ojs-backend-redis/commit/84944a945a9b28b4a47ad7db84fae428e0e8e3de))
* **metrics:** add Prometheus instrumentation for jobs and HTTP ([2188730](https://github.com/openjobspec/ojs-backend-redis/commit/21887304582c70400b992f59ed938def23a8bd66))
* **redis:** implement Redis backend with codec and key management ([a0361f9](https://github.com/openjobspec/ojs-backend-redis/commit/a0361f92364b598c518de24653a0a35162ef889a))
* **scheduler:** add background job scheduler ([2b0337c](https://github.com/openjobspec/ojs-backend-redis/commit/2b0337c016e6a6a13ea8c9a661293cc01a070584))
* **server:** add server configuration and HTTP routing ([a0e8e3b](https://github.com/openjobspec/ojs-backend-redis/commit/a0e8e3bfff46e4b9ce9d39ffc6e3858de5b0fce4))
* **server:** integrate admin, metrics, and gRPC into server ([d14e717](https://github.com/openjobspec/ojs-backend-redis/commit/d14e717c554068539c7cec216887f2b049af54c1))


### Bug Fixes

* **core:** use ErrCodeConflict in NewConflictError constructor ([075f74b](https://github.com/openjobspec/ojs-backend-redis/commit/075f74be0f27eebd64bde3e9665525b62b12bd34))


### Performance Improvements

* **redis:** bound regex cache to prevent unbounded memory growth ([eaa270b](https://github.com/openjobspec/ojs-backend-redis/commit/eaa270ba1a5fd62eadfd1c7ed5b04d351124a7fe))

## [Unreleased]

### Changed
- Aligned manifest implementation version with the server OJS version.
- Aligned GitHub Actions Go toolchain selection with `go.mod`.
- Corrected README job examples so `args` payloads match server validation.

### Added
- Runtime confidence tests for admin placeholders, server routing, and worker directive behavior.
- Project governance files (`CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`).
- Release, dependency update, and vulnerability scan automation workflows.

## [1.0.0-rc.1] - 2026-02-14

### Added
- Initial Redis-backed OpenJobSpec server release candidate with OJS conformance, retries, scheduling, cron, workflows, and dead-letter support.
