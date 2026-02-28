# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0](https://github.com/openjobspec/ojs-backend-redis/compare/v0.1.0...v0.2.0) (2026-02-28)


### Features

* add cluster mode support ([bdc8fc8](https://github.com/openjobspec/ojs-backend-redis/commit/bdc8fc8a225133c612dcb351873a54e199d0bd3f))
* add graceful drain on SIGTERM signal ([5110a93](https://github.com/openjobspec/ojs-backend-redis/commit/5110a93f349b5859f75f41333a1e825582df6b14))
* add health check endpoint ([f9f8c96](https://github.com/openjobspec/ojs-backend-redis/commit/f9f8c96a158fed51dde3820f49c14ffddb4e6085))
* implement job priority queue support ([d0880ba](https://github.com/openjobspec/ojs-backend-redis/commit/d0880ba79acf1eeb719f74e032eed1542aaa3c55))


### Bug Fixes

* correct error response format for batch operations ([161e38f](https://github.com/openjobspec/ojs-backend-redis/commit/161e38f5f0ea248b777f16c91902e39dc5e6ff98))
* correct Lua script atomicity for pipeline ([6713897](https://github.com/openjobspec/ojs-backend-redis/commit/67138971f57732b8f64cabc3490642d2414cfd97))
* handle connection timeout gracefully ([e523a74](https://github.com/openjobspec/ojs-backend-redis/commit/e523a74da8f8d3f28d0946fdcfde3f032c64ff73))


### Performance Improvements

* optimize batch insert query ([58d3c24](https://github.com/openjobspec/ojs-backend-redis/commit/58d3c2426567bbb7eb50c72139e21dfe512b7fe0))
* optimize query for pending job lookup ([1d0fc13](https://github.com/openjobspec/ojs-backend-redis/commit/1d0fc138c4f83e15bfdd14d4c5ea349e1e9d0c5a))
* optimize scan-based job listing ([96e6aac](https://github.com/openjobspec/ojs-backend-redis/commit/96e6aac0e8a804e3a36043440601e0d7e180919e))

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
