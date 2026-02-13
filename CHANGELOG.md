# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
