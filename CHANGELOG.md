# Changelog

## [0.17.0-rc.1](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc...v0.17.0-rc.1) (2026-08-11)


### Bug Fixes

* **ci:** publish to hex from release-please's own run ([5f2b266](https://github.com/u2i/reactive_dag/commit/5f2b266fe9eae8642a7b6170b74e8cd82bcd55a4))
* **ci:** publish to hex from release-please's own run ([dba486c](https://github.com/u2i/reactive_dag/commit/dba486c341e427d9806e5a1a72019ef2cf813eeb))

## [0.17.0-rc](https://github.com/u2i/reactive_dag/compare/v0.16.0...v0.17.0-rc) (2026-08-11)


### ⚠ BREAKING CHANGES

* recompute_by — the unit a change invalidates, subsuming key_rule
* payload_key no longer statically defaults to :key — it derives from the primary key (identical outcome for resources whose PK is :key). Composite-PK payload nodes change contract: rows must carry identity fields, not a :key column.
* key_rule {:bucket, kind} is removed (merged, never released) — declare the Calendar calculation in group_by and use key_rule: {:group, from: :key}.
* the `reference` DSL entity is now `context`; the cell meta key `reference_inputs` (read by Graph.build_parents for hand-built plans) is now `context_inputs`.
* reduce/join `read:` no longer accepts functions (use `query:`, or run/compute for non-Ash reads); verdict nodes must declare `status:` instead of returning :status rows from `into:`; `into:` returning a list now raises (use `expand:`).

### Features

* aggregate/reduce parity — one vocabulary at both rungs ([b0163ef](https://github.com/u2i/reactive_dag/commit/b0163efa2ad5100925d88b60cb71411c12968fd4))
* Ash-first authoring — declarative slots, library-run reads, run actions ([6f27365](https://github.com/u2i/reactive_dag/commit/6f27365d5b66166e50eeb72aef9b47742ec6957a))
* bounded concurrency on per_key ([7a92cf7](https://github.com/u2i/reactive_dag/commit/7a92cf781dd79ff26bd39a18b0d111ac7da3b692))
* bounded concurrency on per_key ([#30](https://github.com/u2i/reactive_dag/issues/30) item 4) ([4dbaf26](https://github.com/u2i/reactive_dag/commit/4dbaf260609dabccd9c287f1f50cf75cbcfda19e))
* combinator reads are Ash-only; verdicts declare status:; expand is a slot ([45905e0](https://github.com/u2i/reactive_dag/commit/45905e0b11fd1ccf8cc40a9cff05a5a0f38b2036))
* composite recompute units — grain stated once, read scoped per column ([3650499](https://github.com/u2i/reactive_dag/commit/36504993b84c125e97aaf6edfae6fc6d3597ddee))
* declarative read/group_by/key/into slots on reduce and join ([5f6f48a](https://github.com/u2i/reactive_dag/commit/5f6f48adf11a13f966799ce2beaa564ef69dfdf0))
* dirties_on — ordinary Ash writes trigger the cascade ([69793d7](https://github.com/u2i/reactive_dag/commit/69793d7f9f04001fe8d50f61c50543bc181c5b68))
* dirties_on — ordinary Ash writes trigger the cascade ([#39](https://github.com/u2i/reactive_dag/issues/39)) ([513b0a4](https://github.com/u2i/reactive_dag/commit/513b0a42a998ce757f9244ffda427aca5f001e4c))
* join sides as Ash relationships — and a real two-node join ([593b291](https://github.com/u2i/reactive_dag/commit/593b29176be63b853a10248d649342f1b47db431))
* key_rule {:bucket, kind} — declarative mid-granularity claims on the calendar ([95bda5c](https://github.com/u2i/reactive_dag/commit/95bda5c1476d967ad158c08ac0bf61c97a498741))
* key_rule {:bucket, kind} — declarative mid-granularity claims on the calendar ([036f2dc](https://github.com/u2i/reactive_dag/commit/036f2dcdbf9a5c9a95774092a3cacd08e52dd973)), closes [#28](https://github.com/u2i/reactive_dag/issues/28)
* key_rule: :group on the combinator — claims derived from group_by itself ([ca36a8e](https://github.com/u2i/reactive_dag/commit/ca36a8e4ee6d863ef04af9ab9f3324e8df960383))
* key_rule: :group on the combinator — claims derived from group_by itself ([576afb4](https://github.com/u2i/reactive_dag/commit/576afb4b7530d038e9908381671093e7d9086099)), closes [#31](https://github.com/u2i/reactive_dag/issues/31)
* keys are Ash keys — derived payload_key, identity-keyed nodes, join pairs ([c411ebd](https://github.com/u2i/reactive_dag/commit/c411ebd0a3cdc7851d46279712b6a00b4375b1b4))
* over_grain — the edge and its grain, declared once ([5b3f69f](https://github.com/u2i/reactive_dag/commit/5b3f69f0715bb376f3c3ec99d09030a6790369fc))
* over_rel — an Ash relationship IS the DAG edge ([60af5b8](https://github.com/u2i/reactive_dag/commit/60af5b8d43cbda007e0544e6d4db39645a34eab9))
* ReactiveDag.Insights — the engine viewed from outside ([#41](https://github.com/u2i/reactive_dag/issues/41)) ([d056d06](https://github.com/u2i/reactive_dag/commit/d056d06ed89a2bfb0de3bbb823618df9b25591ef))
* ReactiveDag.Insights — the read API behind the dashboard ([d829fcb](https://github.com/u2i/reactive_dag/commit/d829fcb3ea26a62b76d428c67e5d09c828bc481e))
* recompute_by — the unit a change invalidates, subsuming key_rule ([abddd94](https://github.com/u2i/reactive_dag/commit/abddd945987609f0e0fe0c3b7b0cba6cd996f054))
* rename the read-as-context edge — `reference` becomes `context` ([75999de](https://github.com/u2i/reactive_dag/commit/75999de95e7c45c35030b7c09797aec529c655ae)), closes [#24](https://github.com/u2i/reactive_dag/issues/24)
* run — invoke a generic Ash action as the node's recompute ([bc04451](https://github.com/u2i/reactive_dag/commit/bc04451fdab4752808cb68de21c9e897cf93686c))
* strategy-reported meta on Report steps ([79936ab](https://github.com/u2i/reactive_dag/commit/79936abebcce697b9438235ae0d89b74a5d66a96))
* strategy-reported meta on Report steps ([#30](https://github.com/u2i/reactive_dag/issues/30) item 3) ([f12956f](https://github.com/u2i/reactive_dag/commit/f12956fb4e9410f4fbf1faa025da5835aca7ad49))
* the classic date rollup — group_by loads Ash calculations; Calendar buckets ship ([97f17f2](https://github.com/u2i/reactive_dag/commit/97f17f24ef1f9b308d3ac122135356833abd9676))
* the classic date rollup — group_by loads Ash CALCULATIONS; Calendar buckets ship ([d4f0b1a](https://github.com/u2i/reactive_dag/commit/d4f0b1a1adea372a022bffdbfc3d4cef46e3fc08)), closes [#23](https://github.com/u2i/reactive_dag/issues/23)
* the per_key rung — a driven loop, so inputs can be fingerprinted ([103fdcd](https://github.com/u2i/reactive_dag/commit/103fdcd58836dc5398d5b6ed41e6d107a198fdf3))
* the per_key rung — a driven loop, so inputs can be fingerprinted ([#30](https://github.com/u2i/reactive_dag/issues/30)) ([948c3c0](https://github.com/u2i/reactive_dag/commit/948c3c097a95f5630ddfc2db8c8f2390e86d95c2))
* unify {:bucket, kind} into :group — one rule, two resolutions ([6419de7](https://github.com/u2i/reactive_dag/commit/6419de7e7628c2667c36e4095375686af22b2f64))


### Bug Fixes

* retire vanished units — reconcile, not just upsert ([90985c8](https://github.com/u2i/reactive_dag/commit/90985c8ba24db48c50662d586cd6b5176e812a98))
* retire vanished units — reconcile, not just upsert ([#37](https://github.com/u2i/reactive_dag/issues/37)) ([25ee47b](https://github.com/u2i/reactive_dag/commit/25ee47b274e483fcc3636870d34378c90496d5eb))


### Reverts

* drop the cross-node join (has_many was the wrong borrowing) ([b93c69e](https://github.com/u2i/reactive_dag/commit/b93c69e77408f42ecc80903c05e70c6cd02f3b4b))
