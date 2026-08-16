# Changelog

## [0.17.0-rc.12](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.11...v0.17.0-rc.12) (2026-08-16)


### Features

* slice — declare the dimension a person selects a node by ([00272a1](https://github.com/u2i/reactive_dag/commit/00272a10819fcb26cde5aa8b04c9ebecfb012190))
* slice — declare the dimension a person selects a node by ([d72bc17](https://github.com/u2i/reactive_dag/commit/d72bc1727ba5f3bcd778d53ca6e3a870741bb35c))

## [0.17.0-rc.11](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.10...v0.17.0-rc.11) (2026-08-16)


### ⚠ BREAKING CHANGES

* `Rows.reconcile/3` returns `{:ok, changed, detail}` rather than `{:ok, changed}`, and `Payload.upsert/6`/`upsert_identity/5` may return `:created`. A caller matching `{:ok, changed}` adds `_`; one matching `== :changed` on an upsert wants `!= :unchanged`.

### Features

* reconcile/3 returns what it already knew — created/updated/revived/retired ([7ad6cb5](https://github.com/u2i/reactive_dag/commit/7ad6cb51a91c3f99fb28b0a5c16efcba1510bfd5))

## [0.17.0-rc.10](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.9...v0.17.0-rc.10) (2026-08-15)


### Features

* ReactiveDag.ScanWorker — the library defines the Oban job ([fcf9bdd](https://github.com/u2i/reactive_dag/commit/fcf9bdd60777c552fadd4a8fd68229c865e2f363))
* ReactiveDag.ScanWorker — the library defines the Oban job ([1792c1a](https://github.com/u2i/reactive_dag/commit/1792c1a37d6237985bf16f1338df5edc32553192))

## [0.17.0-rc.9](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.8...v0.17.0-rc.9) (2026-08-15)


### Features

* observed: :partial — a scan that looked at only part of the upstream ([#83](https://github.com/u2i/reactive_dag/issues/83)) ([4a7e759](https://github.com/u2i/reactive_dag/commit/4a7e7594838f9983ce75777469523f1f1f68498c))
* observed: :partial — a scan that looked at only part of the upstream ([#83](https://github.com/u2i/reactive_dag/issues/83)) ([e5672da](https://github.com/u2i/reactive_dag/commit/e5672dacb13abc9d075c2fc8847a808ec074739e))


### Performance Improvements

* count in the datastore instead of loading every row ([#76](https://github.com/u2i/reactive_dag/issues/76)) ([bda0134](https://github.com/u2i/reactive_dag/commit/bda0134f8eeba8c63ce26033d7816d9162d38b4a))
* count in the datastore instead of loading every row ([#76](https://github.com/u2i/reactive_dag/issues/76)) ([975965c](https://github.com/u2i/reactive_dag/commit/975965ca8d7632a611ac06b961380b2a5b26c60c))

## [0.17.0-rc.8](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.7...v0.17.0-rc.8) (2026-08-15)


### ⚠ BREAKING CHANGES

* a marking leaf now reports returning keys as changed where it previously reported nothing. That is the bug being fixed, but a host relying on the old silence will see downstream recomputes it did not before.
* `meta[:retain_if_vanished]` is now `:keep | {:mark, fun}` rather than `true`. Only reachable by a host inspecting cell meta directly; the DSL spelling `retain_if_vanished true` is unchanged.

### Features

* report revivals instead of warning about them (closes [#82](https://github.com/u2i/reactive_dag/issues/82)) ([d9101bf](https://github.com/u2i/reactive_dag/commit/d9101bfde19622bc52ea075136b3a7808ad60adf))
* warn when a marked-retired key returns unchanged ([#82](https://github.com/u2i/reactive_dag/issues/82)) ([cd58a34](https://github.com/u2i/reactive_dag/commit/cd58a34e1cdb15d798bbf109bba170151a717285))


### Bug Fixes

* declaring no computation is a compile error ([#91](https://github.com/u2i/reactive_dag/issues/91)) ([15b2ec9](https://github.com/u2i/reactive_dag/commit/15b2ec9335a605d4a15f23348c729996b92dedd9))
* declaring no computation is a compile error ([#91](https://github.com/u2i/reactive_dag/issues/91)) ([f2d2944](https://github.com/u2i/reactive_dag/commit/f2d2944e04b3f7e4bd94b9d65d593da09fd603cc))
* the silent-revival warning missed the declared mark policy ([956f128](https://github.com/u2i/reactive_dag/commit/956f128cf596397a7e40022ae9532ecbe5a92166))


### Code Refactoring

* keep and mark are one declaration, not two mechanisms ([c6f7c78](https://github.com/u2i/reactive_dag/commit/c6f7c788bd6b898f3cd01619cc00ade4a80d1c65))

## [0.17.0-rc.7](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.6...v0.17.0-rc.7) (2026-08-14)


### Features

* poll_cell/3 + controls/1 — the library describes, the host builds the GUI ([9ef7f73](https://github.com/u2i/reactive_dag/commit/9ef7f734fad7812d0ba5264a04202b42fdf72bdb))
* retain_if_vanished — keep the row when a scan drops its key ([#85](https://github.com/u2i/reactive_dag/issues/85)) ([3bcebd4](https://github.com/u2i/reactive_dag/commit/3bcebd4b02a8382cd6aaee2e6a9cfcd0910872e5))
* retain_if_vanished — keep the row when a scan drops its key ([#85](https://github.com/u2i/reactive_dag/issues/85)) ([76eafcb](https://github.com/u2i/reactive_dag/commit/76eafcbd2add27efa95e3a1c05aa6a7ecece9335))
* scan args + cadence, declared on the leaf ([#86](https://github.com/u2i/reactive_dag/issues/86)) ([7c183c3](https://github.com/u2i/reactive_dag/commit/7c183c3f5bcf6b3bc1911a8a99e3bf7b9204caa5))
* scan args + cadence, declared on the leaf ([#86](https://github.com/u2i/reactive_dag/issues/86)) ([29ef2fe](https://github.com/u2i/reactive_dag/commit/29ef2fe58a59ee9917f37f19e5462c324349c25f))

## [0.17.0-rc.6](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.5...v0.17.0-rc.6) (2026-08-13)


### ⚠ BREAKING CHANGES

* the `:on_step` option is removed. A caller using it attaches a handler instead — three lines, and it no longer has to be the only consumer:

### Features

* the drain emits telemetry, replacing the :on_step callback ([332ca2a](https://github.com/u2i/reactive_dag/commit/332ca2a4e20b5842061b24a90c90528ae915c53b))

## [0.17.0-rc.5](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.4...v0.17.0-rc.5) (2026-08-12)


### ⚠ BREAKING CHANGES

* the modules and config keys listed above are removed. A host with a coordination-tuple table can drop it; nothing reads or writes it. A host with a custom `CoordinationWriter` moves its extension columns (`source_ref`, `last_seen_at`, `tombstoned_at`, `strength`) onto the node's own resource, where the rest of the row already is. A leaf driver calling `Tuple.reconcile/3` calls `ReactiveDag.Node.Rows.reconcile/3` with the cell instead of the cell id. `Tuple.reconcile_set/3` has no replacement: it batched the spine write, and there is no spine to batch.
* the six functions above are removed. A caller wanting a node's statuses uses `ReactiveDag.Node.Rows` (which reads the resource, with policies and filters); a caller wanting freshness reads a column on the node's own resource. `ReactiveDag.Tuple.Writer.put/3` returns `:ok` rather than a boolean, which the CoordinationWriter contract already allowed and reads as "assume changed".

### Features

* fingerprint — what counts as the same observation ([#73](https://github.com/u2i/reactive_dag/issues/73)) ([f631a1b](https://github.com/u2i/reactive_dag/commit/f631a1b5f836f95b52071f7746b6a7e87ee62f7f))
* remove the coordination tuple entirely ([ae23687](https://github.com/u2i/reactive_dag/commit/ae2368799ad2fa7676800e780907c189d0122c1c))
* the coordination spine is a presence set ([fb45d39](https://github.com/u2i/reactive_dag/commit/fb45d39659b54927cbae6fb92551f988eca3c16b))

## [0.17.0-rc.4](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.3...v0.17.0-rc.4) (2026-08-12)


### ⚠ BREAKING CHANGES

* `Verdict.for_cell/2` takes a `%ReactiveDag.Cell{}` rather than a cell id string (get one from `plan.cells[id]`), and no longer accepts `:key_scope` — filter the resource and use `Verdict.rollup/2` instead. `Insights.cell_status/2` drops `:last_observed_at`; freshness is a tuple column with no resource equivalent, and nothing read it. A `union` over a node with no payload attributes now raises at assembly instead of silently unioning nothing.
* `verdict? true` and the `status:` slot on `reduce`/`join` are removed, along with `Insights.cell_status/2`'s `verdict?` field. A tableless verdict node becomes a payload node: give it a table with a `:status` column, an `:upsert` action, and rewrite `status: fn g, items -> "present" end` as

### Features

* read results from resources, not the coordination tuple ([98f23f5](https://github.com/u2i/reactive_dag/commit/98f23f590abcf61fb63a5d9c7890937954fdfca1))
* verdicts are rows — drop the tableless verdict node ([29f7133](https://github.com/u2i/reactive_dag/commit/29f7133279b96a96e477f50c7d0b8a6d356f8f7c))

## [0.17.0-rc.3](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.2...v0.17.0-rc.3) (2026-08-12)


### ⚠ BREAKING CHANGES

* remove attestations from the library

### Features

* remove attestations from the library ([c2bf45e](https://github.com/u2i/reactive_dag/commit/c2bf45e68dfd882ff15c72374cb330f174529424))

## [0.17.0-rc.2](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.1...v0.17.0-rc.2) (2026-08-12)


### Features

* `status:` writes a :status column when the node has one ([634cb99](https://github.com/u2i/reactive_dag/commit/634cb99842d8f248628e1362c1c44d6ecf55eae1))
* `status:` writes a :status column when the node has one ([41f3b7a](https://github.com/u2i/reactive_dag/commit/41f3b7abb1b645f79d654ab74056a5fd3921d0c0))
* ReactiveDag.Basis — a versioned content digest of a row set ([a6891d2](https://github.com/u2i/reactive_dag/commit/a6891d2b7350a6f9e4066f32fbc7fe95f1507f1f))
* ReactiveDag.Basis — a versioned content digest of a row set ([6fd7dfa](https://github.com/u2i/reactive_dag/commit/6fd7dfa291bd78c14fc71ca1787ef0403acd03a2))
* ReactiveDag.Config.validate!/0 — fail at boot, not at the first query ([41d12d2](https://github.com/u2i/reactive_dag/commit/41d12d207d92565bf983ef8661ae44a46e4232c9))
* ReactiveDag.Config.validate!/0 — fail at boot, not at the first query ([#53](https://github.com/u2i/reactive_dag/issues/53)) ([acf2862](https://github.com/u2i/reactive_dag/commit/acf2862931db26e58adfebc9ebc515dbeed8ea32))
* scan — declare the scanner that feeds a leaf ([0908eea](https://github.com/u2i/reactive_dag/commit/0908eea0833c991222557648ebba014470af14c4))
* scan — declare the scanner that feeds a leaf ([afc6d10](https://github.com/u2i/reactive_dag/commit/afc6d1009be7593659ae5b056e376be711b42895))
* snapshot the changed row on the frontier, so claims survive it ([09ab41d](https://github.com/u2i/reactive_dag/commit/09ab41d5d98837bce06f03fdc6cc9bc43212f332))
* snapshot the changed row on the frontier, so claims survive it ([#60](https://github.com/u2i/reactive_dag/issues/60)) ([a2e5929](https://github.com/u2i/reactive_dag/commit/a2e592971b108a3600c5fa8c0b7a9fef4a3e3b94))
* union — the graph-wide roll-up as a node ([b7584c2](https://github.com/u2i/reactive_dag/commit/b7584c29196b63c93e70bfc9df59a41f8c7b78e4))
* union — the graph-wide roll-up as a node (+ an attested guard) ([c6a435b](https://github.com/u2i/reactive_dag/commit/c6a435b616ada98b5b47887bfe9a1d775400f1d8))


### Bug Fixes

* a scanner and a computation on one node is a contradiction ([7f1daaf](https://github.com/u2i/reactive_dag/commit/7f1daaf7009aa613ec669996951a0d029d55b7b3))
* an attested view declaring payload attributes now raises ([3cd9fb1](https://github.com/u2i/reactive_dag/commit/3cd9fb18e550a4471f4ebc8c5e9505a32f9e691e))

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
