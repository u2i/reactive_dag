# Changelog

## [0.17.0-rc.29](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.28...v0.17.0-rc.29) (2026-08-17)


### Features

* `Report.by/2` — a meta count broken down per bucket ([#149](https://github.com/u2i/reactive_dag/issues/149)) ([1aa802c](https://github.com/u2i/reactive_dag/commit/1aa802c4cca8c199e48325025b660bacf0b2f57d))

## [0.17.0-rc.28](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.27...v0.17.0-rc.28) (2026-08-17)


### Features

* zero-arity functions as deferred `poll … args:` values ([#147](https://github.com/u2i/reactive_dag/issues/147)) ([9d8c01b](https://github.com/u2i/reactive_dag/commit/9d8c01b93ecc59548672c9e6f982738365081a51))

## [0.17.0-rc.27](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.26...v0.17.0-rc.27) (2026-08-17)


### Features

* schedule_drain — dirties_on marks AND consumes the mark ([b85d319](https://github.com/u2i/reactive_dag/commit/b85d3198e52534cfba37acd2f5c858b59a2a8e08))
* schedule_drain — dirties_on marks AND consumes the mark ([#142](https://github.com/u2i/reactive_dag/issues/142)) ([c514b17](https://github.com/u2i/reactive_dag/commit/c514b1720162d2e8ea6c2caae74d7d9683444279))
* Source.progress/3 — the only signal from inside one poll ([efeac0e](https://github.com/u2i/reactive_dag/commit/efeac0e169ee4df4197bc889899a5ff62bb6d87e))
* Source.progress/3 — the only signal from inside one poll ([5394276](https://github.com/u2i/reactive_dag/commit/53942760cee7fde0b566a2c70c876e11077f4e86))

## [0.17.0-rc.26](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.25...v0.17.0-rc.26) (2026-08-17)


### Features

* slice poll_as — ask a SOURCE for one slice, not just stored rows ([555a5e5](https://github.com/u2i/reactive_dag/commit/555a5e5233ccf85e0b8000cd5ad4ea71c38b72ef))
* slice poll_as — ask a SOURCE for one slice, not just stored rows ([f0a6389](https://github.com/u2i/reactive_dag/commit/f0a6389ad30d21a6e283c0dd2aab77facadec0ae))

## [0.17.0-rc.25](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.24...v0.17.0-rc.25) (2026-08-16)


### Features

* reject two nodes declaring one scanner; say why a tableless slice fails ([6c4ad6b](https://github.com/u2i/reactive_dag/commit/6c4ad6b70aaa7f4dafc669a6d1edf0b6a1075ee0))
* reject two nodes declaring one scanner; say why a tableless slice fails ([b1cf079](https://github.com/u2i/reactive_dag/commit/b1cf0791caf42f21623b0a98877820f20b345af2))


### Bug Fixes

* a bare-list poll result no longer crashes the scan ([980b434](https://github.com/u2i/reactive_dag/commit/980b4343c20d05b3f3a907b721d9cf0338672ad1))
* a bare-list poll result no longer crashes the scan ([#138](https://github.com/u2i/reactive_dag/issues/138)) ([669a21f](https://github.com/u2i/reactive_dag/commit/669a21f471873564a7ce34ef48494b12e6209389))

## [0.17.0-rc.24](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.23...v0.17.0-rc.24) (2026-08-16)


### Features

* a sweep keeps its per-source detail, and reports progress ([b1ac8fb](https://github.com/u2i/reactive_dag/commit/b1ac8fbe60efd5af84be58cacfab4bcdf753484b))
* a sweep keeps its per-source detail, and reports progress ([#133](https://github.com/u2i/reactive_dag/issues/133)) ([75c9fe9](https://github.com/u2i/reactive_dag/commit/75c9fe91ebb5295df87b89eb186b2e74bfb26feb))

## [0.17.0-rc.23](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.22...v0.17.0-rc.23) (2026-08-16)


### Features

* a scan can be cancelled, not just succeed or fail ([76b59b3](https://github.com/u2i/reactive_dag/commit/76b59b323e19cd185d0c4fe7034abf9f49b183bd))
* a scan can be cancelled, not just succeed or fail ([#122](https://github.com/u2i/reactive_dag/issues/122)) ([8fefe40](https://github.com/u2i/reactive_dag/commit/8fefe4047e59111a261780f9065e60e818fd3a99))

## [0.17.0-rc.22](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.21...v0.17.0-rc.22) (2026-08-16)


### Features

* a sweep is the default crontab — one job, all sources, graph order ([c440c2a](https://github.com/u2i/reactive_dag/commit/c440c2a0171c2b6c18d19e06d6d174a4c7dbb3bc))
* crontab/3 takes `order:`, and drops a dedup source-as-node made vestigial ([4b53f20](https://github.com/u2i/reactive_dag/commit/4b53f2027d18c33e0e441733fcce33e3e1fcfbfb))
* crontab/3 takes order:, and drops a dedup source-as-node made vestigial ([472e6ab](https://github.com/u2i/reactive_dag/commit/472e6ab4c80b17acf8ccc6d1bacf10341dea00a4))
* make the sweep safe under duplicate enqueues and a multi-node cluster ([c1fce64](https://github.com/u2i/reactive_dag/commit/c1fce641cf3a28cc87d59cd330187664d9586f5b))
* order the SWEEP, not just the crontab list ([5c5fdca](https://github.com/u2i/reactive_dag/commit/5c5fdca5809478c93cd0f862aa7d2ebb1f785091))

## [0.17.0-rc.21](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.20...v0.17.0-rc.21) (2026-08-16)


### ⚠ BREAKING CHANGES

* a source is a node — `poll` replaces `scan`, and the pairing is an edge

### Features

* a node can decline a claimed key, distinct from retiring it ([17bd42c](https://github.com/u2i/reactive_dag/commit/17bd42cba8ffda0b9bb351851f524c47303d9f7b))
* a node can decline a claimed key, distinct from retiring it ([226e9a7](https://github.com/u2i/reactive_dag/commit/226e9a7773906e6a18c6c68edcf457863457e4f4))
* a source is a node — `poll` replaces `scan`, and the pairing is an edge ([caeed28](https://github.com/u2i/reactive_dag/commit/caeed2879685c5ffced720c773282810ec0c4d3f))

## [0.17.0-rc.20](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.19...v0.17.0-rc.20) (2026-08-16)


### Features

* scan/reprocess telemetry carries the job args, and emits :start ([d553985](https://github.com/u2i/reactive_dag/commit/d553985685d0f0fdf3339ed4d4f27311a97426ae))
* scan/reprocess telemetry carries the job args, and emits :start ([fb54b0a](https://github.com/u2i/reactive_dag/commit/fb54b0a47815890b7457088eb9670ef8c97b8949))

## [0.17.0-rc.19](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.18...v0.17.0-rc.19) (2026-08-16)


### Features

* cell status says WHY a key count is zero ([fd71bfd](https://github.com/u2i/reactive_dag/commit/fd71bfd456a811e707f0d3feb2dd2f04d53e0274))
* cell status says WHY a key count is zero ([6ec21f7](https://github.com/u2i/reactive_dag/commit/6ec21f73f86269294074bc2421d7b819cccb5dc1))

## [0.17.0-rc.18](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.17...v0.17.0-rc.18) (2026-08-16)


### Features

* a drain step carries op and depth ([f575712](https://github.com/u2i/reactive_dag/commit/f575712abeb725399a60af6242f2e1d83eecb719))
* a drain step carries op and depth ([#114](https://github.com/u2i/reactive_dag/issues/114)) ([dbc326e](https://github.com/u2i/reactive_dag/commit/dbc326e5ef97cfc1b731a413a9a5aba5df04b912))

## [0.17.0-rc.17](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.16...v0.17.0-rc.17) (2026-08-16)


### Bug Fixes

* one scheduled crawl per scanner, and don't lose a shared scanner's args ([0b299ed](https://github.com/u2i/reactive_dag/commit/0b299ed273cc1e9978fb66af7cfdaa6c276bbc2f))
* one scheduled crawl per scanner, and don't lose a shared scanner's args ([237cf8d](https://github.com/u2i/reactive_dag/commit/237cf8d97a274e8ecb96a5b8283c0d90c22e5d7a))

## [0.17.0-rc.16](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.15...v0.17.0-rc.16) (2026-08-16)


### Bug Fixes

* let the boolean :upsert form report :created ([#107](https://github.com/u2i/reactive_dag/issues/107)) ([15d37ea](https://github.com/u2i/reactive_dag/commit/15d37ea8f25f8d88edc75d953587c05614ad0232))
* let the boolean :upsert form report :created ([#107](https://github.com/u2i/reactive_dag/issues/107)) ([3bf040e](https://github.com/u2i/reactive_dag/commit/3bf040ec6c49d07a2fd77801b0a26660f29b93f1))

## [0.17.0-rc.15](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.14...v0.17.0-rc.15) (2026-08-16)


### Features

* crontab/3 takes host args; prove one poll can mark several leaves ([52b3013](https://github.com/u2i/reactive_dag/commit/52b3013d28de134d831884029a13f1152e313cd8))
* crontab/3 takes host args; prove one poll can mark several leaves ([ab189d5](https://github.com/u2i/reactive_dag/commit/ab189d53f4e62269074f6464ce51bdf3717835d7))

## [0.17.0-rc.14](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.13...v0.17.0-rc.14) (2026-08-16)


### Features

* a reprocess invalidates the fingerprint, so the work actually re-runs ([94399da](https://github.com/u2i/reactive_dag/commit/94399da0fc2566fe6f18850ebfd5b22352544034))
* a reprocess invalidates the fingerprint, so the work actually re-runs ([4f98aa8](https://github.com/u2i/reactive_dag/commit/4f98aa889c3b6b26454ac0b37e5b90d5a2c73857))

## [0.17.0-rc.13](https://github.com/u2i/reactive_dag/compare/v0.17.0-rc.12...v0.17.0-rc.13) (2026-08-16)


### Features

* ReprocessWorker — queue a re-derive and drain it now ([184b123](https://github.com/u2i/reactive_dag/commit/184b123718ec8032363d75dcae4bc863a4eed21e))
* ReprocessWorker — queue a re-derive and drain it now ([4dff714](https://github.com/u2i/reactive_dag/commit/4dff714e47718b447e070cbd506829d4cc9ae503))

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
