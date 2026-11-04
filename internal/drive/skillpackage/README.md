# Drive Skill packages

This package validates ZIP archives and SKILL.md frontmatter before a Skill is
published. It returns relative regular-file paths, content readers and the Unix
owner execute bit. `internal/drive/skills.go` applies these files in one atomic
commit; Drive path normalization, case-fold collisions and quotas still apply at
that boundary.

ZIP filename bytes must be UTF-8, independently of the UTF-8 flag. Comments,
extra fields and DOS metadata that do not affect the retained projection are
accepted. Go's ZIP reader still interprets ZIP64 sizes/offsets and verifies entry
content. Unix/macOS entries without file-type bits are ordinary files, or
directories when the name ends in `/`; permission bits remain meaningful.
Explicit special types and setuid/setgid/sticky bits are rejected. Archives are
not renamed or normalized, and `__MACOSX` is not silently discarded.

| Behavior | Executable evidence |
|---|---|
| Info-ZIP, Python, JSZip, native macOS ditto, 7-Zip; Go FileInfoHeader timestamps; executable and UTF-8 filenames | `TestProducerArchivesPreservePublishedContent`; HTTP `TestDrivePublishesCommonZIPProducers` |
| Comments, timestamp fields, missing Unix type bits, unflagged UTF-8, synthetic Windows DOS flags | `TestBenignZIPMetadata` |
| Traversal, absolute/invalid UTF-8 paths, duplicates, file/folder collisions, extra sidecar root, special file modes | `TestArchivePathAndTypeRules` |
| Entry count, per-entry and expanded size, upload archive size | HTTP `TestDriveSkillArchiveRules` |
| Vendor names, name charset, YAML anchors/aliases/tags, NUL description | `TestFrontmatterRules` |
| Bounded frontmatter consumption from a source that continues indefinitely | `TestFrontmatterConsumptionIsBounded` |
| Atomic path/quota refusal after valid archive parsing | HTTP `TestDriveSkillCommitRefusalPreservesPublishedFiles` |
| Non-NFC filenames remain explicit Drive path errors | HTTP `TestDriveSkillPublicationDoesNotNormalizePaths` |

The testdata README records actual producer provenance. Windows Explorer and
WinRAR cases are synthetic metadata shapes, not claims that those tools ran.
