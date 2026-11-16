# Generated specification manifests

These files are generated from the reference KeyDB tree by:

```sh
cargo run -p xtask -- spec
```

| File | Contents |
|---|---|
| `commands.json` | 236 command entries: name, C handler, arity, flags, ACL categories |
| `configs.json` | configuration directives and their KeyDB types/defaults |
| `tests.json` | Tcl test inventory with line counts |
| `keydb.lock` | FNV-1a content hashes of the reference source and test tree |
| `coverage.md` | implemented-vs-total command report |
| `implemented.txt` | hand-maintained list of implemented commands |

The target is the attached `KeyDB/` development snapshot, whose
`version.h` reports `255.255.255`. Upstream v6.3.4 is fetched separately into
`reference/keydb/` for triage.

`cargo run -p xtask -- check` validates the committed manifests without needing
the reference tree.
