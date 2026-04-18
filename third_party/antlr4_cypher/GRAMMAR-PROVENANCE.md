`third_party/antlr4_cypher/Cypher.g4` is the local grammar used to generate the
vendored C++ parser sources under `third_party/antlr4_cypher/antlr4`.

Its provenance is split across two sources:

- The vendored parser subtree layout and packaging follow
  `qi-hua/antlr4-cypher`:
  https://github.com/qi-hua/antlr4-cypher
- The local `Cypher.g4` is adapted from the legacy openCypher grammar, whose
  original source attribution is preserved in the header comment of
  `Cypher.g4`:
  https://s3.amazonaws.com/artifacts.opencypher.org/legacy/Cypher.g4

The `qi-hua/antlr4-cypher` project is distributed under the BSD 3-Clause
License. See `third_party/antlr4_cypher/LICENSE`.

The original legacy openCypher grammar is distributed under Apache License 2.0
with an attribution notice. See:

- `third_party/antlr4_cypher/LICENSE.openCypher-Apache-2.0`
- `third_party/antlr4_cypher/NOTICE.openCypher`
