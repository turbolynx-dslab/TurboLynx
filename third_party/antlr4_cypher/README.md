# antlr4_cypher


## Grammar provenance

`Cypher.g4` is the local grammar used to generate the vendored C++ parser sources
under `antlr4/`.

- The vendored parser packaging and top-level subtree layout follow
  `qi-hua/antlr4-cypher` (BSD 3-Clause).
- The local `Cypher.g4` is adapted from the legacy openCypher grammar, and the
  source attribution is preserved in the file header,
  `GRAMMAR-PROVENANCE.md`, `LICENSE.openCypher-Apache-2.0`, and
  `NOTICE.openCypher`.

## Setup docker

Refer https://github.com/antlr/antlr4/tree/master/docker

```
git clone https://github.com/antlr/antlr4.git
cd antlr4
git checkout v4.11.0
cd docker

docker build -t antlr/antlr4 --platform linux/amd64 .
```

### Run docker

```
# need to assign your directory
MY_ANTLR_DIRECTORY=/home/jhko/dev/turbograph-v3/tbgpp-compiler/third_party/antlr4_cypher
cd $MY_ANTLR_DIRECTORY

# execute
docker run --rm -u $(id -u ${USER}):$(id -g ${USER}) -v `pwd`:/work antlr/antlr4 -Dlanguage=Cpp Cypher.g4 -o antlr4/
```
