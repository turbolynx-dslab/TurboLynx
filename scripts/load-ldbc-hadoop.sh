#!/bin/bash
# Load an LDBC SNB Datagen (Hadoop variant) dataset into a TurboLynx
# workspace. Intended for cross-checking against the Spark mini fixture
# under scripts/load-ldbc-mini.sh — the two datagen variants differ in
# entity columns, datetime format, and how list-valued fields are laid
# out, so the same query stack should be exercised on both to catch
# format-specific regressions.
#
# Pipeline:
#   1. docker run ldbc/datagen:latest  (Hadoop v0.3.6+, params.ini for
#      ldbc.snb.datagen.generator.scaleFactor:snb.interactive.0.1)
#   2. scripts/preprocessors/ldbc-preprocessors/ldbc-hadoop-preprocess.py
#      to convert the raw output into the TurboLynx fixture shape that
#      this script consumes.
#   3. bash scripts/load-ldbc-hadoop.sh <build-dir> <data-dir> <workspace-dir>
set -euo pipefail
BUILD_DIR=${1:-}
DATA=${2:-}
WS=${3:-}
if [ -z "$BUILD_DIR" ] || [ -z "$DATA" ] || [ -z "$WS" ]; then
    echo "usage: $0 <build-dir> <data-dir> <workspace-dir>"
    echo "  data-dir: output of ldbc-hadoop-preprocess.py (has dynamic/ + static/)"
    exit 2
fi
TURBOLYNX="$BUILD_DIR/tools/turbolynx"
if [ ! -x "$TURBOLYNX" ]; then
    echo "ERROR: turbolynx binary not found at $TURBOLYNX"
    exit 1
fi
rm -rf "$WS"
mkdir -p "$WS"
"$TURBOLYNX" import \
    --workspace "$WS" \
    --nodes Person          "$DATA/dynamic/Person.csv" \
    --nodes Comment:Message "$DATA/dynamic/Comment.csv" \
    --nodes Post:Message    "$DATA/dynamic/Post.csv" \
    --nodes Forum           "$DATA/dynamic/Forum.csv" \
    --nodes Organisation    "$DATA/static/Organisation.csv" \
    --nodes Place           "$DATA/static/Place.csv" \
    --nodes Tag             "$DATA/static/Tag.csv" \
    --nodes TagClass        "$DATA/static/TagClass.csv" \
    --relationships HAS_CREATOR    "$DATA/dynamic/Comment_hasCreator_Person.csv" \
    --relationships HAS_CREATOR    "$DATA/dynamic/Post_hasCreator_Person.csv" \
    --relationships IS_LOCATED_IN  "$DATA/dynamic/Person_isLocatedIn_Place.csv" \
    --relationships IS_LOCATED_IN  "$DATA/dynamic/Comment_isLocatedIn_Place.csv" \
    --relationships IS_LOCATED_IN  "$DATA/dynamic/Post_isLocatedIn_Place.csv" \
    --relationships IS_LOCATED_IN  "$DATA/static/Organisation_isLocatedIn_Place.csv" \
    --relationships KNOWS          "$DATA/dynamic/Person_knows_Person.csv" \
    --relationships LIKES          "$DATA/dynamic/Person_likes_Comment.csv" \
    --relationships LIKES          "$DATA/dynamic/Person_likes_Post.csv" \
    --relationships HAS_INTEREST   "$DATA/dynamic/Person_hasInterest_Tag.csv" \
    --relationships STUDY_AT       "$DATA/dynamic/Person_studyAt_Organisation.csv" \
    --relationships WORK_AT        "$DATA/dynamic/Person_workAt_Organisation.csv" \
    --relationships REPLY_OF       "$DATA/dynamic/Comment_replyOf_Post.csv" \
    --relationships REPLY_OF       "$DATA/dynamic/Comment_replyOf_Comment.csv" \
    --relationships HAS_TAG        "$DATA/dynamic/Comment_hasTag_Tag.csv" \
    --relationships HAS_TAG        "$DATA/dynamic/Forum_hasTag_Tag.csv" \
    --relationships HAS_TAG        "$DATA/dynamic/Post_hasTag_Tag.csv" \
    --relationships CONTAINER_OF   "$DATA/dynamic/Forum_containerOf_Post.csv" \
    --relationships HAS_MODERATOR  "$DATA/dynamic/Forum_hasModerator_Person.csv" \
    --relationships HAS_MEMBER     "$DATA/dynamic/Forum_hasMember_Person.csv" \
    --relationships IS_PART_OF     "$DATA/static/Place_isPartOf_Place.csv" \
    --relationships IS_SUBCLASS_OF "$DATA/static/TagClass_isSubclassOf_TagClass.csv" \
    --relationships HAS_TYPE       "$DATA/static/Tag_hasType_TagClass.csv" \
    --log-level warn
echo "Loaded to $WS"
