DATA=/source-data/ldbc/sf1
WS=/data/ldbc/sf1_test

mkdir -p $WS

/turbograph-v3/build-release/tools/turbolynx import \
    --workspace $WS \
    --nodes Person            --nodes $DATA/dynamic/Person.csv \
    --nodes Comment:Message   --nodes $DATA/dynamic/Comment.csv \
    --nodes Post:Message      --nodes $DATA/dynamic/Post.csv \
    --nodes Forum             --nodes $DATA/dynamic/Forum.csv \
    --nodes Organisation      --nodes $DATA/static/Organisation.csv \
    --nodes Place             --nodes $DATA/static/Place.csv \
    --nodes Tag               --nodes $DATA/static/Tag.csv \
    --nodes TagClass          --nodes $DATA/static/TagClass.csv \
    --relationships HAS_CREATOR         --relationships $DATA/dynamic/Comment_hasCreator_Person.csv \
    --relationships POST_HAS_CREATOR    --relationships $DATA/dynamic/Post_hasCreator_Person.csv \
    --relationships IS_LOCATED_IN       --relationships $DATA/dynamic/Person_isLocatedIn_Place.csv \
    --relationships KNOWS               --relationships $DATA/dynamic/Person_knows_Person.csv \
    --relationships LIKES               --relationships $DATA/dynamic/Person_likes_Comment.csv \
    --relationships LIKES_POST          --relationships $DATA/dynamic/Person_likes_Post.csv \
    --relationships HAS_INTEREST        --relationships $DATA/dynamic/Person_hasInterest_Tag.csv \
    --relationships STUDY_AT            --relationships $DATA/dynamic/Person_studyAt_Organisation.csv \
    --relationships REPLY_OF            --relationships $DATA/dynamic/Comment_replyOf_Post.csv \
    --relationships REPLY_OF            --relationships $DATA/dynamic/Comment_replyOf_Comment.csv \
    --relationships COMMENT_IS_LOCATED_IN --relationships $DATA/dynamic/Comment_isLocatedIn_Place.csv \
    --relationships HAS_TAG             --relationships $DATA/dynamic/Comment_hasTag_Tag.csv \
    --relationships CONTAINER_OF        --relationships $DATA/dynamic/Forum_containerOf_Post.csv \
    --relationships HAS_MODERATOR       --relationships $DATA/dynamic/Forum_hasModerator_Person.csv \
    --relationships HAS_MEMBER          --relationships $DATA/dynamic/Forum_hasMember_Person.csv \
    --relationships FORUM_HAS_TAG       --relationships $DATA/dynamic/Forum_hasTag_Tag.csv \
    --relationships POST_HAS_TAG        --relationships $DATA/dynamic/Post_hasTag_Tag.csv \
    --relationships POST_IS_LOCATED_IN  --relationships $DATA/dynamic/Post_isLocatedIn_Place.csv \
    --relationships WORK_AT             --relationships $DATA/dynamic/Person_workAt_Organisation.csv \
    --relationships ORG_IS_LOCATED_IN   --relationships $DATA/static/Organisation_isLocatedIn_Place.csv \
    --relationships IS_PART_OF          --relationships $DATA/static/Place_isPartOf_Place.csv \
    --relationships IS_SUBCLASS_OF      --relationships $DATA/static/TagClass_isSubclassOf_TagClass.csv \
    --relationships HAS_TYPE            --relationships $DATA/static/Tag_hasType_TagClass.csv
