DATA=/source-data/dbpedia
WS=/data/dbpedia

mkdir -p $WS

/turbograph-v3/build-release/tools/turbolynx import \
    --workspace $WS \
    --skip-histogram \
    --nodes NODE                                                    --nodes $DATA/nodes.json \
    --relationships "http://dbpedia.org/ontology/wikiPageRedirects" --relationships $DATA/edges_wikiPageRedirects_3801.csv \
    --relationships "http://dbpedia.org/property/homepage"          --relationships $DATA/edges_homepage_589.csv \
    --relationships "http://xmlns.com/foaf/0.1/homepage"            --relationships $DATA/edges_homepage_7372.csv \
    --relationships "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" --relationships $DATA/edges_22-rdf-syntax-ns#type_6803.csv \
    --relationships "http://dbpedia.org/property/website"           --relationships $DATA/edges_website_315.csv \
    --relationships "http://dbpedia.org/ontology/foundationPlace"   --relationships $DATA/edges_foundationPlace_3010.csv \
    --relationships "http://dbpedia.org/ontology/developer"         --relationships $DATA/edges_developer_3910.csv \
    --relationships "http://dbpedia.org/ontology/nationality"       --relationships $DATA/edges_nationality_797.csv \
    --relationships "http://dbpedia.org/property/clubs"             --relationships $DATA/edges_clubs_7515.csv \
    --relationships "http://dbpedia.org/ontology/birthPlace"        --relationships $DATA/edges_birthPlace_2950.csv \
    --relationships "http://purl.org/dc/terms/subject"              --relationships $DATA/edges_subject_5592.csv \
    --relationships "http://dbpedia.org/ontology/thumbnail"         --relationships $DATA/edges_thumbnail_4765.csv \
    --relationships "http://dbpedia.org/ontology/city"              --relationships $DATA/edges_city_55.csv \
    --relationships "http://www.w3.org/2002/07/owl#sameAs"          --relationships $DATA/edges_owl#sameAs_9037.csv \
    --relationships "http://xmlns.com/foaf/0.1/depiction"           --relationships $DATA/edges_depiction_6991.csv \
    --relationships "http://dbpedia.org/ontology/country"           --relationships $DATA/edges_country_8183.csv \
    --relationships "http://dbpedia.org/ontology/countryOrigin"     --relationships $DATA/edges_countryOrigin_7466.csv \
    --relationships "http://dbpedia.org/ontology/keyPerson"         --relationships $DATA/edges_keyPerson_285.csv \
    --relationships "http://dbpedia.org/ontology/location"          --relationships $DATA/edges_location_4045.csv \
    --relationships "http://dbpedia.org/ontology/locationCity"      --relationships $DATA/edges_locationCity_1514.csv \
    --relationships "http://dbpedia.org/ontology/locationCountry"   --relationships $DATA/edges_locationCountry_1719.csv \
    --relationships "http://dbpedia.org/ontology/industry"          --relationships $DATA/edges_industry_6326.csv \
    --relationships "http://dbpedia.org/ontology/genre"             --relationships $DATA/edges_genre_3794.csv \
    --relationships "http://dbpedia.org/ontology/musicBy"           --relationships $DATA/edges_musicBy_6854.csv \
    --relationships "http://dbpedia.org/ontology/musicComposer"     --relationships $DATA/edges_musicComposer_4171.csv \
    --relationships "http://dbpedia.org/ontology/musicFusionGenre"  --relationships $DATA/edges_musicFusionGenre_354.csv \
    --relationships "http://dbpedia.org/ontology/musicSubgenre"     --relationships $DATA/edges_musicSubgenre_7829.csv \
    --relationships "http://dbpedia.org/ontology/musicalArtist"     --relationships $DATA/edges_musicalArtist_8387.csv \
    --relationships "http://dbpedia.org/ontology/musicalBand"       --relationships $DATA/edges_musicalBand_8808.csv \
    --relationships "http://dbpedia.org/ontology/stylisticOrigin"   --relationships $DATA/edges_stylisticOrigin_3904.csv \
    --relationships "http://dbpedia.org/ontology/subregion"         --relationships $DATA/edges_subregion_6627.csv \
    --relationships "http://dbpedia.org/property/album"             --relationships $DATA/edges_album_5285.csv
