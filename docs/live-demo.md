# Live Demo

> A hosted live demo is not yet available.

## Try It Locally

The fastest way to try TurboLynx is with the sample LDBC SF1 dataset:

**1. Build** (see [Installation](installation/overview.md)):
```bash
mkdir build && cd build
cmake -GNinja -DCMAKE_BUILD_TYPE=Release -DENABLE_TCMALLOC=OFF -DBUILD_UNITTESTS=OFF -DTBB_TEST=OFF ..
ninja
```

**2. Download sample dataset:**
[LDBC SF1 dataset](https://drive.google.com/file/d/1PqXw_Fdp9CDVwbUqTQy0ET--mgakGmOA/view?usp=drive_link)

**3. Load and query:**
```bash
./tools/bulkload --workspace /tmp/demo_db --data /path/to/ldbc_sf1
./tools/client --workspace /tmp/demo_db
TurboLynx >> analyze
TurboLynx >> MATCH (n:Person) RETURN n.firstName, n.lastName LIMIT 10;
```

## Sample Queries

```cypher
-- Count all persons
MATCH (n:Person) RETURN COUNT(*);

-- 1-hop friends
MATCH (a:Person)-[:KNOWS]->(b:Person)
WHERE a.firstName = 'Alice'
RETURN b.firstName, b.lastName;

-- 2-hop friends of friends
MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
WHERE a.id = 933
RETURN DISTINCT c.firstName
LIMIT 20;

-- Most connected persons
MATCH (a:Person)-[:KNOWS]->(b:Person)
RETURN a.firstName, COUNT(*) AS friends
ORDER BY friends DESC
LIMIT 10;
```
