# TurboLynx Demo — Scene-Based Narrative Design

> **핵심 원칙:**
> 1. **Scene 기반** — 탭/섹션 전환으로 각 기술 개념을 독립적으로 설명
> 2. **Step-by-step 빌드업** — 각 Scene 안에서 Prev/Next로 개념이 쌓임
> 3. **연속 서사** — Problem → CGC → Query → GEM → SSRF → Performance 순서로 이해가 쌓임
> 4. **example.jsx 스타일** — SVG 그래프, 인터랙티브 테이블, "Aha!" 인사이트 박스

---

## 전체 네비게이션 구조

```
[⚡ Problem] [🧩 CGC] [🔍 Query] [⚙️ GEM] [🗜️ SSRF] [🏆 Performance]
     ↑ tab bar 항상 노출, 각 탭 클릭 or 섹션 내 Next 버튼으로 다음 탭으로 이동
```

각 섹션은 **full viewport height**, 스크롤 없음. 섹션 내부 step만 Prev/Next.

---

## Scene 0 — ⚡ The Problem: Schemaless Data

**목표:** 왜 기존 엔진이 DBpedia 같은 schemaless 그래프에서 실패하는지 직관적으로 이해

**핵심 메시지:**
- DBpedia는 레이블이 없음 — 모든 노드는 그냥 `NODE`
- 타입 정보는 `rdf:type` 엣지로만 알 수 있음 (레이블 ≠ 타입)
- 같은 타입 내에서도 스키마가 완전히 다름 (Person 657개 → 508가지 서로 다른 스키마)
- 하나의 flat table로 합치면 NULL이 폭발적으로 늘어남

### 실제 DBpedia 샘플 데이터 (hardcoded for demo)

분석 스크립트: `demo/scripts/analyze_dbpedia.py`
분석 결과: `demo/scripts/dbpedia_demo_data.json`

**4개 타입, 13개 노드:**

```
Person (4 nodes):
  Tiger Woods         — 22 props: [height, weight, abstract, birthDate, birthPlace,
                                    team, position, graduationYear, deathDate*, ...]
  Ferdinand I (Portug)— 15 props: [abstract, activeYearsEnd, activeYearsStart,
                                    birthDate, deathDate, orderInOffice, ...]
  Kate Forsyth        — 17 props: [abstract, birthDate, birthName, birthYear,
                                    wikiPageID, wikiPageRevisionID, ...]
  Cato the Elder      — 12 props: [abstract, orderInOffice, allegiance,
                                    wikiPageID, ...]

Film (3 nodes):
  Sholay              — 12 props: [abstract, runtime, wikiPageID, ...]
  Private Life Henry  — 18 props: [abstract, runtime, budget, gross, imdbId,
                                    releaseDate, director, ...]
  One Night of Love   — 14 props: [abstract, gross, imdbId, runtime,
                                    musicComposer, ...]

City (3 nodes):
  Lake City, Florida  — 40 props: [abstract, areaTotal, populationDensity,
                                    areaCode, areaLand, elevation, utcOffset, ...]
  Priolo Gargallo     — 22 props: [abstract, areaCode, areaTotal, elevation,
                                    population, ...]
  Fisher, Arkansas    — 36 props: [abstract, areaCode, areaLand, areaTotal,
                                    populationDensity, ...]

Book (3 nodes):
  Moon Goddess & Son  — 20 props: [abstract, dcc, isbn, lcc, numberOfPages,
                                    oclc, publisher, author, ...]
  Lycurgus of Thrace  — 5 props:  [abstract, wikiPageID, comment, label]
  The English Teacher — 15 props: [abstract, dcc, lcc, numberOfPages, oclc, ...]
```

**일부 엣지 (시각화용):**
- Tiger Woods -[birthPlace]→ Lake City (실제 관계는 아니지만 시각화용)
- Private Life Henry -[director]→ Alexander Korda (타입 노드 없이 레이블만)
- 각 노드 -[rdf:type]→ 타입노드 (점선 군집 경계로 표현)

### Steps (4단계)

---

**Step 0 — "DBpedia: 77M nodes. All labeled the same."**

```
화면 구성:
┌─────────────────────────────────────────────────────────────────┐
│  Interactive Force-Directed Graph (SVG + 커스텀 spring physics) │
│                                                                   │
│  ┌ ─ ─ Person ─ ─ ┐   ┌ ─ ─ Film ─ ─ ─ ┐                      │
│    ○ Tiger Woods        ○ Sholay                                  │
│    ○ Ferdinand I         ○ Private Life..                         │
│    ○ Kate Forsyth        ○ One Night..                            │
│    ○ Cato the Elder   └ ─ ─ ─ ─ ─ ─ ─ ─ ┘                      │
│  └ ─ ─ ─ ─ ─ ─ ─ ┘                                              │
│                        ┌ ─ ─ Book ─ ─ ─ ┐                      │
│  ┌ ─ ─ City ─ ─ ─ ┐    ○ Moon Goddess                           │
│    ○ Lake City          ○ Lycurgus                                │
│    ○ Priolo              ○ English Teacher                        │
│    ○ Fisher           └ ─ ─ ─ ─ ─ ─ ─ ─ ┘                      │
│  └ ─ ─ ─ ─ ─ ─ ─ ┘                                              │
│                                                                   │
│  모든 노드 위에: "NODE" 레이블 (동일)                              │
│  군집 경계 점선 + 타입명 (rdf:type 엣지로 결정됨)                  │
│  일부 property 엣지 (birthPlace, director 등) 회색 선             │
└─────────────────────────────────────────────────────────────────┘

인터랙션:
  - 노드 드래그 가능 (spring physics)
  - hover: 노드명 tooltip
  - 우하단 안내: "Click any node to see its schema →"

하단 설명:
  "Every node shares the same 'NODE' label.
   Type information only comes from rdf:type edges."
```

---

**Step 1 — "Same type. Completely different schemas."**

```
화면 구성:
┌─────────────────────────────────────────────────────────────────┐
│  Interactive Graph (유지) + 오른쪽 Schema Panel                  │
│                                                                   │
│  ○ Tiger Woods  ──클릭→  ┌─────────────────────────────────┐    │
│                           │  Tiger Woods  (Person)           │    │
│                           │  22 properties                   │    │
│                           │  ├ abstract    ████              │    │
│                           │  ├ height      ██                │    │
│                           │  ├ weight      ██                │    │
│                           │  ├ birthDate   ██                │    │
│                           │  ├ birthPlace  ██                │    │
│                           │  ├ team        ██                │    │
│                           │  └ ...16 more                    │    │
│                           └─────────────────────────────────┘    │
│                                                                   │
│  다른 Person 노드 클릭 → 완전히 다른 schema 노출                  │
│  Ferdinand I: 15 props (다른 집합)                                │
│  Cato the Elder: 12 props (또 다른 집합)                         │
│                                                                   │
│  우하단 통계:                                                     │
│  "Person type: 657 nodes → 508 unique schemas"                   │
│  "City type:  4,500 nodes → 1,298 unique schemas"                │
└─────────────────────────────────────────────────────────────────┘

인터랙션:
  - 노드 클릭 → 오른쪽 패널에 schema 표시 (슬라이드인)
  - 다른 노드 클릭 → 패널 업데이트
  - "Neo4j Mode" 토글 버튼:
    ON → 각 노드 원 위에 작은 schema key들이 텍스트로 표시
         "Per-row schema: reads full schema on every access"
    OFF → 원래 깔끔한 노드로 돌아감
```

---

**Step 2 — "What if we just merge everything into one table?"**

```
애니메이션 순서:
1. 그래프 노드들이 왼쪽으로 이동, 테이블로 변환 (400ms)
2. Wide table 등장:

   name               abstract  runtime  budget  gross  birthDate  areaTotal  isbn  ...  (총 45+ 컬럼)
   Tiger Woods        ████      NULL     NULL    NULL   ██         NULL       NULL
   Ferdinand I        ████      NULL     NULL    NULL   ██         NULL       NULL
   Kate Forsyth       ████      NULL     NULL    NULL   ██         NULL       NULL
   Sholay             ████      ██       NULL    NULL   NULL       NULL       NULL
   Private Life Henry ████      ██       ██      ██     NULL       NULL       NULL
   Lake City, FL      ████      NULL     NULL    NULL   NULL       ██         NULL
   Lycurgus of Thrace ████      NULL     NULL    NULL   NULL       NULL       NULL
   ...

3. NULL 셀 빨간 하이라이트 (하나씩 점등, 500ms)
4. 카운터 애니메이션:
   NULL cells: 0 → 487
   NULL ratio: 0% → 83%

통계 카드:
  13 nodes × 45 columns = 585 cells
  487 NULLs (83%)
  Only 98 cells have real data

하단:
  "Every engine reading this table must check each NULL.
   At DBpedia scale: 77M nodes × 282K properties → 212B checks/query."
```

---

**Step 3 — "At DBpedia scale, this kills every engine."**

```
화면 구성 (수치 카드 + 바 차트):

┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│   77 Million     │  │   282,764        │  │   212 Billion    │
│   nodes          │  │   unique schemas │  │   NULL checks    │
│   in DBpedia     │  │   per query plan │  │   per query      │
└──────────────────┘  └──────────────────┘  └──────────────────┘

엔진별 쿼리 시간 (실제 벤치마크):
  Neo4j   ████████████████████████████████████  301s  (타임아웃)
  Kuzu    █████████████████████                  132s
  DuckDB  █████████████████████                  141s
  TurboLynx █  3.5s

"All of them read NULLs they don't need.
 TurboLynx eliminates them before the query even starts."
```

**"Aha!" 박스 (Step 3 끝):**
> "TurboLynx solves this with CGC — Compact Graphlet Chunking.
> Instead of one flat table, it groups nodes by schema similarity
> into graphlets, eliminating 212 billion NULL checks at the storage level."

---

### 인터랙션 상세

#### Force-Directed Graph (Steps 0-1)
```
구현: React + SVG + 커스텀 requestAnimationFrame physics
- 노드: SVG circle, r=22
- 같은 타입 attraction: F = k * (target_dist - actual_dist) where target_dist = 80px
- 다른 타입 repulsion: F = -k / dist^2
- 전체 중력: 화면 중앙으로 약한 인력
- 드래그: mousedown → fix position → mousemove → mouseup → release
- 타입 군집 경계: d3-like convex hull OR 단순 타입별 center 기준 dashed ellipse
```

#### Schema Panel (Step 1)
```
- 클릭한 노드의 schema를 오른쪽 사이드 패널에 표시
- 각 property: 이름 + 값 미리보기 + 길이 바
- 색상: 해당 타입 색상
- 애니메이션: slide-in from right (framer-motion)
- 다른 타입의 노드 클릭 시: 색상 + 내용 교체
```

#### Table Animation (Step 2)
```
- 노드들이 테이블 행으로 "날아가는" 애니메이션
  (framer-motion layoutId 또는 FLIP 기법)
- NULL 셀: 하나씩 빨간색으로 점등 (stagger animation)
- 카운터: requestAnimationFrame eased count-up
```

---

### 타입별 색상

| 타입 | 색상 | hex |
|------|------|-----|
| Person | 파랑 | #3B82F6 |
| Film | 보라 | #8B5CF6 |
| City | 노랑/주황 | #F59E0B |
| Book | 초록 | #10B981 |

---

### 데이터 상수 위치

`demo/app/lib/demo-data.ts` 에 `DBPEDIA_NODES`, `DBPEDIA_TYPE_STATS` 추가:

```typescript
export const DBPEDIA_NODES = [
  // Person
  { id: "n1", name: "Tiger Woods",          type: "Person", color: "#3B82F6",
    schema: ["abstract","height","weight","birthDate","birthPlace","team","position",
             "graduationYear","college","profession","wikiPageID","wikiPageRevisionID",
             "thumb","caption","website"] },
  { id: "n2", name: "Ferdinand I",           type: "Person", color: "#3B82F6",
    schema: ["abstract","activeYearsEndYear","activeYearsStartYear","birthDate",
             "deathDate","orderInOffice","wikiPageID","wikiPageRevisionID",
             "predecessor","successor","spouse","parent","child","sibling","religion"] },
  { id: "n3", name: "Kate Forsyth",          type: "Person", color: "#3B82F6",
    schema: ["abstract","birthDate","birthName","birthYear","wikiPageID",
             "wikiPageRevisionID","genre","nationality","award","occupation",
             "activeYearsStart","language","movement","label","publisher",
             "almaMater","education"] },
  { id: "n4", name: "Cato the Elder",        type: "Person", color: "#3B82F6",
    schema: ["abstract","orderInOffice","allegiance","wikiPageID",
             "wikiPageRevisionID","birthDate","deathDate","rank","caption","era"] },
  // Film
  { id: "n5", name: "Sholay",                type: "Film", color: "#8B5CF6",
    schema: ["abstract","runtime","wikiPageID","wikiPageRevisionID",
             "country","language","starring","director"] },
  { id: "n6", name: "Private Life of Henry", type: "Film", color: "#8B5CF6",
    schema: ["abstract","runtime","budget","gross","imdbId","releaseDate",
             "director","producer","distributor","cinematography",
             "editing","starring","musicComposer","country","language",
             "wikiPageID","wikiPageRevisionID","thumbnail"] },
  { id: "n7", name: "One Night of Love",     type: "Film", color: "#8B5CF6",
    schema: ["abstract","gross","imdbId","runtime","musicComposer",
             "wikiPageID","wikiPageRevisionID","releaseDate",
             "director","starring","country"] },
  // City
  { id: "n8",  name: "Lake City, FL",        type: "City", color: "#F59E0B",
    schema: ["abstract","areaTotal","populationDensity","areaCode","areaLand",
             "areaWater","elevation","utcOffset","timezone","postalCode",
             "governmentType","leaderName","leaderTitle","population",
             "populationAsOf","areaMetro","areaUrban","subdivisionName",
             "isPartOf","country","longd","latd","wikiPageID","wikiPageRevisionID",
             "north","east","south","west","map","mapCaption","blank","blankName",
             "settlementType","areaCode","areaCode","populationTotal",
             "populationDensity","populationUrban","populationMetro",
             "areaTotalSqMi","areaLandSqMi"] },
  { id: "n9",  name: "Priolo Gargallo",      type: "City", color: "#F59E0B",
    schema: ["abstract","areaCode","areaTotal","elevation","population",
             "wikiPageID","wikiPageRevisionID","country","isPartOf",
             "longd","latd","mayor","postalCode","istat","cap",
             "provinceCode","region","timezone","utcOffset","areaWater"] },
  { id: "n10", name: "Fisher, Arkansas",     type: "City", color: "#F59E0B",
    schema: ["abstract","areaCode","areaLand","areaTotal","populationDensity",
             "elevation","wikiPageID","wikiPageRevisionID","postalCode",
             "isPartOf","country","longd","latd","timezone","utcOffset",
             "governmentType","population","populationAsOf",
             "blank","blankName","areaTotalSqMi","areaLandSqMi",
             "populationTotal","populationDensity","areaCode2","areaCode3"] },
  // Book
  { id: "n11", name: "Moon Goddess & Son",   type: "Book", color: "#10B981",
    schema: ["abstract","dcc","isbn","lcc","numberOfPages","oclc",
             "publisher","author","wikiPageID","wikiPageRevisionID",
             "releaseDate","language","subject","dewey","mediaType",
             "country","genre","series","edition","coverArtist"] },
  { id: "n12", name: "Lycurgus of Thrace",   type: "Book", color: "#10B981",
    schema: ["abstract","wikiPageID","wikiPageRevisionID","comment","label"] },
  { id: "n13", name: "The English Teacher",  type: "Book", color: "#10B981",
    schema: ["abstract","dcc","lcc","numberOfPages","oclc",
             "wikiPageID","wikiPageRevisionID","author","publisher",
             "releaseDate","language","isbn","genre","country","mediaType"] },
] as const;

export const DBPEDIA_TYPE_STATS = [
  { type: "Person", total: 657,    uniqueSchemas: 508  },
  { type: "Film",   total: 421,    uniqueSchemas: 287  },
  { type: "City",   total: 4500,   uniqueSchemas: 1298 },
  { type: "Book",   total: 3154,   uniqueSchemas: 557  },
] as const;

// Full DBpedia scale numbers
export const DBPEDIA_SCALE = {
  totalNodes:     77_000_000,
  uniqueSchemas:  282_764,
  nullChecks:     212_000_000_000,
} as const;
```

---

### 구현 파일

- `S0_Problem.tsx` — 전면 재작성
  - `ForceGraph` 컴포넌트 (SVG + requestAnimationFrame physics)
  - `SchemaPanel` 컴포넌트 (클릭시 우측 슬라이드인)
  - `TableView` 컴포넌트 (Step 2 NULL 테이블 + 카운터)
  - `ScaleView` 컴포넌트 (Step 3 수치 카드 + 벤치마크 바)
- `demo/app/lib/demo-data.ts` — `DBPEDIA_NODES`, `DBPEDIA_TYPE_STATS`, `DBPEDIA_SCALE` 추가

---

## Scene 1 — 🧩 CGC: Compact Graphlet Chunking

**목표:** CGC가 schemaless 데이터를 어떻게 조직화하는지 이해

### Steps (5단계)

**Step 0 — "Raw: 10 nodes, 7 distinct schemas"**
```
SVG: 10개 Person 노드가 흩어진 상태
각 노드는 회색으로 균일하게 표시
Subtitle: "No structure — schema chaos"
```

**Step 1 — "Extract schemas"**
```
SVG: 각 노드가 자신의 schema에 따라 색깔로 분류됨
  파란 노드: {age, FN, LN}
  보라 노드: {FN, LN, gender}
  노란 노드: {FN}
  초록 노드: {name, age}
  ...
노드마다 아래에 "{age,FN,LN}" 텍스트 레이블
```

**Step 2 — "Size-based layering"**
```
SVG: 같은 schema끼리 그룹핑되어 나란히 정렬됨
레이어: [Layer 0: large groups] [Layer 1: medium] [Layer 2: singletons]
```

**Step 3 — "Agglomerative clustering (cost-aware)"**
```
SVG: 유사한 스키마 그룹들이 병합 화살표로 합쳐짐
Cost Function 공식: c(H) = C_sch·|H| + C_null·ΣΓ(gl) + C_vec·ΣΨ(|gl|)
슬라이더 3개 (interactive):
  C_sch: schema count penalty
  C_null: null overhead penalty
  C_vec: vectorization bonus
```

**Step 4 — "Final: 4 graphlets"**
```
SVG: 노드들이 4개 그룹(그래프렛)으로 깔끔하게 모임
  gl₁ (파랑): v0, v5, v7 — schema: [age, FN, LN]
  gl₂ (보라): v1, v2, v9 — schema: [FN, LN, gender]
  gl₃ (노랑): v3, v4, v6 — schema: [age, gender, major]
  gl₄ (초록): v8        — schema: [name, age]

NULL 비교:
  Before (1 flat table): 54 NULLs (67%)
  After  (4 graphlets):   6 NULLs (13%)
  → 89% NULL reduction
```

**"Aha!" 박스:**
> "CGC finds the cost-optimal balance between schema count, NULL overhead, and vectorization.
> On DBpedia (282,764 schemas → 34 graphlets), it eliminates **212 billion** NULL checks per query."

---

## Scene 2 — 🔍 Query: How a Query Runs

**목표:** CGC Index가 쿼리를 어떻게 처리하는지 이해 (DBpedia Hero Query 기준)

### Steps (4단계)

**Step 0 — "The Query"**
```
Cypher 에디터 스타일로 쿼리 표시:
  MATCH (p)-[:birthPlace]->(c),
        (p)-[:rdf:type]->(t)
  WHERE p.position = 'Goalkeeper'
    AND c.populationTotal > 1000000
  RETURN p.name, p.birthDate, c.name, c.population, t.type

아래에: "DBpedia에서 포지션이 Goalkeeper인 선수의 출생도시 인구가 100만 이상인 선수 찾기"
```

**Step 1 — "CGC Schema Index Lookup"**
```
34개 파티션이 원형/그리드로 표시됨 (dot cloud)
쿼리가 CGC index를 조회함

결과:
  파티션 #7  → lit (파랑): position 속성 포함 → Goalkeeper-compatible
  파티션 #12 → lit (주황): populationTotal 속성 포함 → City-compatible
  나머지 32개 → dimmed (회색)

카운터: "2/34 partitions scanned · 212B null-ops skipped"
```

**Step 2 — "UnionAll Query Plan"**
```
트리 구조로 query plan 표시:
  UnionAll(gl_p1, gl_p2, gl_p3)        ← 파티션 #7의 서브그룹
       ⋈ [:birthPlace]
  UnionAll(gl_c1, gl_c2)               ← 파티션 #12의 서브그룹
       ⋈ [:rdf:type]
  gl_t1

"기존 엔진: 전체 77M 노드 스캔
 TurboLynx: 해당 파티션만 스캔"
```

**Step 3 — "Results"**
```
결과 테이블이 위에서 아래로 stagger 등장:
  name              born        city            population
  Gianluigi Buffon  1978-01-28  Carrara         65,497
  Oliver Kahn       1969-06-15  Karlsruhe       308,436
  Petr Čech         1982-05-20  Plzeň           170,548
  Manuel Neuer      1986-03-27  Gelsenkirchen   260,654
  Iker Casillas     1981-05-20  Madrid          3,223,334

실행 시간 배지: ⚡ 14ms
```

**"Aha!" 박스:**
> "The CGC schema index answered *'which partitions contain position and populationTotal?'* in microseconds.
> 32 of 34 partitions were never touched."

---

## Scene 3 — ⚙️ GEM: Graphlet Execution Model

**목표:** GEM이 조인 순서를 어떻게 최적화하는지 이해

### Steps (4단계)

**Step 0 — "The Join Problem"**
```
파티션 #7 (Goalkeeper nodes) 3개 서브그룹: gl_p1, gl_p2, gl_p3
파티션 #12 (City nodes) 2개 서브그룹: gl_c1, gl_c2

Naive approach: 3 × 2 = 6가지 조인 경우의 수
SVG: 6개의 선이 모두 교차하며 연결된 복잡한 그래프
"⚠️ Plan search space bloating: 6 combinations"
```

**Step 1 — "GEM analyzes join benefit"**
```
각 서브그룹 쌍에 대해 조인 방향 화살표 표시:
  gl_p1 (44,200 rows) → p→c가 유리 (smaller side first)
  gl_p2 (38,100 rows) → p→c가 유리
  gl_c1  (5,700 rows) → c→p가 유리

화살표 색깔로 방향 구분: 파랑 = p→c, 주황 = c→p
```

**Step 2 — "Virtual Graphlet α forms"**
```
SVG: gl_p1, gl_p2, gl_c1 이 하나의 점선 박스로 묶임
  Virtual Graphlet α (파란 테두리)
  └─ gl_p1 (44,200) + gl_p2 (38,100) + gl_c1 (5,700)
  Join order: p→c (Goalkeeper → City)
  Estimated output: 88,100 rows
```

**Step 3 — "Virtual Graphlet β + final result"**
```
SVG: gl_p3, gl_c2 가 두 번째 박스로 묶임
  Virtual Graphlet β (주황 테두리)
  └─ gl_p3 (12,000) + gl_c2 (1,800)
  Join order: c→p

Before:  6 combinations → 6 execution plans
After:   2 Virtual Graphlets → 2 execution plans
Savings: -67% plan search space
```

**"Aha!" 박스:**
> "GEM collapses 6 graphlet combinations into 2 Virtual Graphlets by grouping by join-order preference.
> This is especially powerful with heterogeneous schemas — more graphlets = more combinations = larger savings."

---

## Scene 4 — 🗜️ SSRF: Semistructured Relations on Flat Records

**목표:** NULL 제거 + 메모리 압축이 어떻게 이루어지는지 이해

### Steps (4단계)

**Step 0 — "Without SSRF: wide table, many NULLs"**
```
조인 결과 테이블 (flat 저장):
  name      born        city    pop      type   draftRound  capacity  flag  ...
  Buffon    1978-01-28  Turin   870K    Soccer   NULL        NULL     NULL  (12 more NULLs)
  Kahn      1969-06-15  Munich  1.47M   Soccer   NULL        NULL     NULL

하이라이트: NULL 셀들이 빨간색으로 표시
카운터: 72% of cells = NULL
"CPU가 각 NULL을 분기 처리 → cache miss → 느림"
```

**Step 1 — "SSRF: Validity Vector"**
```
같은 데이터에 Validity Vector 추가:
  name      born        city    pop     type   | validity
  Buffon    1978-01-28  Turin   870K    Soccer | ██ 11001
  Kahn      1969-06-15  Munich  1.47M   Soccer | ██ 11001

"NULL 저장 안 함 — 있는 값만 저장, 비트벡터로 위치 표시"
```

**Step 2 — "Memory comparison"**
```
메모리 바 비교 (애니메이션):
  Standard Schema:  ████████████████████████████  100%  (전체 너비)
  SSRF:             ████████                        28%  (애니메이션으로 줄어듦)

절감: -72% memory usage

"SSRF row들은 연속 메모리에 패킹됨 → SIMD 활용 가능"
```

**Step 3 — "SIMD execution"**
```
시각화: 8개 레인의 SIMD 레지스터
  레지스터에 validity bit이 1인 값들이 연속으로 로드됨
  한 번의 SIMD 명령으로 8개 비교 동시 처리

Before: 10M rows × 15 cols = branch per NULL
After:  2.8M compact rows × 5 common attrs = SIMD batch
```

**"Aha!" 박스:**
> "SSRF removes NULL storage and enables CPU vectorization.
> On the Hero Query (Goalkeeper + City join), memory drops from 100% to 28% and enables full SIMD throughput."

---

## Scene 5 — 🏆 Performance

**목표:** 실제 벤치마크 결과 제시

### Layout

```
상단: 벤치마크 선택 버튼
  [DBpedia Q13] [LDBC SF10] [TPC-H Q3]

중앙: 수평 막대 차트
  (TurboLynx 기준 다른 시스템의 실행 시간 배율)

  DBpedia Q13:
    TurboLynx:  ██  1×
    Neo4j:      ████████████████████████████████  86.14×
    Kuzu:       ████████████  18.88×
    DuckPGQ:    ███████████████  20.23×
    Umbra:      ████████████████  23.07×
    DuckDB:     ███████████████  20.22×

하단: 핵심 수치 카드 3개
  [86× faster than Neo4j] [5,319× faster scan] [28× vs SA baseline]
```

### Interactive
- 벤치마크 전환 시 막대 길이 애니메이션
- hover 시 정확한 수치 tooltip

---

## 구현 파일 구조

```
demo/app/
├── app/
│   ├── page.tsx                    ← Scene navigation root
│   └── globals.css                 ← 다크 테마 변수 유지
├── components/
│   └── scenes/
│       ├── SceneNav.tsx            ← 탭 바 + Prev/Next 버튼
│       ├── S0_Problem.tsx          ← Scene 0: The Problem
│       ├── S1_CGC.tsx              ← Scene 1: CGC (5 steps)
│       ├── S2_Query.tsx            ← Scene 2: Query (4 steps)
│       ├── S3_GEM.tsx              ← Scene 3: GEM (4 steps)
│       ├── S4_SSRF.tsx             ← Scene 4: SSRF (4 steps)
│       └── S5_Performance.tsx      ← Scene 5: Performance
└── lib/
    └── demo-data.ts                ← 모든 데이터 상수 (nodes, schemas, benchmarks)
```

---

## 공통 UI 패턴

### Scene 컨테이너
```tsx
// 전체 viewport, 스크롤 없음
<div style={{ height: "100dvh", display: "flex", flexDirection: "column" }}>
  <SceneNav currentScene={N} totalScenes={6} currentStep={step} totalSteps={M} />
  <div style={{ flex: 1, overflow: "hidden" }}>
    {/* Scene 내용 */}
  </div>
</div>
```

### Step 네비게이션 (Scene 내부)
```tsx
<div className="flex items-center gap-3">
  <button onClick={() => setStep(s => Math.max(0, s-1))}>← Back</button>
  {/* 도트 인디케이터 */}
  {steps.map((_, i) => <div className={i === step ? "filled" : "empty"} />)}
  <button onClick={() => setStep(s => Math.min(MAX, s+1))}>Next →</button>
</div>
```

### "Aha!" 인사이트 박스
```tsx
// 각 Scene 마지막 step에 등장
<motion.div
  initial={{ opacity: 0, y: 16 }}
  animate={{ opacity: 1, y: 0 }}
  className="border border-blue-500/30 bg-blue-500/10 rounded-xl p-4"
>
  <span className="text-blue-400">✓ Key Insight:</span>
  {insight}
</motion.div>
```

### SVG 노드 그래프 (공통)
- viewBox="0 0 720 200"
- 노드: circle r=16, fill=color+"30", stroke=color
- 레이블: text, fontSize=9, fontFamily="monospace"
- 전환: CSS transition on cx/cy (framer-motion layout animation)

---

## 애니메이션 타이밍

| 이벤트 | 애니메이션 |
|--------|-----------|
| Scene 전환 | framer-motion AnimatePresence, slide-left/right, 300ms |
| Step 전환 | fade + slight y-shift, 250ms |
| SVG 노드 이동 | CSS transition: 600ms ease-in-out |
| NULL 카운터 | requestAnimationFrame, 600ms eased |
| 메모리 바 압축 | framer-motion width, 800ms |
| 결과 테이블 rows | stagger 60ms, from opacity:0 y:8 |
| "Aha!" 박스 | delay 200ms after last step content, fade+y |

---

## 데이터 상수 (lib/demo-data.ts)

```typescript
// Scene 0, 1 에서 공유
export const PERSON_NODES = [
  { id: "v0", attrs: { age: 20, FN: "John", LN: "Doe" } },
  { id: "v1", attrs: { FN: "Frank", LN: "Hill" } },
  { id: "v2", attrs: { FN: "Franz" } },
  { id: "v3", attrs: { age: 25, gender: "F", major: "Math" } },
  { id: "v4", attrs: { gender: "M", major: "CS", name: "Mike" } },
  { id: "v5", attrs: { FN: "Sara", LN: "Kim", age: 22 } },
  { id: "v6", attrs: { name: "Alex", major: "Physics", birthday: "95-03" } },
  { id: "v7", attrs: { FN: "Lee", age: 30, url: "dbp.org" } },
  { id: "v8", attrs: { name: "Yuna", age: 28 } },
  { id: "v9", attrs: { FN: "Tom", LN: "Park", gender: "M" } },
];

export const ALL_ATTRS = ["age", "FN", "LN", "gender", "major", "name", "birthday", "url"];

export const GRAPHLETS = [
  { id: "gl₁", color: "#3B82F6", schema: ["age","FN","LN"], nodes: ["v0","v5","v7"] },
  { id: "gl₂", color: "#8B5CF6", schema: ["FN","LN","gender"], nodes: ["v1","v2","v9"] },
  { id: "gl₃", color: "#F59E0B", schema: ["age","gender","major"], nodes: ["v3","v4","v6"] },
  { id: "gl₄", color: "#10B981", schema: ["name","age"], nodes: ["v8"] },
];

// Scene 5
export const BENCHMARKS = {
  dbpedia: {
    label: "DBpedia Q13",
    data: [
      { system: "TurboLynx", ratio: 1,     color: "#e84545" },
      { system: "Neo4j",     ratio: 86.14, color: "#64748b" },
      { system: "Kuzu",      ratio: 18.88, color: "#64748b" },
      { system: "DuckPGQ",   ratio: 20.23, color: "#64748b" },
      { system: "Umbra",     ratio: 23.07, color: "#64748b" },
      { system: "DuckDB",    ratio: 20.22, color: "#64748b" },
    ],
  },
  ldbc: { ... },
  tpch: { ... },
};
```

---

## 구현 순서

1. `lib/demo-data.ts` — 모든 데이터 상수 정의
2. `components/scenes/SceneNav.tsx` — 탭바 + Prev/Next + 도트 인디케이터
3. `app/page.tsx` — Scene router (useState scene + step)
4. `S0_Problem.tsx` — 3 steps, NULL 테이블 + 카운터
5. `S1_CGC.tsx` — 5 steps, SVG 노드 그래프 + 슬라이더
6. `S2_Query.tsx` — 4 steps, dot cloud + plan tree + results table
7. `S3_GEM.tsx` — 4 steps, SVG 조인 그래프 → VG 형성
8. `S4_SSRF.tsx` — 4 steps, NULL 테이블 → 메모리 바
9. `S5_Performance.tsx` — 벤치마크 차트
10. 기존 `components/cockpit/` 및 `components/sections/` 삭제
