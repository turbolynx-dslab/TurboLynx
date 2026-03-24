// =============================================================================
// [crud] DeltaStore module-level unit tests
// =============================================================================
// Tests the in-memory mutation buffer components independently:
//   M-1: InsertBuffer
//   M-2: UpdateSegment
//   M-3: DeleteMask
//   M-4: Read Merge (combined)
//   M-5: AdjListDelta
//   M-6: VID allocation (placeholder)
//   M-7: DeltaStore top-level
//   M-8: In-memory ExtentID allocation
// =============================================================================

#include "catch.hpp"
#include "storage/delta_store.hpp"
#include "common/constants.hpp"

using namespace duckdb;

// ============================================================
// M-1: InsertBuffer
// ============================================================

TEST_CASE("M-1-1 InsertBuffer append", "[crud][insert-buffer]") {
    InsertBuffer buf;
    REQUIRE(buf.Empty());
    REQUIRE(buf.Size() == 0);

    buf.AppendRow({Value::BIGINT(1), Value("John")});
    REQUIRE(buf.Size() == 1);
    REQUIRE_FALSE(buf.Empty());
}

TEST_CASE("M-1-2 InsertBuffer multiple appends", "[crud][insert-buffer]") {
    InsertBuffer buf;
    buf.AppendRow({Value::BIGINT(1), Value("John")});
    buf.AppendRow({Value::BIGINT(2), Value("Jane")});
    buf.AppendRow({Value::BIGINT(3), Value("Bob")});
    REQUIRE(buf.Size() == 3);
}

TEST_CASE("M-1-3 InsertBuffer scan order", "[crud][insert-buffer]") {
    InsertBuffer buf;
    buf.AppendRow({Value::BIGINT(10), Value("Alice")});
    buf.AppendRow({Value::BIGINT(20), Value("Bob")});

    auto& rows = buf.GetRows();
    REQUIRE(rows.size() == 2);
    CHECK(rows[0][0].GetValue<int64_t>() == 10);
    CHECK(rows[0][1].GetValue<string>() == "Alice");
    CHECK(rows[1][0].GetValue<int64_t>() == 20);
    CHECK(rows[1][1].GetValue<string>() == "Bob");
}

TEST_CASE("M-1-4 InsertBuffer GetRow", "[crud][insert-buffer]") {
    InsertBuffer buf;
    buf.AppendRow({Value::BIGINT(42), Value("Test")});

    auto& row = buf.GetRow(0);
    CHECK(row[0].GetValue<int64_t>() == 42);
    CHECK(row[1].GetValue<string>() == "Test");
}

TEST_CASE("M-1-5 InsertBuffer clear", "[crud][insert-buffer]") {
    InsertBuffer buf;
    buf.AppendRow({Value::BIGINT(1), Value("X")});
    buf.AppendRow({Value::BIGINT(2), Value("Y")});
    REQUIRE(buf.Size() == 2);

    buf.Clear();
    REQUIRE(buf.Size() == 0);
    REQUIRE(buf.Empty());
}

// ============================================================
// M-2: UpdateSegment
// ============================================================

TEST_CASE("M-2-1 UpdateSegment basic set/get", "[crud][update-segment]") {
    UpdateSegment seg;
    REQUIRE(seg.Empty());

    seg.Set(5, 0, Value("Jane"));
    REQUIRE(seg.Size() == 1);

    auto* val = seg.Get(5, 0);
    REQUIRE(val != nullptr);
    CHECK(val->GetValue<string>() == "Jane");
}

TEST_CASE("M-2-2 UpdateSegment overwrite same row", "[crud][update-segment]") {
    UpdateSegment seg;
    seg.Set(3, 0, Value("First"));
    seg.Set(3, 0, Value("Second"));

    auto* val = seg.Get(3, 0);
    REQUIRE(val != nullptr);
    CHECK(val->GetValue<string>() == "Second");
    CHECK(seg.Size() == 1);  // still 1 row
}

TEST_CASE("M-2-3 UpdateSegment multiple properties", "[crud][update-segment]") {
    UpdateSegment seg;
    seg.Set(0, 1, Value("John"));    // prop 1 = name
    seg.Set(0, 2, Value::BIGINT(25)); // prop 2 = age

    auto* name = seg.Get(0, 1);
    auto* age = seg.Get(0, 2);
    REQUIRE(name != nullptr);
    REQUIRE(age != nullptr);
    CHECK(name->GetValue<string>() == "John");
    CHECK(age->GetValue<int64_t>() == 25);
}

TEST_CASE("M-2-4 UpdateSegment miss returns nullptr", "[crud][update-segment]") {
    UpdateSegment seg;
    seg.Set(5, 0, Value("X"));

    CHECK(seg.Get(5, 1) == nullptr);  // wrong prop
    CHECK(seg.Get(6, 0) == nullptr);  // wrong row
    CHECK(seg.Get(99, 99) == nullptr); // nothing
}

TEST_CASE("M-2-5 UpdateSegment HasUpdate", "[crud][update-segment]") {
    UpdateSegment seg;
    CHECK_FALSE(seg.HasUpdate(0));

    seg.Set(0, 1, Value("X"));
    CHECK(seg.HasUpdate(0));
    CHECK_FALSE(seg.HasUpdate(1));
}

TEST_CASE("M-2-6 UpdateSegment clear", "[crud][update-segment]") {
    UpdateSegment seg;
    seg.Set(0, 0, Value("A"));
    seg.Set(1, 0, Value("B"));
    REQUIRE(seg.Size() == 2);

    seg.Clear();
    REQUIRE(seg.Empty());
    CHECK(seg.Get(0, 0) == nullptr);
}

TEST_CASE("M-2-7 UpdateSegment ForEach", "[crud][update-segment]") {
    UpdateSegment seg;
    seg.Set(0, 1, Value::BIGINT(10));
    seg.Set(2, 3, Value::BIGINT(20));

    int count = 0;
    int64_t sum = 0;
    seg.ForEach([&](idx_t offset, idx_t prop_key, const Value& val) {
        count++;
        sum += val.GetValue<int64_t>();
    });
    CHECK(count == 2);
    CHECK(sum == 30);
}

// ============================================================
// M-3: DeleteMask
// ============================================================

TEST_CASE("M-3-1 DeleteMask empty", "[crud][delete-mask]") {
    DeleteMask mask;
    REQUIRE(mask.Empty());
    CHECK_FALSE(mask.IsDeleted(0));
    CHECK_FALSE(mask.IsDeleted(100));
}

TEST_CASE("M-3-2 DeleteMask delete and check", "[crud][delete-mask]") {
    DeleteMask mask;
    mask.Delete(5);
    CHECK(mask.IsDeleted(5));
    CHECK_FALSE(mask.IsDeleted(4));
    CHECK_FALSE(mask.IsDeleted(6));
    CHECK(mask.Count() == 1);
}

TEST_CASE("M-3-3 DeleteMask idempotent", "[crud][delete-mask]") {
    DeleteMask mask;
    mask.Delete(3);
    mask.Delete(3);  // same row again
    CHECK(mask.IsDeleted(3));
    CHECK(mask.Count() == 1);  // still 1
}

TEST_CASE("M-3-4 DeleteMask multiple deletes", "[crud][delete-mask]") {
    DeleteMask mask;
    mask.Delete(0);
    mask.Delete(5);
    mask.Delete(10);
    CHECK(mask.Count() == 3);
    CHECK(mask.IsDeleted(0));
    CHECK(mask.IsDeleted(5));
    CHECK(mask.IsDeleted(10));
    CHECK_FALSE(mask.IsDeleted(1));
}

TEST_CASE("M-3-5 DeleteMask clear", "[crud][delete-mask]") {
    DeleteMask mask;
    mask.Delete(1);
    mask.Delete(2);
    mask.Delete(3);
    REQUIRE(mask.Count() == 3);

    mask.Clear();
    CHECK(mask.Empty());
    CHECK_FALSE(mask.IsDeleted(1));
    CHECK_FALSE(mask.IsDeleted(2));
}

// ============================================================
// M-4: Read Merge (combined scenario)
// ============================================================

TEST_CASE("M-4-1 empty delta has no effect", "[crud][read-merge]") {
    UpdateSegment seg;
    DeleteMask mask;
    InsertBuffer buf;

    // Simulate base data: row 0 = {id:1, name:"John"}
    // With empty delta, all base rows should be returned as-is
    CHECK(seg.Empty());
    CHECK(mask.Empty());
    CHECK(buf.Empty());

    // No update → Get returns nullptr → use base value
    CHECK(seg.Get(0, 0) == nullptr);
    // Not deleted → row is valid
    CHECK_FALSE(mask.IsDeleted(0));
}

TEST_CASE("M-4-2 update overrides base value", "[crud][read-merge]") {
    UpdateSegment seg;
    // Base: row 3, prop 0 = "John"
    // Delta: row 3, prop 0 = "Jane"
    seg.Set(3, 0, Value("Jane"));

    auto* val = seg.Get(3, 0);
    REQUIRE(val != nullptr);
    CHECK(val->GetValue<string>() == "Jane");

    // Other rows unaffected
    CHECK(seg.Get(0, 0) == nullptr);
    CHECK(seg.Get(4, 0) == nullptr);
}

TEST_CASE("M-4-3 delete masks base row", "[crud][read-merge]") {
    DeleteMask mask;
    // Base has rows 0-9. Delete row 5.
    mask.Delete(5);

    // Simulated scan: iterate rows 0-9, skip deleted
    int visible_count = 0;
    for (idx_t i = 0; i < 10; i++) {
        if (!mask.IsDeleted(i)) visible_count++;
    }
    CHECK(visible_count == 9);
}

TEST_CASE("M-4-4 insert buffer adds new rows", "[crud][read-merge]") {
    InsertBuffer buf;
    // Base has 5 rows (0-4). Insert 2 more.
    buf.AppendRow({Value::BIGINT(100), Value("New1")});
    buf.AppendRow({Value::BIGINT(200), Value("New2")});

    // Total visible = base(5) + insert(2) = 7
    idx_t base_count = 5;
    idx_t total = base_count + buf.Size();
    CHECK(total == 7);
}

TEST_CASE("M-4-5 combined update+delete+insert", "[crud][read-merge]") {
    UpdateSegment seg;
    DeleteMask mask;
    InsertBuffer buf;

    // Base: 5 rows (0-4)
    // Update row 1
    seg.Set(1, 0, Value("Updated"));
    // Delete row 3
    mask.Delete(3);
    // Insert 1 new row
    buf.AppendRow({Value::BIGINT(999), Value("NewRow")});

    // Scan simulation
    idx_t base_count = 5;
    int visible_base = 0;
    bool row1_updated = false;

    for (idx_t i = 0; i < base_count; i++) {
        if (mask.IsDeleted(i)) continue;
        visible_base++;
        auto* upd = seg.Get(i, 0);
        if (upd && i == 1) {
            row1_updated = (upd->GetValue<string>() == "Updated");
        }
    }

    CHECK(visible_base == 4);       // 5 - 1 deleted
    CHECK(row1_updated == true);
    CHECK(buf.Size() == 1);         // 1 inserted
    // Total visible = 4 + 1 = 5
    CHECK(visible_base + (int)buf.Size() == 5);
}

// ============================================================
// M-5: AdjListDelta
// ============================================================

TEST_CASE("M-5-1 AdjListDelta insert edge", "[crud][adjlist-delta]") {
    AdjListDelta delta;
    REQUIRE(delta.Empty());

    delta.InsertEdge(100, 200, 1001);  // src=100 → dst=200, edge_id=1001

    auto* edges = delta.GetInserted(100);
    REQUIRE(edges != nullptr);
    REQUIRE(edges->size() == 1);
    CHECK((*edges)[0].dst_vid == 200);
    CHECK((*edges)[0].edge_id == 1001);
    CHECK(delta.InsertedCount() == 1);
}

TEST_CASE("M-5-2 AdjListDelta delete edge", "[crud][adjlist-delta]") {
    AdjListDelta delta;
    delta.DeleteEdge(100, 1001);  // src=100, edge_id=1001

    CHECK(delta.IsEdgeDeleted(100, 1001));
    CHECK_FALSE(delta.IsEdgeDeleted(100, 1002));
    CHECK_FALSE(delta.IsEdgeDeleted(200, 1001));
    CHECK(delta.DeletedCount() == 1);
}

TEST_CASE("M-5-3 AdjListDelta insert then delete", "[crud][adjlist-delta]") {
    AdjListDelta delta;
    delta.InsertEdge(100, 200, 5000);
    delta.DeleteEdge(100, 5000);

    // Edge was inserted then deleted → should not be visible
    auto* edges = delta.GetInserted(100);
    REQUIRE(edges != nullptr);
    // Insert list still has it (delta doesn't remove from insert list)
    // But delete check should filter it out during scan
    CHECK(delta.IsEdgeDeleted(100, 5000));
}

TEST_CASE("M-5-4 AdjListDelta multiple edges per src", "[crud][adjlist-delta]") {
    AdjListDelta delta;
    delta.InsertEdge(100, 200, 1);
    delta.InsertEdge(100, 300, 2);
    delta.InsertEdge(100, 400, 3);
    delta.DeleteEdge(100, 2);  // delete edge to 300

    auto* edges = delta.GetInserted(100);
    REQUIRE(edges != nullptr);
    CHECK(edges->size() == 3);  // all 3 in insert list

    // Scan: filter out deleted
    int visible = 0;
    for (auto& e : *edges) {
        if (!delta.IsEdgeDeleted(100, e.edge_id)) visible++;
    }
    CHECK(visible == 2);  // 3 - 1 deleted
}

TEST_CASE("M-5-5 AdjListDelta no entries for unknown src", "[crud][adjlist-delta]") {
    AdjListDelta delta;
    CHECK(delta.GetInserted(999) == nullptr);
    CHECK_FALSE(delta.IsEdgeDeleted(999, 1));
}

TEST_CASE("M-5-6 AdjListDelta clear", "[crud][adjlist-delta]") {
    AdjListDelta delta;
    delta.InsertEdge(1, 2, 100);
    delta.DeleteEdge(3, 200);
    REQUIRE_FALSE(delta.Empty());

    delta.Clear();
    CHECK(delta.Empty());
    CHECK(delta.InsertedCount() == 0);
    CHECK(delta.DeletedCount() == 0);
}

// ============================================================
// M-6: VID allocation (placeholder — depends on catalog)
// ============================================================

TEST_CASE("M-6-1 VID structure", "[crud][vid]") {
    // VID = [partition_id:16][extent_id:16][offset:32]
    uint64_t partition_id = 3;
    uint64_t extent_id = 7;
    uint64_t offset = 42;

    uint64_t vid = (partition_id << 48) | (extent_id << 32) | offset;

    CHECK((uint16_t)(vid >> 48) == 3);
    CHECK((uint16_t)((vid >> 32) & 0xFFFF) == 7);
    CHECK((uint32_t)(vid & 0xFFFFFFFF) == 42);
}

// ============================================================
// M-7: DeltaStore top-level container
// ============================================================

TEST_CASE("M-7-1 DeltaStore empty on creation", "[crud][delta-store]") {
    DeltaStore store;
    CHECK(store.Empty());
}

TEST_CASE("M-7-2 DeltaStore get components by ID", "[crud][delta-store]") {
    DeltaStore store;

    // Access creates on demand
    auto& seg = store.GetUpdateSegment(0);
    auto& mask = store.GetDeleteMask(0);
    // Use an in-memory ExtentID for insert buffer (partition 1 -> ExtentID 0x0001FF00)
    uint32_t inmem_eid = store.AllocateInMemoryExtentID(1);
    auto& buf = store.GetInsertBuffer(inmem_eid);
    auto& adj = store.GetAdjListDelta(1);

    seg.Set(0, 0, Value("test"));
    mask.Delete(5);
    buf.AppendRow({Value::BIGINT(1)});
    adj.InsertEdge(10, 20, 100);

    CHECK_FALSE(store.Empty());
    CHECK(seg.Size() == 1);
    CHECK(mask.Count() == 1);
    CHECK(buf.Size() == 1);
    CHECK(adj.InsertedCount() == 1);
}

TEST_CASE("M-7-3 DeltaStore isolation between extents", "[crud][delta-store]") {
    DeltaStore store;

    store.GetUpdateSegment(0).Set(0, 0, Value("ext0"));
    store.GetUpdateSegment(1).Set(0, 0, Value("ext1"));

    CHECK(store.GetUpdateSegment(0).Get(0, 0)->GetValue<string>() == "ext0");
    CHECK(store.GetUpdateSegment(1).Get(0, 0)->GetValue<string>() == "ext1");
}

TEST_CASE("M-7-4 DeltaStore clear all", "[crud][delta-store]") {
    DeltaStore store;

    store.GetUpdateSegment(0).Set(0, 0, Value("x"));
    store.GetDeleteMask(0).Delete(1);
    uint32_t eid = store.AllocateInMemoryExtentID(0);
    store.GetInsertBuffer(eid).AppendRow({Value::BIGINT(1)});
    store.GetAdjListDelta(0).InsertEdge(1, 2, 3);
    REQUIRE_FALSE(store.Empty());

    store.Clear();
    CHECK(store.Empty());
}

// ============================================================
// M-8: In-memory ExtentID allocation
// ============================================================

TEST_CASE("M-8-1 IsInMemoryExtent", "[crud][inmem-extent]") {
    // Normal extent: partition 0, local extent 0
    CHECK_FALSE(IsInMemoryExtent(0x00000000));
    // Normal extent: partition 5, local extent 100
    CHECK_FALSE(IsInMemoryExtent(0x00050064));
    // In-memory extent: partition 0, local extent 0xFF00
    CHECK(IsInMemoryExtent(0x0000FF00));
    // In-memory extent: partition 3, local extent 0xFFFF
    CHECK(IsInMemoryExtent(0x0003FFFF));
    // Edge case: local extent 0xFEFF is NOT in-memory
    CHECK_FALSE(IsInMemoryExtent(0x0000FEFF));
    // Edge case: local extent 0xFF00 IS in-memory
    CHECK(IsInMemoryExtent(0x0000FF00));
}

TEST_CASE("M-8-2 AllocateInMemoryExtentID", "[crud][inmem-extent]") {
    DeltaStore store;

    // Allocate first in-memory extent for partition 5
    uint32_t eid1 = store.AllocateInMemoryExtentID(5);
    CHECK(IsInMemoryExtent(eid1));
    CHECK((eid1 >> 16) == 5);  // upper 16 bits = partition ID
    CHECK((eid1 & 0xFFFF) == 0xFF00);  // first slot

    // Allocate second in-memory extent for same partition
    uint32_t eid2 = store.AllocateInMemoryExtentID(5);
    CHECK(IsInMemoryExtent(eid2));
    CHECK((eid2 >> 16) == 5);
    CHECK((eid2 & 0xFFFF) == 0xFF01);  // second slot
    CHECK(eid1 != eid2);

    // Different partition gets independent allocation
    uint32_t eid3 = store.AllocateInMemoryExtentID(10);
    CHECK(IsInMemoryExtent(eid3));
    CHECK((eid3 >> 16) == 10);
    CHECK((eid3 & 0xFFFF) == 0xFF00);  // first slot for partition 10
}

TEST_CASE("M-8-3 GetInMemoryExtentIDs", "[crud][inmem-extent]") {
    DeltaStore store;

    uint32_t eid1 = store.AllocateInMemoryExtentID(7);
    store.GetInsertBuffer(eid1).AppendRow({Value::BIGINT(1)});

    uint32_t eid2 = store.AllocateInMemoryExtentID(7);
    store.GetInsertBuffer(eid2).AppendRow({Value::BIGINT(2)});

    auto ids = store.GetInMemoryExtentIDs(7);
    CHECK(ids.size() == 2);

    // Different partition has none
    auto ids_other = store.GetInMemoryExtentIDs(8);
    CHECK(ids_other.size() == 0);
}

TEST_CASE("M-8-4 InsertBuffer keyed by in-memory ExtentID", "[crud][inmem-extent]") {
    DeltaStore store;

    uint32_t eid = store.AllocateInMemoryExtentID(3);
    store.GetInsertBuffer(eid).AppendRow(
        vector<string>{"name", "age"},
        vector<Value>{Value("Alice"), Value::BIGINT(30)});
    store.GetInsertBuffer(eid).AppendRow(
        vector<string>{"name", "age"},
        vector<Value>{Value("Bob"), Value::BIGINT(25)});

    auto* buf = store.FindInsertBuffer(eid);
    REQUIRE(buf != nullptr);
    CHECK(buf->Size() == 2);
    CHECK(buf->GetRow(0)[0].GetValue<string>() == "Alice");
    CHECK(buf->GetRow(1)[0].GetValue<string>() == "Bob");

    // FindInsertBufferByPartition also works
    auto* buf_by_part = store.FindInsertBufferByPartition(3);
    REQUIRE(buf_by_part != nullptr);
    CHECK(buf_by_part->Size() == 2);
}
