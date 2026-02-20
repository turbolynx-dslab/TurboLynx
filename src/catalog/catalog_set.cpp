#include "catalog/catalog_set.hpp"
#include "catalog/dependency_manager.hpp"
#include "catalog/catalog.hpp"

#include "common/exception.hpp"
#include "parser/parsed_data/alter_table_info.hpp"
#include "common/string_util.hpp"
#include "parser/column_definition.hpp"
#include "main/client_context.hpp"
#include <sys/fcntl.h>
#include <iostream>

#ifdef ENABLE_SANITIZER_FLAG
	#include <sanitizer/lsan_interface.h>
#endif

#include "icecream.hpp"

namespace duckdb {

//! Class responsible to keep track of state when removing entries from the catalog.
class EntryDropper {
public:
	explicit EntryDropper(CatalogSet &catalog_set, idx_t entry_index)
	    : catalog_set(catalog_set), entry_index(entry_index) {
	}

	~EntryDropper() {
	}

private:
	CatalogSet &catalog_set;
	bool old_deleted;
	idx_t entry_index;
};

CatalogSet::CatalogSet(Catalog &catalog, unique_ptr<DefaultGenerator> defaults)
    : catalog(&catalog), defaults(move(defaults)), current_entry(0) {
	mapping = make_unique<case_insensitive_map_t<MappingValue *>>();
	entries = make_unique<unordered_map<idx_t, CatalogEntry *>>();
}

bool CatalogSet::CreateEntry(ClientContext &context, const string &name, CatalogEntry* value,
                             unordered_set<CatalogEntry *> &dependencies) {
	idx_t entry_index;
	auto mapping_value = GetMapping(context, name);
	if (mapping_value == nullptr || mapping_value->deleted) {
		// entry has never been created: create a dummy deleted entry first
		entry_index = current_entry;
		current_entry = entry_index + 1;

#ifdef ENABLE_SANITIZER_FLAG
		__lsan_disable();
#endif
		auto dummy_node = new CatalogEntry(CatalogType::INVALID, value->catalog, name);
#ifdef ENABLE_SANITIZER_FLAG
		__lsan_enable();
#endif
		dummy_node->timestamp = 0;
		dummy_node->deleted = true;
		dummy_node->set = this;

		entries->insert_or_assign(entry_index, move(dummy_node));
		PutMapping(context, name, entry_index);
	} else {
		return true; // already exists (LoadCatalog case)
	}

	value->timestamp = 0;
	value->set = this;

	value->child = move(entries->at(entry_index));
	value->child->parent = value;
	entries->at(entry_index) = move(value);
	return true;
}

bool CatalogSet::GetEntryInternal(ClientContext &context, idx_t entry_index, CatalogEntry *&catalog_entry) {
	catalog_entry = entries->at(entry_index);
	if (HasConflict(context, catalog_entry->timestamp)) {
		throw TransactionException("Catalog write-write conflict on alter with \"%s\"", catalog_entry->name);
	}
	if (catalog_entry->deleted) {
		return false;
	}
	return true;
}

bool CatalogSet::GetEntryInternal(ClientContext &context, const string &name, idx_t &entry_index,
                                  CatalogEntry *&catalog_entry) {
	auto mapping_value = GetMapping(context, name);
	if (mapping_value == nullptr || mapping_value->deleted) {
		return false;
	}
	entry_index = mapping_value->index;
	return GetEntryInternal(context, entry_index, catalog_entry);
}

bool CatalogSet::AlterEntry(ClientContext &context, const string &name, AlterInfo *alter_info) {
	D_ASSERT(false);
	return true;
}

void CatalogSet::DropEntryDependencies(ClientContext &context, idx_t entry_index, CatalogEntry &entry, bool cascade) {
	EntryDropper dropper(*this, entry_index);
	entries->at(entry_index)->deleted = true;
	entry.catalog->dependency_manager->DropObject(context, &entry, cascade);
}

void CatalogSet::DropEntryInternal(ClientContext &context, idx_t entry_index, CatalogEntry &entry, bool cascade) {
	DropEntryDependencies(context, entry_index, entry, cascade);

	auto value = new CatalogEntry(CatalogType::DELETED_ENTRY, entry.catalog, entry.name);
	value->child = nullptr;
	value->set = this;
	value->deleted = true;
}

bool CatalogSet::DropEntry(ClientContext &context, const string &name, bool cascade) {
	lock_guard<mutex> write_lock(catalog->write_lock);
	idx_t entry_index;
	CatalogEntry *entry;
	if (!GetEntryInternal(context, name, entry_index, entry)) {
		return false;
	}
	if (entry->internal) {
		throw CatalogException("Cannot drop entry \"%s\" because it is an internal system entry", entry->name);
	}

	DropEntryInternal(context, entry_index, *entry, cascade);
	return true;
}

void CatalogSet::CleanupEntry(CatalogEntry *catalog_entry) {
	D_ASSERT(catalog_entry->parent);
	if (catalog_entry->parent->type != CatalogType::UPDATED_ENTRY) {
		lock_guard<mutex> lock(catalog_lock);
		if (!catalog_entry->deleted) {
			catalog_entry->catalog->dependency_manager->EraseObject(catalog_entry);
		}
		auto parent = catalog_entry->parent;
		parent->child = move(catalog_entry->child);
	}
}

bool CatalogSet::HasConflict(ClientContext &context, transaction_t timestamp) {
	return false;
}

MappingValue *CatalogSet::GetMapping(ClientContext &context, const string &name, bool get_latest) {
	MappingValue *mapping_value;

	auto entry = mapping->find(name);
	if (entry != mapping->end()) {
		mapping_value = entry->second;
	} else {
		return nullptr;
	}
	if (get_latest) {
		return mapping_value;
	}
	while (mapping_value->child) {
		if (UseTimestamp(context, mapping_value->timestamp)) {
			break;
		}
		mapping_value = mapping_value->child;
		D_ASSERT(mapping_value);
	}

	return mapping_value;
}

void CatalogSet::PutMapping(ClientContext &context, const string &name, idx_t entry_index) {
	auto entry = mapping->find(name);

#ifdef ENABLE_SANITIZER_FLAG
	__lsan_disable();
#endif
	auto new_value = new MappingValue(entry_index);
#ifdef ENABLE_SANITIZER_FLAG
	__lsan_enable();
#endif
	new_value->timestamp = 0;
	if (entry != mapping->end()) {
		if (HasConflict(context, entry->second->timestamp)) {
			throw TransactionException("Catalog write-write conflict on name \"%s\"", name);
		}
		new_value->child = move(entry->second);
		new_value->child->parent = new_value;
	}
	mapping->insert(std::make_pair(name, move(new_value)));
}

void CatalogSet::DeleteMapping(ClientContext &context, const string &name) {
	D_ASSERT(false);
}

bool CatalogSet::UseTimestamp(ClientContext &context, transaction_t timestamp) {
	return true;
}

CatalogEntry *CatalogSet::GetEntryForTransaction(ClientContext &context, CatalogEntry *current) {
	while (current->child) {
		if (UseTimestamp(context, current->timestamp)) {
			break;
		}
		current = current->child;
		D_ASSERT(current);
	}
	return current;
}

CatalogEntry *CatalogSet::GetCommittedEntry(CatalogEntry *current) {
	return current;
}

pair<string, idx_t> CatalogSet::SimilarEntry(ClientContext &context, const string &name) {
	lock_guard<mutex> lock(catalog_lock);

	string result;
	idx_t current_score = (idx_t)-1;
	for (auto &kv : *mapping) {
		auto mapping_value = GetMapping(context, kv.first);
		if (mapping_value && !mapping_value->deleted) {
			auto ldist = StringUtil::LevenshteinDistance(kv.first, name);
			if (ldist < current_score) {
				current_score = ldist;
				result = kv.first;
			}
		}
	}
	return {result, current_score};
}

CatalogEntry *CatalogSet::CreateEntryInternal(ClientContext &context, unique_ptr<CatalogEntry> entry) {
	auto &name = entry->name;
	auto entry_index = current_entry;
	current_entry = entry_index + 1;
	auto catalog_entry = entry.get();

	entry->timestamp = 0;

	PutMapping(context, name, entry_index);
	return catalog_entry;
}

CatalogEntry *CatalogSet::GetEntry(ClientContext &context, const string &name) {
	auto mapping_value = GetMapping(context, name);
	if (mapping_value != nullptr && !mapping_value->deleted) {
		auto catalog_entry = entries->at(mapping_value->index);
		CatalogEntry *current = GetEntryForTransaction(context, catalog_entry);
		if (current->deleted || (current->name != name && !UseTimestamp(context, mapping_value->timestamp))) {
			return nullptr;
		}
		return current;
	}

	if (!defaults || defaults->created_all_entries) {
		return nullptr;
	}

	auto entry = defaults->CreateDefaultEntry(context, name);
	if (!entry) {
		return nullptr;
	}
	auto result = CreateEntryInternal(context, move(entry));
	if (result) {
		return result;
	}
	return GetEntry(context, name);
}

void CatalogSet::UpdateTimestamp(CatalogEntry *entry, transaction_t timestamp) {
	entry->timestamp = timestamp;
}

void CatalogSet::AdjustEnumDependency(CatalogEntry *entry, ColumnDefinition &column, bool remove) {
	D_ASSERT(false);
}

void CatalogSet::AdjustTableDependencies(CatalogEntry *entry) {
	D_ASSERT(false);
}

void CatalogSet::Undo(CatalogEntry *entry) {
	D_ASSERT(false);
}

void CatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry *)> &callback) {
	unique_lock<mutex> lock(catalog_lock);
	if (defaults && !defaults->created_all_entries) {
		auto default_entries = defaults->GetDefaultEntries();
		for (auto &default_entry : default_entries) {
			auto map_entry = mapping->find(default_entry);
			if (map_entry == mapping->end()) {
				lock.unlock();
				auto entry = defaults->CreateDefaultEntry(context, default_entry);
				lock.lock();
				if (entry) {  // CreateDefaultEntry may return nullptr for deprecated generators
					CreateEntryInternal(context, move(entry));
				}
			}
		}
		defaults->created_all_entries = true;
	}
	for (auto &kv : *entries) {
		auto entry = kv.second;
		entry = GetEntryForTransaction(context, entry);
		if (!entry->deleted) {
			callback(entry);
		}
	}
}

void CatalogSet::Scan(const std::function<void(CatalogEntry *)> &callback) {
	lock_guard<mutex> lock(catalog_lock);
	for (auto &kv : *entries) {
		auto entry = kv.second;
		entry = GetCommittedEntry(entry);
		if (!entry->deleted) {
			callback(entry);
		}
	}
}

} // namespace duckdb
