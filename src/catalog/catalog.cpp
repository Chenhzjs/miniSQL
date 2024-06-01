#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  return 4 + 4 + 4 +table_meta_pages_.size() * 8 + index_meta_pages_.size() * 8;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
    if (init) {
      catalog_meta_ = CatalogMeta::NewInstance();
      next_table_id_ = 0;
      next_index_id_ = 0;
    } else {
      auto catalog_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
      catalog_meta_ = CatalogMeta::DeserializeFrom(catalog_page->GetData());
      ASSERT(catalog_meta_->CATALOG_METADATA_MAGIC_NUM == CatalogMeta::CATALOG_METADATA_MAGIC_NUM, "The CATALOG_METADATA_MAGIC_NUM is wrong!");
      for (auto iter : catalog_meta_->table_meta_pages_) {
        table_id_t table_id = iter.first;
        page_id_t table_page_id = iter.second;
        auto table_page = buffer_pool_manager_->FetchPage(table_page_id);
        TableMetadata *table_meta_data;
        TableMetadata::DeserializeFrom(table_page->GetData(), table_meta_data);
        std::string table_name = table_meta_data->GetTableName();
        ASSERT(table_id == table_meta_data->GetTableId(), "do not use the iter.first.");
        table_names_.emplace(table_name, table_id);
        TableInfo *table_info = TableInfo::Create();
        TableHeap *table_heap = TableHeap::Create(buffer_pool_manager, table_meta_data->GetFirstPageId(), table_meta_data->GetSchema(), log_manager_, lock_manager_);
        table_info->Init(table_meta_data, table_heap);
        tables_.emplace(table_id, table_info);
        if (table_id >= next_table_id_) {
          next_table_id_ = table_id + 1;
        }
      }
      for (auto iter : catalog_meta_->index_meta_pages_) {
        index_id_t index_id = iter.first;
        page_id_t index_page_id = iter.second;
        auto index_page = buffer_pool_manager_->FetchPage(index_page_id);
        IndexMetadata *index_meta_data = nullptr;
        IndexMetadata::DeserializeFrom(index_page->GetData(), index_meta_data);
        std::string index_name = index_meta_data->GetIndexName();
        table_id_t table_id = index_meta_data->GetTableId();
        ASSERT(index_id == index_meta_data->GetIndexId(), "do not use the iter.first.");
        std::unordered_map<std::string, index_id_t> index_temp_map{{index_name, index_id}};
        index_names_.emplace(tables_[table_id]->GetTableName(), index_temp_map);
        IndexInfo *index_info = IndexInfo::Create();
        TableInfo *table_info = tables_[table_id];
        index_info->Init(index_meta_data, table_info, buffer_pool_manager_);
        indexes_.emplace(index_id, index_info);
        if (index_id >= next_index_id_) {
          next_index_id_ = index_id + 1;
        }
      }
    }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
//  LOG(INFO) << "20";
  delete catalog_meta_;
//  LOG(INFO) << "21";
  for (auto iter : tables_) {
//    LOG(INFO) << iter.first << " " << iter.second->GetTableId() << " " << iter.second->GetTableHeap();
    delete iter.second;
  }
//  LOG(INFO) << "22";
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  if (table_names_.find(table_name) != table_names_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }
  table_names_.emplace(table_name, next_table_id_);
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->table_meta_pages_.emplace(next_table_id_, page_id);
  dberr_t err = FlushCatalogMetaPage();
  if (err != DB_SUCCESS) return err;
  TableMetadata* table_meta_data = TableMetadata::Create(next_table_id_, table_name, page_id, schema);
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_);
  table_meta_data->SerializeTo(page->GetData());
  table_info = TableInfo::Create();
  table_info->Init(table_meta_data, table_heap);
  tables_.emplace(next_table_id_, table_info);
  next_table_id_ ++;
  buffer_pool_manager_->UnpinPage(page_id, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id = table_names_[table_name];
  table_info = tables_[table_id];
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  if (table_names_.empty()) return DB_TABLE_NOT_EXIST;
  for (auto iter: tables_) {
    tables.push_back(iter.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  if (index_names_.find(table_name) != index_names_.end()) {
    auto temp = index_names_.find(table_name)->second;
    if (temp.find(index_name) != temp.end()) {
        return DB_INDEX_ALREADY_EXIST;
    }
  }
  table_id_t table_id = table_names_[table_name];
  std::vector<std::uint32_t> index_keys_index;
  for (auto iter : index_keys) {
    uint32_t index;
    if (tables_[table_id]->GetSchema()->GetColumnIndex(iter, index) != DB_SUCCESS) {
        return DB_COLUMN_NAME_NOT_EXIST;
    }
    index_keys_index.push_back(index);
  }
  page_id_t page_id;
  auto page = buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->index_meta_pages_.emplace(next_index_id_, page_id);
  dberr_t err = FlushCatalogMetaPage();
  if (err != DB_SUCCESS) return err;
  IndexMetadata *index_meta_data;
  index_meta_data = IndexMetadata::Create(next_index_id_, index_name, table_id, index_keys_index);
  index_meta_data->SerializeTo(page->GetData());
  TableInfo *table_info = tables_[table_id];
  index_info = IndexInfo::Create();
  index_info->Init(index_meta_data, table_info, buffer_pool_manager_);
  std::unordered_map<std::string, index_id_t> index_temp_map{{index_name, next_index_id_}};
  index_names_.emplace(table_name, index_temp_map);
  // get the needed column
  TableHeap *table_heap = table_info->GetTableHeap();
  vector<Field> field;
  for (auto iter = table_heap->Begin(txn); iter != table_heap->End(); iter ++) {
    vector<Field> field;
    for (auto i : index_keys_index) {
        field.push_back(*(iter->GetField(i)));
    }
    Row row(field);
    index_info->GetIndex()->InsertEntry(row, iter->GetRowId(), txn);
  }
  indexes_.emplace(next_index_id_, index_info);
  next_index_id_ ++;
  buffer_pool_manager_->UnpinPage(page_id, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if (index_names_.find(table_name) == index_names_.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  auto temp = index_names_.find(table_name)->second;
  if (temp.find(index_name) == temp.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t index_id = temp.find(index_name)->second;
  index_info = indexes_.find(index_id)->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  if (index_names_.find(table_name) == index_names_.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  indexes.clear();
  auto start = index_names_.find(table_name);
  for (auto iter : start->second) {
    auto index_id = iter.second;
    indexes.push_back(indexes_.find(index_id)->second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {

  // delete index
  auto start = index_names_.find(table_name);
  if (start != index_names_.end()) {
    for (auto iter : start->second) {
      auto index_name = iter.first;
      dberr_t s = DropIndex(table_name, index_name);
      if (s != DB_SUCCESS) {
          return s;
      }
    }
  }
  // delete table
  table_id_t table_id = table_names_[table_name];
  table_names_.erase(table_name);
  page_id_t page_id = catalog_meta_->table_meta_pages_[table_id];
  tables_.erase(table_id);
  buffer_pool_manager_->DeletePage(page_id);
  catalog_meta_->table_meta_pages_.erase(table_id);
  dberr_t err = FlushCatalogMetaPage();
  if (err != DB_SUCCESS) return err;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if (index_names_.find(table_name) == index_names_.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  auto temp = index_names_.find(table_name)->second;
  if (temp.find(index_name) == temp.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t index_id = temp.find(index_name)->second;
  temp.erase(index_name);
  indexes_.erase(index_id);
  page_id_t page_id = catalog_meta_->index_meta_pages_[index_id];
  buffer_pool_manager_->DeletePage(page_id);

  catalog_meta_->index_meta_pages_.erase(index_id);
  dberr_t err = FlushCatalogMetaPage();
  if (err != DB_SUCCESS) return err;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  if (tables_.find(table_id) != tables_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }
  auto table_meta_page = buffer_pool_manager_->FetchPage(page_id);
  TableMetadata *table_meta_data;
  TableMetadata::DeserializeFrom(table_meta_page->GetData(), table_meta_data);
  TableInfo *table_info;
  TableHeap *table_heap;
  table_heap = TableHeap::Create(buffer_pool_manager_, page_id, table_meta_data->GetSchema(), log_manager_, lock_manager_);
  table_info = TableInfo::Create();
  table_info->Init(table_meta_data, table_heap);
  table_names_.emplace(table_meta_data->GetTableName(), table_id);
  tables_.emplace(table_id, table_info);
  buffer_pool_manager_->UnpinPage(page_id, true);
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  if (indexes_.find(index_id) != indexes_.end()) {
    return DB_INDEX_ALREADY_EXIST;
  }
  auto index_meta_page = buffer_pool_manager_->FetchPage(page_id);
  IndexMetadata *index_meta_data = nullptr;
  IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta_data);
  std::unordered_map<std::string, index_id_t> index_temp_map{{index_meta_data->GetIndexName(), index_id}};
  table_id_t table_id = index_meta_data->GetTableId();
  auto table_name = tables_.find(table_id)->second->GetTableName();
  if (index_names_.find(table_name) != index_names_.end()) {
    index_names_.find(table_name)->second.emplace(index_meta_data->GetIndexName(), index_id);
  } else {
    index_names_.emplace(table_name, index_temp_map);
  }
  IndexInfo *index_info = IndexInfo::Create();
  index_info->Init(index_meta_data, tables_.find(table_id)->second, buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(page_id, true);
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if (tables_.find(table_id) == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_[table_id];
  return DB_SUCCESS;
}