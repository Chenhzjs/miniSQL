#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t serialized_size = 0;
  memcpy(buf + serialized_size, &SCHEMA_MAGIC_NUM, sizeof(uint32_t));
  serialized_size += sizeof(uint32_t);
  uint32_t col_size = GetColumnCount();
  memcpy(buf + serialized_size, &col_size, sizeof(uint32_t));
  serialized_size += sizeof(uint32_t);
  auto iter = columns_.begin();
  for (; iter != columns_.end(); iter ++) {
    uint32_t iter_size = (*iter)->SerializeTo(buf + serialized_size);
    serialized_size += iter_size;
  }
  memcpy(buf + serialized_size, &is_manage_, sizeof(bool));
  serialized_size += sizeof(bool);
  return serialized_size;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t serialized_size = 0;
  serialized_size += sizeof(uint32_t);
  serialized_size += sizeof(uint32_t);
  auto iter = columns_.begin();
  for (; iter != columns_.end(); iter ++) {
    uint32_t iter_size = (*iter)->GetSerializedSize();
    serialized_size += iter_size;
  }
  serialized_size += sizeof(bool);
  return serialized_size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // replace with your code here
  uint32_t serialized_size = 0;
  uint32_t magic_num_check;
  uint32_t col_size;
  std::vector<Column *> columns;
  bool is_manage;
  memcpy(&magic_num_check, buf + serialized_size, sizeof(uint32_t));
  serialized_size += sizeof(uint32_t);
  ASSERT(magic_num_check == SCHEMA_MAGIC_NUM, "DeserializeFrom of schema is wrong.");
  memcpy(&col_size, buf + serialized_size, sizeof(uint32_t));
  serialized_size += sizeof(uint32_t);
  for (uint32_t i = 0; i < col_size; i ++) {
    Column *column = nullptr;
    uint32_t i_size = Column::DeserializeFrom(buf + serialized_size, column);
    serialized_size += i_size;
    columns.push_back(column);
  }
  memcpy(buf + serialized_size, &is_manage, sizeof(bool));
  serialized_size += sizeof(bool);
  schema = new Schema(columns, is_manage);
  return serialized_size;
}