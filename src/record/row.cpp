#include "record/row.h"
#include <cmath>
/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t field_count = GetFieldCount();
  uint32_t serialized_size = 0;
  memcpy(buf + serialized_size, &field_count, sizeof(uint32_t));
  serialized_size += sizeof(uint32_t);
  if (field_count == 0) return serialized_size;
  uint32_t bitmap_size = (field_count + 7) / 8;
  char *null_bitmap = new char[bitmap_size];
  memset(null_bitmap, 0, sizeof(char) * bitmap_size);
  auto iter = fields_.begin();
  int pos = 0;
  for (; iter != fields_.end(); iter ++) {
    if (!((*iter)->IsNull())) {
      null_bitmap[pos / 8] += pow(2, pos % 8);
    }
    pos ++;
  }
  memcpy(buf + serialized_size, null_bitmap, sizeof(char) * bitmap_size);
  serialized_size += sizeof(char) * bitmap_size;
  iter = fields_.begin();
  for (; iter != fields_.end(); iter ++) {
    int field_size = (*iter)->SerializeTo(buf + serialized_size);
    serialized_size += field_size;
  }
  delete [] null_bitmap;
  return serialized_size;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  fields_.clear();
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t field_count;
  uint32_t serialized_size = 0;
  memcpy(&field_count, buf + serialized_size, sizeof(uint32_t));
  serialized_size += sizeof(uint32_t);
  if (field_count == 0) return serialized_size;
  uint32_t bitmap_size = (field_count + 7) / 8;
  char *null_bitmap = new char[bitmap_size];
  memcpy(null_bitmap, buf + serialized_size, sizeof(char) * bitmap_size);
  serialized_size += bitmap_size;
  for (int i = 0; i < field_count; i ++) {
    if ((null_bitmap[i / 8] >> i) % 2 != 0) {
      auto type = schema->GetColumn(i)->GetType();
      switch (type) {
        case kTypeInt:
          int32_t field_int;
          memcpy(&field_int, buf + serialized_size, sizeof(int32_t));
          serialized_size += sizeof(int32_t);
          fields_.push_back(new Field(type, field_int));
          break;
        case kTypeFloat:
          float field_float;
          memcpy(&field_float, buf + serialized_size, sizeof(float));
          serialized_size += sizeof(float);
          fields_.push_back(new Field(type, field_float));
          break;
        case kTypeChar:
          uint32_t len;
          memcpy(&len, buf + serialized_size, sizeof(uint32_t));
          serialized_size += sizeof(uint32_t);
          char* field_char = new char[len];
          memcpy(field_char, buf + serialized_size, sizeof(char) * len);
          serialized_size += sizeof(char) * len;
          fields_.push_back(new Field(type, field_char, len, true));
          delete [] field_char;
          break;
      }
    } else {
      auto type = schema->GetColumn(i)->GetType();
      fields_.push_back(new Field(type));
    }
  }
  delete [] null_bitmap;
  return serialized_size;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t field_count;
  uint32_t serialized_size = 0;
  field_count = GetFieldCount();
  serialized_size += sizeof(uint32_t);
  if (field_count == 0) return serialized_size;
  uint32_t bitmap_size = (field_count + 7) / 8;
  serialized_size += bitmap_size;
  auto iter = fields_.begin();
  int field_tot = 0;
  for (; iter != fields_.end(); iter ++) {
    int field_iter = (*iter)->GetSerializedSize();
    field_tot += field_iter;
  }
  serialized_size += field_tot;
  return serialized_size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
