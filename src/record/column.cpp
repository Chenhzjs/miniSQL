#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t serialized_size = 0;
  memcpy(buf + serialized_size, &COLUMN_MAGIC_NUM, sizeof(uint32_t));
  serialized_size += sizeof(uint32_t);
  uint32_t name_size = name_.length();
  memcpy(buf + serialized_size, &name_size, sizeof(uint32_t));
  serialized_size += sizeof(uint32_t);
  const char *name_char = name_.c_str();
  memcpy(buf + serialized_size, name_char, sizeof(char) * name_size);
  serialized_size += sizeof(char) * name_size;
  memcpy(buf + serialized_size, &type_, sizeof(TypeId));
  serialized_size += sizeof(TypeId);
  memcpy(buf + serialized_size, &len_, sizeof(uint32_t));
  serialized_size += sizeof(uint32_t);
  memcpy(buf + serialized_size, &table_ind_, sizeof(uint32_t));
  serialized_size += sizeof(uint32_t);
  memcpy(buf + serialized_size, &nullable_, sizeof(bool));
  serialized_size += sizeof(bool);
  memcpy(buf + serialized_size, &unique_, sizeof(bool));
  serialized_size += sizeof(bool);
  return serialized_size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  uint32_t serialized_size = 0;
  serialized_size += sizeof(uint32_t);
  serialized_size += sizeof(uint32_t);
  serialized_size += sizeof(char) * name_.length();
  serialized_size += sizeof(TypeId);
  serialized_size += sizeof(uint32_t);
  serialized_size += sizeof(uint32_t);
  serialized_size += sizeof(bool);
  serialized_size += sizeof(bool);
  return serialized_size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  // replace with your code here
  if (column != nullptr) {
    LOG(WARNING) << "Pointer to column is not null in column deserialize." 									 << std::endl;
  }
  uint32_t magic_num_check;
  uint32_t name_size;
  TypeId type;
  uint32_t len;
  uint32_t table_ind;
  bool nullable;
  bool unique;
  uint32_t serialized_size = 0;
  memcpy(&magic_num_check, buf + serialized_size, sizeof(uint32_t));
  serialized_size += sizeof(uint32_t);
  ASSERT(magic_num_check == COLUMN_MAGIC_NUM, "DeserializeFrom of column is wrong.");
  memcpy(&name_size, buf + serialized_size, sizeof(uint32_t));
  serialized_size += sizeof(uint32_t);
  char *name_char = new char[name_size + 1];
  memcpy(name_char, buf + serialized_size, sizeof(char) * name_size);
  serialized_size += sizeof(char) * name_size;
  name_char[name_size] = '\0';
  std::string name(name_char, name_size);
  memcpy(&type, buf + serialized_size, sizeof(TypeId));
  serialized_size += sizeof(TypeId);
  memcpy(&len, buf + serialized_size, sizeof(uint32_t));
  serialized_size += sizeof(uint32_t);
  memcpy(&table_ind, buf + serialized_size, sizeof(uint32_t));
  serialized_size += sizeof(uint32_t);
  memcpy(&nullable, buf + serialized_size, sizeof(bool));
  serialized_size += sizeof(bool);
  memcpy(&unique, buf + serialized_size, sizeof(bool));
  serialized_size += sizeof(bool);
  if (type == kTypeChar) {
    column = new Column(name, type, len, table_ind, nullable, unique);
  } else {
    column = new Column(name, type, table_ind, nullable, unique);
  }
  return serialized_size;
}
