#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>
#include <cstdio>
#include "utils/tree_file_mgr.h"
#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"
#include "parser/minisql_yacc.h"
#include <time.h>

extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
   *
   */
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   /**/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
//      cout << "SeqScan" << endl;
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
//      cout << "IndexScan" << endl;
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
//      cout << "Values" << endl;
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
//  cout << "here0" << endl;
  auto executor = CreateExecutor(exec_ctx, plan);
//  cout << "here1" << endl;
  try {
    executor->Init();
//    cout << "here2" << endl;
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
//      cout << "here3" << endl;
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
//    cout << "here4" << endl;
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  cout << "Database " << db_name << " is created successfully." << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  cout << "Database " << db_name << " is dropped successfully." << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed to " << db_name << "." << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
// create table t1(a int, b char(20) unique, c float, primary key(a, c));
/*
 * 14[label="kNodeCreateTable(1,69)\nid(14)"]
 * |
 * 0[label="kNodeIdentifier(1,15)\nid(0),val(t1)"] -> 15[label="kNodeColumnDefinitionList(1,69)\nid(15)"]
 *                                                    |
 *                                                    3[label="kNodeColumnDefinition(1,22)\nid(3)"] ------------------------------------------------------> 7[label="kNodeColumnDefinition(1,40)\nid(7),val(unique)"] -------------------------------------------> 10[label="kNodeColumnDefinition(1,50)\nid(10)"] -----------------------------------------------------> 13[label="kNodeColumnList(1,68)\nid(13),val(primary keys)"]
 *                                                    |                                                                                                     |                                                                                                      |                                                                                                      |
 *                                                    1[label="kNodeIdentifier(1,17)\nid(1),val(a)"] -> 2[label="kNodeColumnType(1,21)\nid(2),val(int)"]    4[label="kNodeIdentifier(1,24)\nid(4),val(b)"] -> 6[label="kNodeColumnType(1,33)\nid(6),val(char)"]    8[label="kNodeIdentifier(1,43)\nid(8),val(c)"] -> [label="kNodeColumnType(1,49)\nid(9),val(float)"]    11[label="kNodeIdentifier(1,64)\nid(11),val(a)"] -> 12[label="kNodeIdentifier(1,67)\nid(12),val(c)"]
 *                                                                                                                                                                                                            |
 *                                                                                                                                                                                                            5[label="kNodeNumber(1,32)\nid(5),val(20)"]
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (current_db_ == "") {
    LOG(ERROR) << "Choose a Database first." << std::endl;
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  DBStorageEngine *current_db = dbs_[current_db_];
  // check if the table is already exist
  vector<TableInfo *> tables;
  current_db->catalog_mgr_->GetTables(tables);
  for (auto iter : tables) {
    if (iter->GetTableName() == table_name) {
      return DB_TABLE_ALREADY_EXIST;
    }
  }
  pSyntaxNode column_list = ast->child_->next_; // Node 15
  pSyntaxNode column_start = column_list->child_; // Node 3
  pSyntaxNode column_iter = column_start; // Node 3
  uint32_t index = 0;
  // find out primary key
  vector<string > primary_keys;
  primary_keys.clear();
  while (column_iter != nullptr) {
    if (column_iter->val_ != nullptr && string(column_iter->val_) == "primary keys") {
      pSyntaxNode primary_key = column_iter->child_;
      while (primary_key != nullptr) {
        primary_keys.emplace_back(primary_key->val_);
        primary_key = primary_key->next_;
      }
    }
    column_iter = column_iter->next_;
  }
  // get column information
  /*
   * Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique);
   * Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique);
   *
   */
//  cout << "primary_keys_size: " << primary_keys.size() << endl;
  std::vector<Column *> columns;
  columns.clear();
  column_iter = column_start; // Node 3
  while (column_iter != nullptr) {
    bool unique = false;
    bool nullable = true;
    if (column_iter->val_ != nullptr && string(column_iter->val_) == "primary keys") {
      column_iter = column_iter->next_;
      continue;
    }
    if (column_iter->val_ != nullptr && string(column_iter->val_) == "unique") {
      unique = true;
    }
    pSyntaxNode column_info = column_iter->child_;
    if (find(primary_keys.begin(), primary_keys.end(), string(column_info->val_)) != primary_keys.end()) {
//      cout << column_info->val_ << " is one of primary key." << endl;
      unique = true;
      nullable = false;
    }
    pSyntaxNode column_type = column_info->next_;
//    cout << "column_type: " << column_type->val_ << endl;
    if (string(column_type->val_) == "int") {
      Column *column = new Column(column_info->val_, kTypeInt, index ++, nullable, unique);
      columns.push_back(column);
    } else if (string(column_type->val_) == "float") {
      Column *column = new Column(column_info->val_, kTypeFloat, index ++, nullable, unique);
      columns.push_back(column);
    } else if (string(column_type->val_) == "char") {
      pSyntaxNode column_char_length = column_type->child_;
      float length_f = stof(column_char_length->val_);
      int length = stoi(column_char_length->val_);
      if (length_f < 0 || static_cast<float>(length) != length_f) {
        if (length_f < 0) {
          LOG(ERROR) << "the length of char type cannot be smaller than 0";
          columns.clear();
          return DB_FAILED;
        } else {
          LOG(ERROR) << "the length of char type cannot be a float";
          columns.clear();
          return DB_FAILED;
        }
      }
      Column *column = new Column(column_info->val_, kTypeChar, length, index ++, nullable, unique);
      columns.push_back(column);
    }
    column_iter = column_iter->next_;
  }
  Schema *schema = new Schema(columns);
//  cout << "column's count: " << schema->GetColumnCount() << endl;
  Txn txn;
  TableInfo *tableInfo = nullptr;
  current_db->catalog_mgr_->CreateTable(table_name, schema, &txn, tableInfo);
  // create index for unique column
  IndexInfo * index_info = nullptr;
  std::vector<string > index_keys;
  index_keys.clear();
//  cout << "here 0" << endl;
  for (auto primary_key : primary_keys) {
    index_keys.emplace_back(primary_key);
  }
  current_db->catalog_mgr_->CreateIndex(table_name, "PRIMARY_KEY", index_keys, &txn, index_info, "bptree");
  for (auto iter : columns) {
    if(iter->IsUnique()){
      if (find(primary_keys.begin(), primary_keys.end(), string(iter->GetName())) != primary_keys.end()) {
//        cout << iter->GetName() << " is primary key" << endl;
        continue;
      }
//      cout << iter->GetName() << " is unique" << endl;
      index_info = nullptr;
      index_keys.clear();
      index_keys.emplace_back(iter->GetName());
      current_db->catalog_mgr_->CreateIndex(table_name, "index_" + iter->GetName(), index_keys, &txn, index_info, "bptree");
    }
  }
//  vector<IndexInfo *> indexes;
//  cout << "here 1" << endl;
//  current_db->catalog_mgr_->GetTableIndexes(table_name, indexes);
//  cout << "here 2" << endl;
//  cout << "indexes_size: " << indexes.size() << endl;
  cout << "Table " << table_name << " is created successfully." << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */

/*
 * 1[label="kNodeDropTable(1,13)\nid(1)"]
 * |
 * 0[label="kNodeIdentifier(1,13)\nid(0),val(t1)"]
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (current_db_ == "") {
    LOG(ERROR) << "Choose a Database first." << std::endl;
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  DBStorageEngine *current_db = dbs_[current_db_];
  vector<TableInfo *> tables;
  current_db->catalog_mgr_->GetTables(tables);
  bool flag = false;
  for (auto iter : tables) {
    if (iter->GetTableName() == table_name) {
      flag = true;
      break;
    }
  }
  if (!flag) {
    return DB_TABLE_NOT_EXIST;
  }
  current_db->catalog_mgr_->DropTable(table_name);
  cout << "Table " << table_name << " is dropped successfully." << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
 /*
  * 0[label="kNodeShowIndexes(1,12)\nid(0)"]
  * */

/* like this
 * +---------+------------+--------------+-------------+------------+
 * | Table   | Key_name   | Seq_in_index | Column_name | Index_type |
 * +---------+------------+--------------+-------------+------------+
 * | table_1 | index_name |            1 | name        | BTREE      |
 * | table_1 | index_id   |            1 | id          | BTREE      |
 * +---------+------------+--------------+-------------+------------+
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (current_db_ == "") {
    LOG(ERROR) << "Choose a Database first." << std::endl;
    return DB_FAILED;
  }
  DBStorageEngine *current_db = dbs_[current_db_];
  // check if the table is already exist
  vector<TableInfo *> tables;
  current_db->catalog_mgr_->GetTables(tables);
  if (tables.empty()) {
    LOG(ERROR) << "No table is in this database." << std::endl;
    return DB_TABLE_NOT_EXIST;
  }
  string index_in_db("Indexes_in_" + current_db_);
  int table_max_width = string("Table").length();
  int key_name_max_width = string("Key_name").length();
  int seq_in_index_max_width = string("Seq_in_index").length();
  int column_name_max_width = string("Column_name").length();
  int index_type_max_width = string("Index_type").length();
  for (auto table : tables) {
    string table_name = table->GetTableName();
    if (table_name.length() > table_max_width) table_max_width = table_name.length();
    vector<IndexInfo*> indexes;
    indexes.clear();
    current_db->catalog_mgr_->GetTableIndexes(table_name, indexes);
//    cout << "indexes_size: " << indexes.size() << endl;
    for (auto index : indexes) {
      string index_name = index->GetIndexName();
      if (index_name.length() > key_name_max_width) key_name_max_width = index_name.length();
      auto index_schema = index->GetIndexKeySchema();
      vector<Column *> columns;
      columns.clear();
      columns = index_schema->GetColumns();
      for (auto column : columns) {
        string column_name = column->GetName();
        if (column_name.length() > column_name_max_width) column_name_max_width = column_name.length();
        string column_type = "BTREE";
      }
    }
  }
  cout << "+" << setfill('-') << setw(table_max_width + 2) << "" << "+"
       << setw(key_name_max_width + 2) << "" << "+"
       << setw(seq_in_index_max_width + 2) << "" << "+"
       << setw(column_name_max_width + 2) << "" << "+"
       << setw(index_type_max_width + 2) << "" << "+"
       << endl;
  cout << "| " << std::left << setfill(' ') << setw(table_max_width) << "Table"
       << " | " << setw(key_name_max_width) << "Key_name"
       << " | " << setw(seq_in_index_max_width) << "Seq_in_index"
       << " | " << setw(column_name_max_width) << "Column_name"
       << " | " << setw(index_type_max_width) << "Index_type"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(table_max_width + 2) << "" << "+"
       << setw(key_name_max_width + 2) << "" << "+"
       << setw(seq_in_index_max_width + 2) << "" << "+"
       << setw(column_name_max_width + 2) << "" << "+"
       << setw(index_type_max_width + 2) << "" << "+"
       << endl;

  for (auto table : tables) {
    string table_name = table->GetTableName();
    if (table_name.length() > table_max_width) table_max_width = table_name.length();
    vector<IndexInfo*> indexes;
    indexes.clear();
    current_db->catalog_mgr_->GetTableIndexes(table_name, indexes);
    for (auto index : indexes) {
      string index_name = index->GetIndexName();
      auto index_schema = index->GetIndexKeySchema();
      vector<Column *> columns;
      columns.clear();
      columns = index_schema->GetColumns();
      int seq = 1;
//      LOG(INFO) << "schema address: " << index_schema << std::endl;
//      LOG(INFO) << "schema count: " << index_schema->GetColumnCount() << std::endl;
      for (auto column : columns) {
        string column_name = column->GetName();
        string column_type = "BTREE";
        cout << "| " << std::left << setfill(' ') << setw(table_max_width) << table_name
             << " | " << setw(key_name_max_width) << index_name
             << " | " << setw(seq_in_index_max_width) << seq ++
             << " | " << setw(column_name_max_width) << column_name
             << " | " << setw(index_type_max_width) << column_type
             << " |" << endl;
      }
    }
  }
  cout << "+" << setfill('-') << setw(table_max_width + 2) << "" << "+"
       << setw(key_name_max_width + 2) << "" << "+"
       << setw(seq_in_index_max_width + 2) << "" << "+"
       << setw(column_name_max_width + 2) << "" << "+"
       << setw(index_type_max_width + 2) << "" << "+"
       << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
/*
 * 5[label="kNodeCreateIndex(1,41)\nid(5)"]
 * |
 * 0[label="kNodeIdentifier(1,17)\nid(0),val(idx1)"] -> 1[label="kNodeIdentifier(1,23)\nid(1),val(t1)"] -> 6[label="kNodeColumnList(1,41)\nid(6),val(index keys)"] ---------------------------------------> 7[label="error type(1,41)\nid(7),val(index type)"]
 *                                                                                                         |                                                                                                |
 *                                                                                                         2[label="kNodeIdentifier(1,25)\nid(2),val(a)"] -> 3[label="kNodeIdentifier(1,28)\nid(3),val(b)"] 4[label="kNodeIdentifier(1,41)\nid(4),val(btree)"]
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (current_db_ == "") {
    LOG(ERROR) << "Choose a Database first." << std::endl;
    return DB_FAILED;
  }
  DBStorageEngine *current_db = dbs_[current_db_];
  // check if the table is already exist
  string index_name = ast->child_->val_;
  string table_name = ast->child_->next_->val_;
  vector<TableInfo *> tables;
  current_db->catalog_mgr_->GetTables(tables);
  TableInfo *table_info;
  bool table_exist = false;
//  LOG(INFO) << "here0" << std::endl;
  for (auto table : tables) {
    if (table->GetTableName() == table_name) {
      table_info = table;
      table_exist = true;
      break;
    }
  }
  if (!table_exist) {
    LOG(ERROR) << "Table " << table_name << " not exists." << std::endl;
    return DB_TABLE_NOT_EXIST;
  }
  vector<IndexInfo *> indexes;
  current_db->catalog_mgr_->GetTableIndexes(table_name, indexes);
//  LOG(INFO) << "here1" << std::endl;
  for (auto index : indexes) {
    if (index->GetIndexName() == index_name) {
      LOG(ERROR) << "Index " << index_name << " already exists." << std::endl;
      return DB_INDEX_ALREADY_EXIST;
    }
  }
//  LOG(INFO) << "here2" << std::endl;
  pSyntaxNode index_type_node = ast->child_->next_->next_->next_;
  string index_type;
  if (index_type_node != nullptr) {
    index_type = index_type_node->child_->val_;
  } else {
    index_type = "btree";
  }
//  LOG(INFO) << "here3" << std::endl;
  pSyntaxNode index_info_node = ast->child_->next_->next_;
  std::vector<string> column_names;
  pSyntaxNode column_node = index_info_node->child_;
  while(column_node != nullptr){
    column_names.emplace_back(column_node->val_);
    column_node = column_node->next_;
  }
//  LOG(INFO) << "column_names_count: " << column_names.size() << std::endl;
  Txn txn;
  IndexInfo *index_info = nullptr;
  dberr_t res = dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, column_names, &txn, index_info, index_type);
//  LOG(INFO) << "schema address: " << index_info->GetIndexKeySchema() << std::endl;
//  LOG(INFO) << "schema count: " << index_info->GetIndexKeySchema()->GetColumnCount() << std::endl;
  if (res == DB_SUCCESS) {
    cout << "Index " << index_name << " is created successfully." << endl;
  }
  return res;
}

/**
 * TODO: Student Implement
 */
 /*
  * 1[label="kNodeDropIndex(1,15)\nid(1)"]
  * |
  * 0[label="kNodeIdentifier(1,15)\nid(0),val(idx1)"]
  */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (current_db_ == "") {
    LOG(ERROR) << "Choose a Database first." << std::endl;
    return DB_FAILED;
  }
  DBStorageEngine *current_db = dbs_[current_db_];
  // check if the table is already exist
  string index_name = ast->child_->val_;
  vector<TableInfo *> tables;
  current_db->catalog_mgr_->GetTables(tables);
  TableInfo *table_info;
  bool drop_flag = false;
  for (auto table : tables) {
    if (current_db->catalog_mgr_->DropIndex(table->GetTableName(), index_name) == DB_SUCCESS) {
      drop_flag = true;
    }
  }
  if (!drop_flag) {
    LOG(ERROR) << "Index " << index_name << " not exists." << std::endl;
    return DB_INDEX_NOT_FOUND;
  }
  cout << "Index " << index_name << " drops successfully." << std::endl;
  return DB_SUCCESS;
}
/*
 * 0[label="kNodeTrxBegin(1,5)\nid(0)"]
 */
dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}
/*
 * 0[label="kNodeTrxCommit(1,6)\nid(0)"]
 */
dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}
/*
 * 0[label="kNodeTrxRollback(1,8)\nid(0)"]
 */
dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
/*
 * 1[label="kNodeExecFile(1,16)\nid(1)"]
 * |
 * 0[label="kNodeString(1,16)\nid(0),val(a.txt)"]
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  clock_t start, end;
  std::string file_name = ast->child_->val_;
  ifstream in(file_name, ios::in);
  if (!in.is_open()) {
    cout << "file not exist, please check the address" << endl;
    return DB_FAILED;
  }
  start = clock();
  const int buf_size = 1024;
  char cmd[buf_size];
  while (!in.eof()) {
    memset(cmd, 0, sizeof(cmd));
    while(in.peek() == '\n' || in.peek() == '\r')
      in.get();
    in.get(cmd, 1024, ';');
    in.get();
    int cmd_len = strlen(cmd);
    cmd[cmd_len] = ';';
    cmd[cmd_len + 1] = '\0';

    // create buffer for sql input
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
//    if (MinisqlParserGetError()) {
//      // error
//      printf("%s\n", MinisqlParserGetErrorMessage());
//    } else {
      // Comment them out if you don't need to debug the syntax tree
  //    printf("[INFO] Sql syntax parse ok!\n");
  //    SyntaxTreePrinter printer(MinisqlGetParserRootNode());
  //    printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
//    }

    auto result = this->Execute(MinisqlGetParserRootNode());

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    // quit condition
    this->ExecuteInformation(result);
    if (result == DB_QUIT) {
      break;
    }
  }
  end = clock();
  cout << "ExecuteExecfile " << file_name << " done successfully in " << (double)(end - start) / CLOCKS_PER_SEC << "s" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
/*
 * 0[label="kNodeQuit(1,4)\nid(0)"]
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
 return DB_QUIT;
}
