/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL

#include "sql/ob_ai_column_worker.h"

#include <cstdint>
#include <string>
#include <vector>

#include "common/mysqlclient/ob_mysql_proxy.h"
#include "common/mysqlclient/ob_mysql_result.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/utility.h"
#include "query/ai/ob_ai_endpoint_resolver.h"
#include "share/ai_service/ob_ai_service_struct.h"
#include "share/ob_errno.h"
#include "share/ob_server_struct.h"
#include "share/rc/ob_server_runtime.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_client.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"

namespace oceanbase
{
namespace sql
{
namespace ai_column
{
namespace
{

const int64_t AI_COLUMN_TICK_US = 100 * 1000;
const int64_t AI_COLUMN_RETRY_DELAY_US = 500 * 1000;
const int64_t AI_COLUMN_REFRESH_DELAY_US = 500 * 1000;
const int64_t AI_COLUMN_SCAN_LIMIT = 64;
const int64_t AI_COLUMN_CALL_TIMEOUT_SEC = 10;

// The row of a scan, reduced to what the publish step needs.
struct ScanRow
{
  std::string rowkey_text;
  std::string rowkey_predicate;
  std::vector<std::string> input;
  std::vector<int64_t> state;
  std::vector<int64_t> request;
  std::vector<bool> is_null;
};

inline common::ObString to_ob_string(const std::string &text)
{
  return common::ObString(static_cast<int32_t>(text.size()), const_cast<char *>(text.c_str()));
}

inline std::string worker_quote_name(const std::string &name) { return "`" + name + "`"; }

bool worker_get_table_schema_ptr(const std::string &db,
                          const std::string &table,
                          const share::schema::ObTableSchema *&table_schema)
{
  bool found = false;
  table_schema = nullptr;
  share::schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  if (nullptr != schema_service) {
    share::schema::ObSchemaGetterGuard guard;
    if (OB_SUCCESS == schema_service->get_runtime_schema_guard(guard)) {
      const common::ObString db_name(static_cast<int32_t>(db.size()), db.c_str());
      const common::ObString table_name(static_cast<int32_t>(table.size()), table.c_str());
      if (OB_SUCCESS == guard.get_table_schema(db_name, table_name, false, table_schema)
          && nullptr != table_schema) {
        found = true;
      }
    }
  }
  return found;
}

bool collect_rowkey_names(const share::schema::ObTableSchema &table,
                          std::vector<std::string> &names)
{
  bool ok = true;
  common::ObSEArray<uint64_t, 4> rowkey_ids;
  if (OB_SUCCESS != table.get_rowkey_column_ids(rowkey_ids) || rowkey_ids.empty()) {
    ok = false;
  } else {
    for (int64_t i = 0; ok && i < rowkey_ids.count(); ++i) {
      const share::schema::ObColumnSchemaV2 *column = table.get_column_schema(rowkey_ids.at(i));
      if (nullptr == column) {
        ok = false;
      } else {
        names.push_back(std::string(column->get_column_name_str().ptr(),
                                    static_cast<size_t>(column->get_column_name_str().length())));
      }
    }
  }
  return ok;
}

bool is_numeric_text(const std::string &text)
{
  bool ok = !text.empty();
  for (size_t i = 0; ok && i < text.size(); ++i) {
    const char c = text.at(i);
    if (!((c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.'
          || c == 'e' || c == 'E')) {
      ok = false;
    }
  }
  return ok;
}

bool is_integer_text(const std::string &text)
{
  bool ok = !text.empty();
  for (size_t i = 0; ok && i < text.size(); ++i) {
    const char c = text.at(i);
    if (!((c >= '0' && c <= '9') || (c == '-' && 0 == i))) {
      ok = false;
    }
  }
  return ok;
}

bool is_vector_text(const std::string &text)
{
  bool ok = text.size() > 1 && '[' == text.at(0) && ']' == text.at(text.size() - 1);
  for (size_t i = 0; ok && i < text.size(); ++i) {
    const char c = text.at(i);
    if (!((c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.' || c == 'e'
          || c == 'E' || c == ',' || c == '[' || c == ']' || c == ' ')) {
      ok = false;
    }
  }
  return ok;
}

// Turns the answer of the model into a SQL literal. A value that cannot be
// stored in the declared type is reported as a failure so the column is retried
// instead of being published with a value the type cannot hold.
int encode_value(const ObAIColumnSpec &spec, const std::string &text, std::string &literal)
{
  int ret = OB_SUCCESS;
  switch (spec.kind) {
    case AI_COLUMN_RESULT_INTEGER:
      if (!is_integer_text(text)) {
        ret = OB_INVALID_DATA;
      } else {
        literal = text;
      }
      break;
    case AI_COLUMN_RESULT_FLOAT:
      if (!is_numeric_text(text)) {
        ret = OB_INVALID_DATA;
      } else {
        literal = text;
      }
      break;
    case AI_COLUMN_RESULT_VECTOR:
      if (!is_vector_text(text)) {
        ret = OB_INVALID_DATA;
      } else {
        // Vector values are written as the string form the SQL layer accepts
        // for a vector column, the same literal an INSERT uses.
        literal.assign("'").append(sql_escape(text)).append("'");
      }
      break;
    case AI_COLUMN_RESULT_JSON:
    case AI_COLUMN_RESULT_TEXT:
    default:
      literal.assign("'").append(sql_escape(text)).append("'");
      break;
  }
  return ret;
}

std::string build_scan_sql(const ObAIColumnEntry &entry, const std::vector<std::string> &rowkey)
{
  std::string sql("select ");
  for (size_t i = 0; i < rowkey.size(); ++i) {
    sql.append("cast(").append(worker_quote_name(rowkey.at(i))).append(" as char) as rk_")
        .append(int_to_string(static_cast<int64_t>(i))).append(", ");
  }
  for (size_t j = 0; j < entry.specs.size(); ++j) {
    const ObAIColumnSpec &spec = entry.specs.at(j);
    const std::string index = int_to_string(static_cast<int64_t>(j));
    sql.append("cast((").append(spec.input_expr).append(") as char) as in_").append(index)
        .append(", cast(").append(worker_quote_name(spec.state_column())).append(" as signed) as st_")
        .append(index)
        .append(", cast(").append(worker_quote_name(spec.request_column())).append(" as signed) as rq_")
        .append(index)
        .append(", (").append(worker_quote_name(spec.column_name)).append(" is null) as nu_")
        .append(index);
    if (j + 1 < entry.specs.size()) {
      sql.append(", ");
    }
  }
  sql.append(" from ").append(worker_quote_name(entry.db)).append(".").append(worker_quote_name(entry.table))
      .append(" where ");
  for (size_t j = 0; j < entry.specs.size(); ++j) {
    const ObAIColumnSpec &spec = entry.specs.at(j);
    if (j > 0) {
      sql.append(" or ");
    }
    sql.append("(").append(worker_quote_name(spec.column_name)).append(" is null and (")
        .append(worker_quote_name(spec.state_column())).append(" div 1000) in (0, 3))");
  }
  sql.append(" limit ").append(int_to_string(AI_COLUMN_SCAN_LIMIT));
  return sql;
}

} // namespace

ObAIColumnWorker &ObAIColumnWorker::get_instance()
{
  static ObAIColumnWorker *worker = new ObAIColumnWorker();
  return *worker;
}

ObAIColumnWorker::ObAIColumnWorker() : started_(false), stopping_(false)
{
}

int ObAIColumnWorker::start()
{
  int ret = OB_SUCCESS;
  bool expected = false;
  if (started_.compare_exchange_strong(expected, true)) {
    stopping_.store(false);
    thread_ = std::thread(&ObAIColumnWorker::run, this);
    LOG_INFO("AI column worker started");
  }
  return ret;
}

void ObAIColumnWorker::stop()
{
  bool expected = true;
  if (started_.compare_exchange_strong(expected, false)) {
    stopping_.store(true);
    if (thread_.joinable()) {
      thread_.join();
    }
    LOG_INFO("AI column worker stopped");
  }
}

void ObAIColumnWorker::run()
{
  while (!stopping_.load()) {
    tick();
    for (int64_t waited = 0; waited < AI_COLUMN_TICK_US && !stopping_.load();
         waited += 10 * 1000) {
      ob_usleep(10 * 1000);
    }
  }
}

void ObAIColumnWorker::tick()
{
  std::vector<ObAIColumnEntry> entries;
  ObAIColumnRegistry::instance().snapshot(entries);
  const int64_t now = ObTimeUtility::current_time();
  if (retry_due_us_.size() > 1024) {
    std::map<std::string, int64_t>::iterator it = retry_due_us_.begin();
    while (it != retry_due_us_.end()) {
      if (it->second <= now) {
        retry_due_us_.erase(it++);
      } else {
        ++it;
      }
    }
  }
  for (size_t i = 0; i < entries.size() && !stopping_.load(); ++i) {
    const int ret = process_table(entries.at(i));
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to process an AI column table", K(ret), "db", entries.at(i).db.c_str(),
               "table", entries.at(i).table.c_str());
    }
  }
}

int ObAIColumnWorker::process_table(const ObAIColumnEntry &entry)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *table = nullptr;
  std::vector<std::string> rowkey;
  if (entry.specs.empty()) {
  } else if (!worker_get_table_schema_ptr(entry.db, entry.table, table)) {
    // The table is not visible yet or has been dropped.
  } else if (!collect_rowkey_names(*table, rowkey)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("an AI column table without a usable row key is skipped", K(ret),
             "db", entry.db.c_str(), "table", entry.table.c_str());
  } else {
    const std::string scan_sql = build_scan_sql(entry, rowkey);
    std::vector<ScanRow> rows;
    SMART_VAR(common::ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(GCTX.sql_proxy_->read(result, scan_sql.c_str()))) {
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        common::sqlclient::ObMySQLResult &reader = *result.get_result();
        int read_ret = OB_SUCCESS;
        while (OB_SUCC(read_ret) && OB_SUCC(read_ret = reader.next())) {
          ScanRow row;
          bool complete = true;
          for (size_t i = 0; complete && i < rowkey.size(); ++i) {
            common::ObObj obj;
            if (OB_FAIL(reader.get_obj(static_cast<int64_t>(i), obj)) || obj.is_null()) {
              complete = false;
            } else {
              const common::ObString text = obj.get_string();
              std::string value(text.ptr(), static_cast<size_t>(text.length()));
              if (i > 0) {
                row.rowkey_text.append("|");
                row.rowkey_predicate.append(" and ");
              }
              row.rowkey_text.append(value);
              row.rowkey_predicate.append(worker_quote_name(rowkey.at(i))).append(" = '")
                  .append(sql_escape(value)).append("'");
            }
          }
          const int64_t base = static_cast<int64_t>(rowkey.size());
          for (size_t j = 0; complete && j < entry.specs.size(); ++j) {
            common::ObObj input_obj;
            int64_t state = 0;
            int64_t request = 0;
            int64_t is_null = 0;
            const int64_t offset = base + static_cast<int64_t>(j) * 4;
            if (OB_FAIL(reader.get_obj(offset, input_obj))
                || OB_FAIL(reader.get_int(offset + 1, state))
                || OB_FAIL(reader.get_int(offset + 2, request))
                || OB_FAIL(reader.get_int(offset + 3, is_null))) {
              complete = false;
            } else {
              std::string input;
              if (!input_obj.is_null()) {
                const common::ObString text = input_obj.get_string();
                input.assign(text.ptr(), static_cast<size_t>(text.length()));
              }
              row.input.push_back(input);
              row.state.push_back(state);
              row.request.push_back(request);
              row.is_null.push_back(0 != is_null);
            }
          }
          if (complete) {
            rows.push_back(row);
          }
        }
        if (OB_ITER_END == read_ret) {
          read_ret = OB_SUCCESS;
        }
        ret = read_ret;
      }
    }
    if (OB_SUCC(ret)) {
      int64_t published = 0;
      const int64_t now = ObTimeUtility::current_time();
      for (size_t i = 0; OB_SUCC(ret) && i < rows.size() && !stopping_.load(); ++i) {
        const ScanRow &row = rows.at(i);
        std::string assignments;
        for (size_t j = 0; j < entry.specs.size(); ++j) {
          const ObAIColumnSpec &spec = entry.specs.at(j);
          const int64_t state_class = row.state.at(j) / AI_TASK_STATE_FACTOR;
          if (!row.is_null.at(j) || (AI_TASK_PENDING != state_class && AI_TASK_RETRY != state_class)) {
            continue;
          }
          std::string key(entry.db);
          key.append("\x1f").append(entry.table).append("\x1f").append(spec.column_name)
              .append("\x1f").append(row.rowkey_text);
          std::map<std::string, int64_t>::const_iterator due = retry_due_us_.find(key);
          if (due != retry_due_us_.end() && due->second > now) {
            continue;
          }
          std::string value;
          int64_t next_state = 0;
          std::string literal;
          const int model_ret = call_model(spec, row.input.at(j), value);
          if (OB_SUCC(model_ret) && OB_SUCCESS == encode_value(spec, value, literal)) {
            retry_due_us_.erase(key);
            next_state = AI_TASK_READY * AI_TASK_STATE_FACTOR
                + row.state.at(j) % AI_TASK_STATE_FACTOR;
          } else if (OB_SUCC(model_ret)) {
            LOG_WARN("the model answered with a value the column type cannot hold",
                     "db", entry.db.c_str(), "table", entry.table.c_str(),
                     "column", spec.column_name.c_str());
            next_state = AI_TASK_FAILED * AI_TASK_STATE_FACTOR
                + row.state.at(j) % AI_TASK_STATE_FACTOR;
            retry_due_us_.erase(key);
          } else {
            LOG_WARN("the model call of an AI column failed", K(model_ret),
                     "db", entry.db.c_str(), "table", entry.table.c_str(),
                     "column", spec.column_name.c_str());
            const int64_t retry_count = row.state.at(j) % AI_TASK_STATE_FACTOR;
            if (retry_count < spec.max_retries) {
              next_state = AI_TASK_RETRY * AI_TASK_STATE_FACTOR + retry_count + 1;
              retry_due_us_[key] = now + AI_COLUMN_RETRY_DELAY_US;
            } else {
              next_state = AI_TASK_FAILED * AI_TASK_STATE_FACTOR + retry_count;
              retry_due_us_.erase(key);
            }
          }
          if (!literal.empty()) {
            assignments.append(worker_quote_name(spec.column_name)).append(" = ").append(literal)
                .append(", ");
          }
          assignments.append(worker_quote_name(spec.state_column())).append(" = ")
              .append(int_to_string(next_state)).append(", ");
        }
        if (assignments.empty()) {
          continue;
        }
        std::string sql("update ");
        sql.append(worker_quote_name(entry.db)).append(".").append(worker_quote_name(entry.table))
            .append(" set ").append(assignments);
        sql.erase(sql.size() - 2, 2);
        sql.append(" where ").append(row.rowkey_predicate);
        for (size_t j = 0; j < entry.specs.size(); ++j) {
          sql.append(" and ").append(worker_quote_name(entry.specs.at(j).request_column()))
              .append(" = ").append(int_to_string(row.request.at(j)));
        }
        int64_t affected_rows = 0;
        if (OB_FAIL(GCTX.sql_proxy_->write(sql.c_str(), affected_rows))) {
          LOG_WARN("failed to publish an AI column result", K(ret), K(sql.c_str()));
        } else if (0 == affected_rows) {
          LOG_INFO("a stale AI column result was discarded", "db", entry.db.c_str(),
                   "table", entry.table.c_str(), "row", row.rowkey_text.c_str());
        } else {
          ++published;
        }
      }
      if (OB_SUCC(ret)) {
        std::string refresh_key(entry.db);
        refresh_key.append("\x1f").append(entry.table);
        std::map<std::string, int64_t>::const_iterator last =
            last_refresh_us_.find(refresh_key);
        const bool refresh = published > 0 || rows.empty();
        if (refresh && (published > 0 || last == last_refresh_us_.end()
                        || now - last->second >= AI_COLUMN_REFRESH_DELAY_US)) {
          last_refresh_us_[refresh_key] = now;
          const int refresh_ret = refresh_unfinished(entry);
          if (OB_FAIL(refresh_ret)) {
            LOG_WARN("failed to refresh the AI column completeness", K(refresh_ret),
                     "db", entry.db.c_str(), "table", entry.table.c_str());
          }
        }
      }
    }
  }
  return ret;
}

int ObAIColumnWorker::refresh_unfinished(const ObAIColumnEntry &entry)
{
  int ret = OB_SUCCESS;
  std::string sql("select ");
  for (size_t j = 0; j < entry.specs.size(); ++j) {
    sql.append("cast(ifnull(sum(").append(worker_quote_name(entry.specs.at(j).column_name))
        .append(" is null), 0) as signed) as n_")
        .append(int_to_string(static_cast<int64_t>(j)));
    if (j + 1 < entry.specs.size()) {
      sql.append(", ");
    }
  }
  sql.append(" from ").append(worker_quote_name(entry.db)).append(".").append(worker_quote_name(entry.table));
  SMART_VAR(common::ObMySQLProxy::MySQLResult, result) {
    if (OB_FAIL(GCTX.sql_proxy_->read(result, sql.c_str()))) {
    } else if (OB_ISNULL(result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      int read_ret = result.get_result()->next();
      if (OB_ITER_END == read_ret) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(read_ret)) {
        ret = read_ret;
      } else {
        for (size_t j = 0; OB_SUCC(ret) && j < entry.specs.size(); ++j) {
          int64_t value = 0;
          if (OB_FAIL(result.get_result()->get_int(static_cast<int64_t>(j), value))) {
          } else {
            ObAIColumnRegistry::instance().set_unfinished(
                entry.db, entry.table, entry.specs.at(j).column_name, value > 0);
          }
        }
      }
    }
  }
  return ret;
}

int ObAIColumnWorker::call_model(const ObAIColumnSpec &spec,
                                 const std::string &input,
                                 std::string &value)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator("AIColumnWorker");
  common::ObAIFuncExprInfo *info = nullptr;
  share::ObAiModelEndpointInfo endpoint;
  query::ObIAiEndpointResolver *resolver =
      share::server_service<query::ObIAiEndpointResolver>();
  if (spec.model_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(resolver)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const common::ObString model_name = to_ob_string(spec.model_name);
    if (OB_FAIL(common::ObAIFuncUtils::get_ai_func_info(allocator, model_name, info))) {
    } else if (OB_FAIL(resolver->resolve_by_model_name(model_name, allocator, endpoint))) {
    } else {
      common::ObArray<common::ObString> headers;
      common::ObJsonObject *body = nullptr;
      common::ObJsonObject *response = nullptr;
      common::ObIJsonBase *result_base = nullptr;
      common::ObString access_key;
      common::ObString request_model = endpoint.get_request_model_name();
      common::ObAIFuncClient client;
      client.set_timeout_sec(AI_COLUMN_CALL_TIMEOUT_SEC);
      if (spec.is_embed()) {
        common::ObAIFuncIEmbed *provider = nullptr;
        common::ObArray<common::ObString> contents;
        if (OB_FAIL(common::ObAIFuncUtils::get_embed_provider(
                allocator, endpoint.get_provider(), provider))) {
        } else if (OB_FAIL(endpoint.get_unencrypted_access_key(allocator, access_key))) {
        } else if (OB_FAIL(provider->get_header(allocator, access_key, headers))) {
        } else if (OB_FAIL(contents.push_back(to_ob_string(input)))) {
        } else if (OB_FAIL(provider->get_body(allocator, request_model, contents, nullptr, body))) {
        } else if (OB_FAIL(client.send_post(allocator, endpoint.get_url(), headers, body, response))) {
        } else if (OB_FAIL(provider->parse_output(allocator, response, result_base))) {
        } else {
          common::ObString text;
          if (OB_FAIL(common::ObAIFuncJsonUtils::print_json_to_str(allocator, result_base, text))) {
          } else {
            value.assign(text.ptr(), static_cast<size_t>(text.length()));
          }
        }
      } else {
        common::ObAIFuncIComplete *provider = nullptr;
        common::ObString system_prompt = to_ob_string(spec.system_prompt);
        common::ObString user_text = to_ob_string(input);
        if (OB_FAIL(common::ObAIFuncUtils::get_complete_provider(
                allocator, endpoint.get_provider(), provider))) {
        } else if (OB_FAIL(endpoint.get_unencrypted_access_key(allocator, access_key))) {
        } else if (OB_FAIL(provider->get_header(allocator, access_key, headers))) {
        } else if (OB_FAIL(provider->get_body(allocator, request_model, system_prompt,
                                              user_text, nullptr, body))) {
        } else if (OB_FAIL(client.send_post(allocator, endpoint.get_url(), headers, body, response))) {
        } else if (OB_FAIL(provider->parse_output(allocator, response, result_base))) {
        } else {
          // The provider returns the answer as a JSON string node; the stored
          // value is its text, exactly as AI_COMPLETE would return it.
          common::ObString text;
          if (OB_FAIL(common::ObAIFuncJsonUtils::print_json_to_str(allocator, result_base, text))) {
          } else {
            value.assign(text.ptr(), static_cast<size_t>(text.length()));
          }
        }
      }
    }
  }
  return ret;
}

} // namespace ai_column
} // namespace sql
} // namespace oceanbase
