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

#ifndef OCEANBASE_SQL_OB_AI_COLUMN_COMMON_H_
#define OCEANBASE_SQL_OB_AI_COLUMN_COMMON_H_

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <string>
#include <vector>

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace sql
{
namespace ai_column
{

// Hidden companion columns store their bookkeeping in the column comment. The
// comment starts with this marker so the DDL resolver can turn the column into
// a hidden one and the metadata views can tell an AI column apart from an
// ordinary column that happens to share the prefix.
const char *const AI_COLUMN_COMMENT_MARKER = "{\"__ai_hidden\":";
const char *const AI_HIDDEN_COLUMN_PREFIX = "__ai_";
const char *const AI_STATE_COLUMN_PREFIX = "__ai_state_";
const char *const AI_REQUEST_COLUMN_PREFIX = "__ai_request_";
const char *const AI_SPEC_COLUMN_PREFIX = "__ai_spec_";

const int64_t AI_TASK_PENDING = 0;
const int64_t AI_TASK_READY = 1;
const int64_t AI_TASK_FAILED = 2;
const int64_t AI_TASK_RETRY = 3;
const int64_t AI_TASK_STATE_FACTOR = 1000;

enum ObAIColumnResultKind
{
  AI_COLUMN_RESULT_JSON = 0,
  AI_COLUMN_RESULT_TEXT,
  AI_COLUMN_RESULT_INTEGER,
  AI_COLUMN_RESULT_FLOAT,
  AI_COLUMN_RESULT_VECTOR
};

// One AI column of one table. The spec is kept in memory for the lifetime of
// the process and is also serialized into the hidden spec column comment so
// the metadata view can expose max_retries and the normalized expression.
struct ObAIColumnSpec
{
  ObAIColumnSpec() : kind(AI_COLUMN_RESULT_TEXT), dimension(0), max_retries(0) {}
  std::string column_name;      // result column, original case
  std::string result_type_ddl;  // text between the column name and AI COLUMN
  ObAIColumnResultKind kind;
  int64_t dimension;            // declared vector dimension, 0 when none
  std::string func;             // "complete" or "embed"
  std::string model_name;
  std::string input_expr;       // second function argument, raw SQL text
  std::string system_prompt;
  int64_t max_retries;

  std::string state_column() const { return AI_STATE_COLUMN_PREFIX + column_name; }
  std::string request_column() const { return AI_REQUEST_COLUMN_PREFIX + column_name; }
  std::string spec_column() const { return AI_SPEC_COLUMN_PREFIX + column_name; }
  bool is_embed() const { return 0 == func.compare("embed"); }
  void reset() { *this = ObAIColumnSpec(); }
};

struct ObAIColumnEntry
{
  std::string db;     // original case, used when building SQL
  std::string table;
  std::vector<ObAIColumnSpec> specs;
  std::vector<bool> unfinished;
};

inline bool is_ai_hidden_column_name(const common::ObString &name)
{
  return name.prefix_match(AI_HIDDEN_COLUMN_PREFIX);
}

inline bool is_ai_hidden_column_name(const std::string &name)
{
  return 0 == name.compare(0, strlen(AI_HIDDEN_COLUMN_PREFIX), AI_HIDDEN_COLUMN_PREFIX);
}

// The DDL resolver marks a column hidden when its comment carries the marker.
inline bool is_ai_hidden_comment(const char *comment)
{
  return NULL != comment
      && 0 == strncmp(comment, AI_COLUMN_COMMENT_MARKER, strlen(AI_COLUMN_COMMENT_MARKER));
}

inline bool is_ai_hidden_comment(const common::ObString &comment)
{
  return comment.prefix_match(AI_COLUMN_COMMENT_MARKER);
}

inline std::string to_lower(const std::string &input)
{
  std::string out(input);
  for (std::string::iterator it = out.begin(); it != out.end(); ++it) {
    if (*it >= 'A' && *it <= 'Z') {
      *it = static_cast<char>(*it - 'A' + 'a');
    }
  }
  return out;
}

inline bool equals_ci(const std::string &lhs, const std::string &rhs)
{
  return to_lower(lhs) == to_lower(rhs);
}

// Escapes a value so it can be placed inside a single quoted SQL literal.
inline std::string sql_escape(const std::string &input)
{
  std::string out;
  out.reserve(input.size() + 8);
  for (std::string::const_iterator it = input.begin(); it != input.end(); ++it) {
    const char c = *it;
    switch (c) {
      case '\0': out.append("\\0"); break;
      case '\n': out.append("\\n"); break;
      case '\r': out.append("\\r"); break;
      case '\\': out.append("\\\\"); break;
      case '\'': out.append("''"); break;
      case '\032': out.append("\\Z"); break;
      default: out.push_back(c); break;
    }
  }
  return out;
}

// Escapes a value so it can be placed inside a JSON string literal.
inline std::string json_escape(const std::string &input)
{
  std::string out;
  out.reserve(input.size() + 8);
  for (std::string::const_iterator it = input.begin(); it != input.end(); ++it) {
    const char c = *it;
    switch (c) {
      case '"': out.append("\\\""); break;
      case '\\': out.append("\\\\"); break;
      case '\b': out.append("\\b"); break;
      case '\f': out.append("\\f"); break;
      case '\n': out.append("\\n"); break;
      case '\r': out.append("\\r"); break;
      case '\t': out.append("\\t"); break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          char tmp[8];
          snprintf(tmp, sizeof(tmp), "\\u%04x", static_cast<unsigned char>(c));
          out.append(tmp);
        } else {
          out.push_back(c);
        }
        break;
    }
  }
  return out;
}

inline std::string int_to_string(const int64_t value)
{
  char tmp[32];
  snprintf(tmp, sizeof(tmp), "%ld", value);
  return std::string(tmp);
}

// Serializes the metadata needed by oceanbase.__all_ai_column. The JSON lives in
// the hidden spec column comment, which is capped by the column comment limit.
inline std::string spec_to_comment_json(const ObAIColumnSpec &spec)
{
  std::string json = AI_COLUMN_COMMENT_MARKER;
  json.append("1,\"kind\":\"spec\"");
  json.append(",\"column\":\"").append(json_escape(spec.column_name)).append("\"");
  json.append(",\"func\":\"").append(json_escape(spec.func)).append("\"");
  json.append(",\"model\":\"").append(json_escape(spec.model_name)).append("\"");
  json.append(",\"max_retries\":").append(int_to_string(spec.max_retries));
  json.append(",\"dimension\":").append(int_to_string(spec.dimension));
  json.append(",\"input\":\"").append(json_escape(spec.input_expr)).append("\"");
  json.append(",\"prompt\":\"").append(json_escape(spec.system_prompt)).append("\"");
  json.append("}");
  return json;
}

inline std::string hidden_comment_json(const char *kind)
{
  std::string json = AI_COLUMN_COMMENT_MARKER;
  json.append("1,\"kind\":\"").append(kind).append("\"}");
  return json;
}

// The in-memory registry of AI columns. It is deliberately process wide: the
// rewrite step publishes the schema side of an AI column and the background
// worker consumes it. Entries are created by the rewrite step and never
// released, so the singleton is allocated once and intentionally leaked.
class ObAIColumnRegistry
{
public:
  static ObAIColumnRegistry &instance()
  {
    static ObAIColumnRegistry *registry = new ObAIColumnRegistry();
    return *registry;
  }

  void register_table(const std::string &db, const std::string &table,
                      const std::vector<ObAIColumnSpec> &specs)
  {
    std::lock_guard<std::mutex> guard(mutex_);
    const int64_t idx = find_index(db, table);
    if (idx < 0) {
      ObAIColumnEntry entry;
      entry.db = db;
      entry.table = table;
      entry.specs = specs;
      entry.unfinished.assign(specs.size(), true);
      entries_.push_back(entry);
    } else {
      ObAIColumnEntry &entry = entries_.at(static_cast<size_t>(idx));
      for (int64_t i = 0; i < static_cast<int64_t>(specs.size()); ++i) {
        bool replaced = false;
        for (int64_t j = 0; j < static_cast<int64_t>(entry.specs.size()); ++j) {
          if (equals_ci(entry.specs.at(static_cast<size_t>(j)).column_name,
                        specs.at(static_cast<size_t>(i)).column_name)) {
            entry.specs.at(static_cast<size_t>(j)) = specs.at(static_cast<size_t>(i));
            replaced = true;
            break;
          }
        }
        if (!replaced) {
          entry.specs.push_back(specs.at(static_cast<size_t>(i)));
          entry.unfinished.push_back(true);
        }
      }
    }
  }

  bool find_table(const std::string &db, const std::string &table, ObAIColumnEntry &out) const
  {
    std::lock_guard<std::mutex> guard(mutex_);
    bool found = false;
    const int64_t idx = find_index(db, table);
    if (idx >= 0) {
      out = entries_.at(static_cast<size_t>(idx));
      found = true;
    }
    return found;
  }

  bool has_table(const std::string &db, const std::string &table) const
  {
    std::lock_guard<std::mutex> guard(mutex_);
    return find_index(db, table) >= 0;
  }

  void snapshot(std::vector<ObAIColumnEntry> &out) const
  {
    std::lock_guard<std::mutex> guard(mutex_);
    out = entries_;
  }

  void set_unfinished(const std::string &db, const std::string &table,
                      const std::string &column, const bool unfinished)
  {
    std::lock_guard<std::mutex> guard(mutex_);
    const int64_t idx = find_index(db, table);
    if (idx >= 0) {
      ObAIColumnEntry &entry = entries_.at(static_cast<size_t>(idx));
      for (int64_t i = 0; i < static_cast<int64_t>(entry.specs.size()); ++i) {
        if (equals_ci(entry.specs.at(static_cast<size_t>(i)).column_name, column)) {
          entry.unfinished.at(static_cast<size_t>(i)) = unfinished;
          break;
        }
      }
    }
  }

  void set_all_unfinished(const std::string &db, const std::string &table, const bool unfinished)
  {
    std::lock_guard<std::mutex> guard(mutex_);
    const int64_t idx = find_index(db, table);
    if (idx >= 0) {
      ObAIColumnEntry &entry = entries_.at(static_cast<size_t>(idx));
      entry.unfinished.assign(entry.specs.size(), unfinished);
    }
  }

  bool is_unfinished(const std::string &db, const std::string &table,
                     const std::string &column) const
  {
    std::lock_guard<std::mutex> guard(mutex_);
    bool unfinished = false;
    const int64_t idx = find_index(db, table);
    if (idx >= 0) {
      const ObAIColumnEntry &entry = entries_.at(static_cast<size_t>(idx));
      for (int64_t i = 0; i < static_cast<int64_t>(entry.specs.size()); ++i) {
        if (equals_ci(entry.specs.at(static_cast<size_t>(i)).column_name, column)) {
          unfinished = entry.unfinished.at(static_cast<size_t>(i));
          break;
        }
      }
    }
    return unfinished;
  }

private:
  ObAIColumnRegistry() {}
  int64_t find_index(const std::string &db, const std::string &table) const
  {
    int64_t found = -1;
    for (int64_t i = 0; i < static_cast<int64_t>(entries_.size()); ++i) {
      if (equals_ci(entries_.at(static_cast<size_t>(i)).db, db)
          && equals_ci(entries_.at(static_cast<size_t>(i)).table, table)) {
        found = i;
        break;
      }
    }
    return found;
  }

  mutable std::mutex mutex_;
  std::vector<ObAIColumnEntry> entries_;
};

} // namespace ai_column
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_AI_COLUMN_COMMON_H_
