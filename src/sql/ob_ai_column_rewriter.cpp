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

#include "sql/ob_ai_column_rewriter.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "lib/container/ob_se_array.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "sql/ob_ai_column_common.h"
#include "sql/ob_ai_column_worker.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace sql
{
namespace ai_column
{
namespace
{

const char *const AI_PROMPT_JSON =
    "You are an AI data transformation function. Return exactly one valid JSON value "
    "and nothing else, without markdown fences or commentary.";
const char *const AI_PROMPT_INTEGER =
    "You are an AI data transformation function. Return exactly one base-10 integer "
    "and nothing else, without markdown fences or commentary.";
const char *const AI_PROMPT_TEXT =
    "You are an AI data transformation function. Answer with plain text only.";

enum TokenType
{
  TOKEN_IDENT = 0,
  TOKEN_QUOTED_IDENT,
  TOKEN_STRING,
  TOKEN_NUMBER,
  TOKEN_PUNCT
};

struct Token
{
  TokenType type;
  int64_t begin;
  int64_t end;
  std::string text;  // lowercase for bare identifiers, unquoted value otherwise
};

struct Edit
{
  int64_t begin;
  int64_t end;
  std::string text;
};

struct TableRef
{
  TableRef() : first_tok(-1), last_tok(-1) {}
  std::string db;
  std::string table;
  int64_t first_tok;
  int64_t last_tok;
};

int rewrite_alter_add(const std::string &sql, const std::vector<Token> &tokens,
                      const TableRef &ref, int64_t action_tok, std::vector<Edit> &edits);
int rewrite_alter_retry(const std::string &sql, const std::vector<Token> &tokens,
                        const TableRef &ref, int64_t retry_tok, std::vector<Edit> &edits);

inline bool is_space_char(const char c)
{
  return ' ' == c || '\t' == c || '\n' == c || '\r' == c || '\f' == c || '\v' == c;
}

inline bool is_ident_start(const char c)
{
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || '_' == c || '$' == c
         || static_cast<unsigned char>(c) >= 0x80;
}

inline bool is_ident_char(const char c)
{
  return is_ident_start(c) || (c >= '0' && c <= '9');
}

inline bool is_digit(const char c) { return c >= '0' && c <= '9'; }

std::string to_lower_copy(const std::string &input)
{
  std::string out(input);
  for (std::string::iterator it = out.begin(); it != out.end(); ++it) {
    if (*it >= 'A' && *it <= 'Z') {
      *it = static_cast<char>(*it - 'A' + 'a');
    }
  }
  return out;
}

std::string trim_copy(const std::string &sql, const int64_t begin, const int64_t end)
{
  int64_t left = begin;
  int64_t right = end;
  while (left < right && is_space_char(sql.at(static_cast<size_t>(left)))) {
    ++left;
  }
  while (right > left && is_space_char(sql.at(static_cast<size_t>(right - 1)))) {
    --right;
  }
  return sql.substr(static_cast<size_t>(left), static_cast<size_t>(right - left));
}

// Case insensitive substring search, used to skip the tokenizer for the vast
// majority of statements that cannot mention an AI column.
bool contains_ci(const std::string &text, const char *needle)
{
  const size_t length = strlen(needle);
  bool found = false;
  if (length > 0 && text.size() >= length) {
    const std::string lower = to_lower_copy(text);
    found = std::string::npos != lower.find(needle);
  }
  return found;
}

bool starts_with_word_ci(const std::string &text, const char *word)
{
  size_t begin = 0;
  while (begin < text.size() && is_space_char(text.at(begin))) {
    ++begin;
  }
  const size_t length = strlen(word);
  bool matched = text.size() >= begin + length;
  for (size_t i = 0; matched && i < length; ++i) {
    char c = text.at(begin + i);
    if (c >= 'A' && c <= 'Z') {
      c = static_cast<char>(c - 'A' + 'a');
    }
    if (c != word[i]) {
      matched = false;
    }
  }
  if (matched && begin + length < text.size()) {
    const char next = text.at(begin + length);
    if ((next >= 'a' && next <= 'z') || (next >= 'A' && next <= 'Z')
        || (next >= '0' && next <= '9') || next == '_') {
      matched = false;
    }
  }
  return matched;
}

bool contains_registered_table(const std::string &text)
{
  std::vector<ObAIColumnEntry> entries;
  ObAIColumnRegistry::instance().snapshot(entries);
  bool found = false;
  for (size_t i = 0; !found && i < entries.size(); ++i) {
    found = contains_ci(text, entries.at(i).table.c_str());
  }
  return found;
}

bool tokenize(const std::string &sql, std::vector<Token> &tokens)
{
  bool ok = true;
  const int64_t n = static_cast<int64_t>(sql.size());
  int64_t i = 0;
  while (ok && i < n) {
    const char c = sql.at(static_cast<size_t>(i));
    if (is_space_char(c)) {
      ++i;
    } else if ('#' == c) {
      while (i < n && '\n' != sql.at(static_cast<size_t>(i))) {
        ++i;
      }
    } else if ('-' == c && i + 1 < n && '-' == sql.at(static_cast<size_t>(i + 1))
               && (i + 2 >= n || is_space_char(sql.at(static_cast<size_t>(i + 2))))) {
      while (i < n && '\n' != sql.at(static_cast<size_t>(i))) {
        ++i;
      }
    } else if ('/' == c && i + 1 < n && '*' == sql.at(static_cast<size_t>(i + 1))) {
      i += 2;
      while (i + 1 < n && !('/' == sql.at(static_cast<size_t>(i))
                            && '*' == sql.at(static_cast<size_t>(i + 1)))) {
        ++i;
      }
      i = (i + 1 < n) ? i + 2 : n;
    } else if ('\'' == c || '"' == c) {
      const char quote = c;
      const int64_t begin = i;
      std::string value;
      ++i;
      bool closed = false;
      while (i < n) {
        const char cur = sql.at(static_cast<size_t>(i));
        if ('\\' == cur && i + 1 < n) {
          value.push_back(sql.at(static_cast<size_t>(i + 1)));
          i += 2;
        } else if (cur == quote) {
          if (i + 1 < n && sql.at(static_cast<size_t>(i + 1)) == quote) {
            value.push_back(quote);
            i += 2;
          } else {
            ++i;
            closed = true;
            break;
          }
        } else {
          value.push_back(cur);
          ++i;
        }
      }
      if (!closed) {
        ok = false;
      } else {
        Token token;
        token.type = TOKEN_STRING;
        token.begin = begin;
        token.end = i;
        token.text = value;
        tokens.push_back(token);
      }
    } else if ('`' == c) {
      const int64_t begin = i;
      std::string value;
      ++i;
      bool closed = false;
      while (i < n) {
        const char cur = sql.at(static_cast<size_t>(i));
        if ('`' == cur) {
          if (i + 1 < n && '`' == sql.at(static_cast<size_t>(i + 1))) {
            value.push_back('`');
            i += 2;
          } else {
            ++i;
            closed = true;
            break;
          }
        } else {
          value.push_back(cur);
          ++i;
        }
      }
      if (!closed) {
        ok = false;
      } else {
        Token token;
        token.type = TOKEN_QUOTED_IDENT;
        token.begin = begin;
        token.end = i;
        token.text = value;
        tokens.push_back(token);
      }
    } else if (is_ident_start(c)) {
      const int64_t begin = i;
      while (i < n && is_ident_char(sql.at(static_cast<size_t>(i)))) {
        ++i;
      }
      Token token;
      token.type = TOKEN_IDENT;
      token.begin = begin;
      token.end = i;
      token.text = to_lower_copy(sql.substr(static_cast<size_t>(begin),
                                            static_cast<size_t>(i - begin)));
      tokens.push_back(token);
    } else if (is_digit(c) || ('.' == c && i + 1 < n && is_digit(sql.at(static_cast<size_t>(i + 1))))) {
      const int64_t begin = i;
      while (i < n) {
        const char cur = sql.at(static_cast<size_t>(i));
        if (is_digit(cur) || '.' == cur || ('e' == cur || 'E' == cur)) {
          ++i;
        } else if (('+' == cur || '-' == cur) && i > begin
                   && ('e' == sql.at(static_cast<size_t>(i - 1))
                       || 'E' == sql.at(static_cast<size_t>(i - 1)))) {
          ++i;
        } else {
          break;
        }
      }
      Token token;
      token.type = TOKEN_NUMBER;
      token.begin = begin;
      token.end = i;
      token.text = sql.substr(static_cast<size_t>(begin), static_cast<size_t>(i - begin));
      tokens.push_back(token);
    } else {
      Token token;
      token.type = TOKEN_PUNCT;
      token.begin = i;
      token.end = i + 1;
      token.text = std::string(1, c);
      tokens.push_back(token);
      ++i;
    }
  }
  return ok;
}

inline bool is_word(const Token &token, const char *word)
{
  return TOKEN_IDENT == token.type && 0 == token.text.compare(word);
}

inline bool is_punct(const Token &token, const char punct)
{
  return TOKEN_PUNCT == token.type && token.text.size() == 1 && token.text.at(0) == punct;
}

inline bool is_name_token(const Token &token)
{
  return TOKEN_IDENT == token.type || TOKEN_QUOTED_IDENT == token.type;
}

// Strips the backticks (if any) and returns the original spelling of an identifier.
std::string token_name(const std::string &sql, const Token &token)
{
  if (TOKEN_QUOTED_IDENT == token.type) {
    return token.text;
  }
  return sql.substr(static_cast<size_t>(token.begin),
                    static_cast<size_t>(token.end - token.begin));
}

bool find_matching_paren(const std::vector<Token> &tokens, const int64_t open_tok,
                         int64_t &close_tok)
{
  bool found = false;
  int64_t depth = 0;
  for (int64_t i = open_tok; !found && i < static_cast<int64_t>(tokens.size()); ++i) {
    if (is_punct(tokens.at(static_cast<size_t>(i)), '(')) {
      ++depth;
    } else if (is_punct(tokens.at(static_cast<size_t>(i)), ')')) {
      --depth;
      if (0 == depth) {
        close_tok = i;
        found = true;
      }
    }
  }
  return found && close_tok > open_tok;
}

bool parse_table_ref(const std::string &sql, const std::vector<Token> &tokens,
                     const int64_t start_tok, const std::string &default_db,
                     TableRef &ref)
{
  bool ok = false;
  if (start_tok >= 0 && start_tok < static_cast<int64_t>(tokens.size())
      && is_name_token(tokens.at(static_cast<size_t>(start_tok)))) {
    ref.first_tok = start_tok;
    ref.last_tok = start_tok;
    ref.db = default_db;
    ref.table = token_name(sql, tokens.at(static_cast<size_t>(start_tok)));
    if (start_tok + 2 < static_cast<int64_t>(tokens.size())
        && is_punct(tokens.at(static_cast<size_t>(start_tok + 1)), '.')
        && is_name_token(tokens.at(static_cast<size_t>(start_tok + 2)))) {
      ref.db = ref.table;
      ref.table = token_name(sql, tokens.at(static_cast<size_t>(start_tok + 2)));
      ref.last_tok = start_tok + 2;
    }
    ok = true;
  }
  return ok;
}

void split_top_level(const std::vector<Token> &tokens, const int64_t begin, const int64_t end,
                     std::vector<std::pair<int64_t, int64_t> > &ranges)
{
  int64_t depth = 0;
  int64_t from = begin;
  for (int64_t i = begin; i < end; ++i) {
    if (is_punct(tokens.at(static_cast<size_t>(i)), '(')) {
      ++depth;
    } else if (is_punct(tokens.at(static_cast<size_t>(i)), ')')) {
      --depth;
    } else if (0 == depth && is_punct(tokens.at(static_cast<size_t>(i)), ',')) {
      ranges.push_back(std::make_pair(from, i));
      from = i + 1;
    }
  }
  ranges.push_back(std::make_pair(from, end));
}

void collect_identifiers(const std::string &expr, std::vector<std::string> &idents)
{
  std::vector<Token> tokens;
  if (tokenize(expr, tokens)) {
    for (size_t i = 0; i < tokens.size(); ++i) {
      const Token &token = tokens.at(i);
      if (TOKEN_IDENT != token.type) {
        continue;
      }
      if (i > 0 && is_punct(tokens.at(i - 1), '.')) {
        continue;
      }
      if (i + 1 < tokens.size() && is_punct(tokens.at(i + 1), '(')) {
        continue;
      }
      idents.push_back(token.text);
    }
  }
}

bool contains_identifier(const std::vector<std::string> &idents, const std::string &name)
{
  const std::string lower = to_lower_copy(name);
  bool found = false;
  for (size_t i = 0; !found && i < idents.size(); ++i) {
    if (idents.at(i) == lower) {
      found = true;
    }
  }
  return found;
}

bool parse_result_type(const std::string &ddl, ObAIColumnResultKind &kind, int64_t &dimension)
{
  bool vector_type = false;
  const std::string lower = to_lower_copy(trim_copy(ddl, 0, static_cast<int64_t>(ddl.size())));
  dimension = 0;
  if (0 == lower.compare(0, 6, "vector")) {
    vector_type = true;
    kind = AI_COLUMN_RESULT_VECTOR;
    const size_t open = lower.find('(');
    const size_t close = lower.find(')');
    if (std::string::npos != open && std::string::npos != close && close > open + 1) {
      dimension = atoll(lower.substr(open + 1, close - open - 1).c_str());
    }
  } else if (0 == lower.compare("json")) {
    kind = AI_COLUMN_RESULT_JSON;
  } else if (std::string::npos != lower.find("int")) {
    kind = AI_COLUMN_RESULT_INTEGER;
  } else if (0 == lower.compare(0, 5, "float") || 0 == lower.compare(0, 6, "double")
             || 0 == lower.compare(0, 7, "decimal") || 0 == lower.compare(0, 7, "numeric")) {
    kind = AI_COLUMN_RESULT_FLOAT;
  } else {
    kind = AI_COLUMN_RESULT_TEXT;
  }
  return !vector_type || dimension > 0;
}

std::string build_column_definition(const ObAIColumnSpec &spec, const std::string &result_name,
                                    const std::string &add_prefix)
{
  std::string def;
  def.reserve(768);
  def.append(result_name).append(" ").append(spec.result_type_ddl);
  def.append(", ").append(add_prefix).append("`").append(spec.state_column())
      .append("` bigint default 0 comment '")
      .append(sql_escape(hidden_comment_json("state"))).append("'");
  def.append(", ").append(add_prefix).append("`").append(spec.request_column())
      .append("` bigint default 0 comment '")
      .append(sql_escape(hidden_comment_json("request"))).append("'");
  def.append(", ").append(add_prefix).append("`").append(spec.spec_column())
      .append("` varchar(512) default null comment '")
      .append(sql_escape(spec_to_comment_json(spec))).append("'");
  return def;
}

// Parses one column definition. Returns 0 when the definition is not an AI
// column, 1 when it is, and an error code otherwise.
int parse_ai_column(const std::string &sql, const std::vector<Token> &tokens,
                    const int64_t first_tok, const int64_t limit_tok,
                    const std::string &add_prefix, ObAIColumnSpec &spec,
                    std::string &replacement, int64_t &after_tok, bool &is_ai)
{
  int ret = OB_SUCCESS;
  is_ai = false;
  after_tok = limit_tok;
  int64_t ai_tok = -1;
  for (int64_t i = first_tok; i + 1 < limit_tok && ai_tok < 0; ++i) {
    if (is_word(tokens.at(static_cast<size_t>(i)), "ai")
        && is_word(tokens.at(static_cast<size_t>(i + 1)), "column")) {
      ai_tok = i;
    }
  }
  if (ai_tok < 0) {
  } else if (ai_tok <= first_tok
             || !is_name_token(tokens.at(static_cast<size_t>(first_tok)))) {
    ret = OB_NOT_SUPPORTED;
  } else {
    const int64_t clause_open = ai_tok + 2;
    int64_t clause_close = -1;
    if (clause_open >= limit_tok
        || !is_punct(tokens.at(static_cast<size_t>(clause_open)), '(')
        || !find_matching_paren(tokens, clause_open, clause_close)
        || clause_close >= limit_tok) {
      ret = OB_NOT_SUPPORTED;
    } else {
      const int64_t func_tok = clause_open + 1;
      int64_t func_close = -1;
      if (func_tok >= clause_close
          || !is_name_token(tokens.at(static_cast<size_t>(func_tok)))
          || func_tok + 1 >= clause_close
          || !is_punct(tokens.at(static_cast<size_t>(func_tok + 1)), '(')
          || !find_matching_paren(tokens, func_tok + 1, func_close)
          || func_close > clause_close) {
        ret = OB_NOT_SUPPORTED;
      } else {
        std::vector<std::pair<int64_t, int64_t> > args;
        split_top_level(tokens, func_tok + 2, func_close, args);
        if (args.size() < 2 || args.at(0).first >= args.at(0).second
            || args.at(1).first >= args.at(1).second) {
          ret = OB_NOT_SUPPORTED;
        } else {
          spec.reset();
          spec.column_name = token_name(sql, tokens.at(static_cast<size_t>(first_tok)));
          spec.result_type_ddl = trim_copy(sql, tokens.at(static_cast<size_t>(first_tok)).end,
                                           tokens.at(static_cast<size_t>(ai_tok)).begin);
          spec.func = to_lower_copy(token_name(sql, tokens.at(static_cast<size_t>(func_tok))));
          if (0 != spec.func.compare("ai_complete") && 0 != spec.func.compare("ai_embed")) {
            ret = OB_NOT_SUPPORTED;
          } else {
            spec.func = (0 == spec.func.compare("ai_embed")) ? "embed" : "complete";
            if (TOKEN_STRING == tokens.at(static_cast<size_t>(args.at(0).first)).type
                || is_name_token(tokens.at(static_cast<size_t>(args.at(0).first)))) {
              spec.model_name = tokens.at(static_cast<size_t>(args.at(0).first)).text;
            } else {
              spec.model_name = trim_copy(sql, tokens.at(static_cast<size_t>(args.at(0).first)).begin,
                                          tokens.at(static_cast<size_t>(args.at(0).second - 1)).end);
            }
            spec.input_expr = trim_copy(sql, tokens.at(static_cast<size_t>(args.at(1).first)).begin,
                                        tokens.at(static_cast<size_t>(args.at(1).second - 1)).end);
            int64_t declared_dim = 0;
            parse_result_type(spec.result_type_ddl, spec.kind, declared_dim);
            int64_t given_dim = 0;
            if (args.size() >= 3 && args.at(2).first < args.at(2).second) {
              const Token &dim_token = tokens.at(static_cast<size_t>(args.at(2).first));
              if (TOKEN_NUMBER == dim_token.type) {
                given_dim = atoll(dim_token.text.c_str());
              }
            }
            if (given_dim > 0 && declared_dim > 0 && given_dim != declared_dim) {
              ret = OB_ERR_INVALID_VECTOR_DIM;
              LOG_USER_ERROR(OB_ERR_INVALID_VECTOR_DIM,
                             static_cast<unsigned>(declared_dim),
                             static_cast<unsigned>(given_dim));
            } else {
              spec.dimension = declared_dim > 0 ? declared_dim : given_dim;
              if (spec.is_embed()) {
                spec.system_prompt.clear();
              } else if (AI_COLUMN_RESULT_JSON == spec.kind) {
                spec.system_prompt = AI_PROMPT_JSON;
              } else if (AI_COLUMN_RESULT_INTEGER == spec.kind) {
                spec.system_prompt = AI_PROMPT_INTEGER;
              } else {
                spec.system_prompt = AI_PROMPT_TEXT;
              }
              after_tok = clause_close + 1;
              if (after_tok + 1 < limit_tok
                  && is_word(tokens.at(static_cast<size_t>(after_tok)), "with")
                  && is_punct(tokens.at(static_cast<size_t>(after_tok + 1)), '(')) {
                int64_t options_close = -1;
                if (!find_matching_paren(tokens, after_tok + 1, options_close)
                    || options_close >= limit_tok) {
                  ret = OB_NOT_SUPPORTED;
                } else {
                  std::vector<std::pair<int64_t, int64_t> > options;
                  split_top_level(tokens, after_tok + 2, options_close, options);
                  for (size_t i = 0; i < options.size(); ++i) {
                    const int64_t from = options.at(i).first;
                    const int64_t to = options.at(i).second;
                    if (to - from >= 3 && is_word(tokens.at(static_cast<size_t>(from)),
                                                  "max_retries")
                        && is_punct(tokens.at(static_cast<size_t>(from + 1)), '=')) {
                      const Token &value = tokens.at(static_cast<size_t>(from + 2));
                      spec.max_retries = (TOKEN_NUMBER == value.type)
                                             ? atoll(value.text.c_str())
                                             : (TOKEN_STRING == value.type
                                                    ? atoll(value.text.c_str())
                                                    : 0);
                    }
                  }
                  after_tok = options_close + 1;
                }
              }
              if (OB_SUCC(ret)) {
                replacement = build_column_definition(
                    spec, token_name(sql, tokens.at(static_cast<size_t>(first_tok))),
                    add_prefix);
                is_ai = true;
              }
            }
          }
        }
      }
    }
  }
  if (OB_NOT_SUPPORTED == ret && !is_ai) {
    // A malformed AI column definition is reported by the regular parser when
    // the statement keeps its original text; only surface the error for a
    // definition this rewriter claims to understand.
    ret = OB_SUCCESS;
  }
  return ret;
}

bool check_ai_dependency(const std::vector<ObAIColumnSpec> &specs, const int64_t extra)
{
  bool conflict = false;
  for (size_t i = 0; !conflict && i < specs.size(); ++i) {
    std::vector<std::string> idents;
    collect_identifiers(specs.at(i).input_expr, idents);
    for (size_t j = 0; !conflict && j < specs.size(); ++j) {
      if (i != j && contains_identifier(idents, specs.at(j).column_name)) {
        conflict = true;
      }
    }
    if (!conflict && extra >= 0 && static_cast<int64_t>(i) != extra
        && contains_identifier(idents, specs.at(static_cast<size_t>(extra)).column_name)) {
      conflict = true;
    }
  }
  return conflict;
}

bool get_table_schema_ptr(const std::string &db, const std::string &table,
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

std::string quote_name(const std::string &name) { return "`" + name + "`"; }

std::string build_rowkey_predicate(const share::schema::ObTableSchema &table)
{
  std::string expr;
  common::ObSEArray<uint64_t, 4> rowkey_ids;
  if (OB_SUCCESS != table.get_rowkey_column_ids(rowkey_ids)) {
    return expr;
  }
  for (int64_t i = 0; i < rowkey_ids.count(); ++i) {
    const share::schema::ObColumnSchemaV2 *column = table.get_column_schema(rowkey_ids.at(i));
    if (nullptr == column) {
      expr.clear();
      return expr;
    }
    const std::string name(column->get_column_name_str().ptr(),
                           static_cast<size_t>(column->get_column_name_str().length()));
    if (0 == i) {
      expr.append("concat('`").append(name).append("` = ', ").append(quote_name(name));
    } else {
      expr.append(", ' AND `").append(name).append("` = ', ").append(quote_name(name));
    }
  }
  if (!expr.empty()) {
    expr.append(")");
  }
  return expr;
}

std::string build_request_id_expr(const share::schema::ObTableSchema &table,
                                  const std::string &request_column)
{
  std::string expr("concat(");
  common::ObSEArray<uint64_t, 4> rowkey_ids;
  if (OB_SUCCESS != table.get_rowkey_column_ids(rowkey_ids)) {
    return std::string();
  }
  for (int64_t i = 0; i < rowkey_ids.count(); ++i) {
    const share::schema::ObColumnSchemaV2 *column = table.get_column_schema(rowkey_ids.at(i));
    if (nullptr == column) {
      return std::string();
    }
    const std::string name(column->get_column_name_str().ptr(),
                           static_cast<size_t>(column->get_column_name_str().length()));
    if (i > 0) {
      expr.append(", '|', ");
    }
    expr.append(quote_name(name));
  }
  expr.append(", '#', ").append(quote_name(request_column)).append(")");
  return expr;
}

std::string build_ai_column_view()
{
  return std::string(
      "(select tc.table_id as table_id, tc.column_id as column_id,"
      " tc.column_name as column_name,"
      " cast(json_extract(pc.comment, '$.max_retries') as signed) as max_retries,"
      " concat(json_unquote(json_extract(pc.comment, '$.prompt')), ' (',"
      " json_unquote(json_extract(pc.comment, '$.input')), ')') as normalized_expr,"
      " sc.column_id as state_column_id, rc.column_id as request_column_id,"
      " pc.column_id as result_spec_column_id"
      " from oceanbase.__all_column tc"
      " join oceanbase.__all_column pc on pc.table_id = tc.table_id"
      " and pc.column_name = concat('__ai_spec_', tc.column_name)"
      " join oceanbase.__all_column sc on sc.table_id = tc.table_id"
      " and sc.column_name = concat('__ai_state_', tc.column_name)"
      " join oceanbase.__all_column rc on rc.table_id = tc.table_id"
      " and rc.column_name = concat('__ai_request_', tc.column_name)"
      " where tc.is_hidden = 0 and pc.is_hidden = 1)");
}

std::string build_task_view()
{
  std::string sql("(");
  std::vector<ObAIColumnEntry> entries;
  ObAIColumnRegistry::instance().snapshot(entries);
  bool first = true;
  for (size_t i = 0; i < entries.size(); ++i) {
    const ObAIColumnEntry &entry = entries.at(i);
    const share::schema::ObTableSchema *table = nullptr;
    if (!get_table_schema_ptr(entry.db, entry.table, table)) {
      continue;
    }
    const std::string rowkey_predicate = build_rowkey_predicate(*table);
    if (rowkey_predicate.empty()) {
      continue;
    }
    for (size_t j = 0; j < entry.specs.size(); ++j) {
      const ObAIColumnSpec &spec = entry.specs.at(j);
      const share::schema::ObColumnSchemaV2 *result_column =
          table->get_column_schema(common::ObString(static_cast<int32_t>(spec.column_name.size()),
                                                    spec.column_name.c_str()));
      if (nullptr == result_column) {
        continue;
      }
      const std::string request_id = build_request_id_expr(*table, spec.request_column());
      if (request_id.empty()) {
        continue;
      }
      if (!first) {
        sql.append(" union all ");
      }
      first = false;
      sql.append("select ").append(int_to_string(static_cast<int64_t>(table->get_table_id())))
          .append(" as table_id, ")
          .append(int_to_string(static_cast<int64_t>(result_column->get_column_id())))
          .append(" as column_id, ").append(request_id)
          .append(" as request_id, (").append(quote_name(spec.state_column()))
          .append(" div 1000) as state, (").append(quote_name(spec.state_column()))
          .append(" mod 1000) as retry_count, now(6) as eligible_ts, ")
          .append(rowkey_predicate).append(" as rowkey_predicate from ")
          .append(quote_name(entry.db)).append(".").append(quote_name(entry.table))
          .append(" where ").append(quote_name(spec.column_name)).append(" is null and (")
          .append(quote_name(spec.state_column())).append(" div 1000) in (0, 3)");
    }
  }
  if (first) {
    sql.append("select cast(0 as signed) as table_id, cast(0 as signed) as column_id,"
               " cast('' as char(1)) as request_id, cast(0 as signed) as state,"
               " cast(0 as signed) as retry_count, now(6) as eligible_ts,"
               " cast('' as char(1)) as rowkey_predicate"
               " from oceanbase.__all_table where 1 = 0");
  }
  sql.append(")");
  return sql;
}

void collect_view_edits(const std::string &sql, const std::vector<Token> &tokens,
                        std::vector<Edit> &edits)
{
  const std::string ai_column_view = build_ai_column_view();
  std::string task_view;
  bool task_view_ready = false;
  for (size_t i = 0; i < tokens.size(); ++i) {
    const Token &token = tokens.at(i);
    if (TOKEN_IDENT != token.type) {
      continue;
    }
    bool matches_column = 0 == token.text.compare("__all_ai_column");
    bool matches_task = 0 == token.text.compare("__all_ai_column_task");
    if (!matches_column && !matches_task) {
      continue;
    }
    int64_t begin = token.begin;
    int64_t end = token.end;
    if (i >= 2 && is_punct(tokens.at(i - 1), '.') && TOKEN_IDENT == tokens.at(i - 2).type
        && 0 == tokens.at(i - 2).text.compare("oceanbase")) {
      begin = tokens.at(i - 2).begin;
    } else if (i >= 1 && is_punct(tokens.at(i - 1), '.')) {
      continue;
    }
    Edit edit;
    edit.begin = begin;
    edit.end = end;
    if (matches_column) {
      edit.text = ai_column_view;
    } else {
      if (!task_view_ready) {
        task_view = build_task_view();
        task_view_ready = true;
      }
      edit.text = task_view;
    }
    edits.push_back(edit);
  }
  (void)sql;
}

std::string apply_edits(const std::string &sql, std::vector<Edit> &edits)
{
  std::sort(edits.begin(), edits.end(), [](const Edit &lhs, const Edit &rhs) {
    return lhs.begin < rhs.begin;
  });
  std::string out;
  out.reserve(sql.size() + 1024);
  int64_t pos = 0;
  for (size_t i = 0; i < edits.size(); ++i) {
    const Edit &edit = edits.at(i);
    if (edit.begin < pos || edit.end < edit.begin) {
      continue;
    }
    out.append(sql, static_cast<size_t>(pos), static_cast<size_t>(edit.begin - pos));
    out.append(edit.text);
    pos = edit.end;
  }
  out.append(sql, static_cast<size_t>(pos), sql.size() - static_cast<size_t>(pos));
  return out;
}

int resolve_table_ids(const std::string &db, const std::string &table,
                      const std::string &column, uint64_t &table_id, uint64_t &column_id)
{
  int ret = OB_SUCCESS;
  table_id = 0;
  column_id = 0;
  const share::schema::ObTableSchema *table_schema = nullptr;
  if (get_table_schema_ptr(db, table, table_schema) && nullptr != table_schema) {
    table_id = table_schema->get_table_id();
    const share::schema::ObColumnSchemaV2 *column_schema = table_schema->get_column_schema(
        common::ObString(static_cast<int32_t>(column.size()), column.c_str()));
    if (nullptr != column_schema) {
      column_id = column_schema->get_column_id();
    }
  }
  return ret;
}

int check_strict_read(const std::string &sql, const std::vector<Token> &tokens,
                      const ObSQLSessionInfo &session, bool &blocked)
{
  int ret = OB_SUCCESS;
  blocked = false;
  bool strict = false;
  std::vector<std::string> idents;
  if (OB_FAIL(session.get_ai_column_strict_read(strict))) {
  } else if (!strict) {
  } else {
    for (size_t i = 0; i < tokens.size(); ++i) {
      const Token &token = tokens.at(i);
      if (TOKEN_IDENT == token.type || TOKEN_QUOTED_IDENT == token.type) {
        idents.push_back(to_lower_copy(token.text));
      }
    }
    std::vector<ObAIColumnEntry> entries;
    ObAIColumnRegistry::instance().snapshot(entries);
    const common::ObString session_db = session.get_database_name();
    const std::string default_db = (nullptr == session_db.ptr() || session_db.empty())
        ? std::string() : to_lower_copy(std::string(session_db.ptr(),
                                                    static_cast<size_t>(session_db.length())));
    for (size_t i = 0; OB_SUCC(ret) && !blocked && i < entries.size(); ++i) {
      const ObAIColumnEntry &entry = entries.at(i);
      const share::schema::ObTableSchema *table_schema = nullptr;
      // A registered name only identifies the table when its database is part
      // of the statement or is the database the session works in.
      const bool db_matches = contains_identifier(idents, entry.db)
          || (!default_db.empty() && default_db == to_lower_copy(entry.db));
      if (!db_matches || !contains_identifier(idents, entry.table)
          || !get_table_schema_ptr(entry.db, entry.table, table_schema)) {
        continue;
      }
      for (size_t j = 0; OB_SUCC(ret) && !blocked && j < entry.specs.size(); ++j) {
        const ObAIColumnSpec &spec = entry.specs.at(j);
        if (!contains_identifier(idents, spec.column_name)
            || !ObAIColumnRegistry::instance().is_unfinished(entry.db, entry.table,
                                                             spec.column_name)) {
          continue;
        }
        uint64_t table_id = 0;
        uint64_t column_id = 0;
        resolve_table_ids(entry.db, entry.table, spec.column_name, table_id, column_id);
        blocked = true;
        ret = OB_ERR_AI_COLUMN_UNFINISHED;
        LOG_USER_ERROR(OB_ERR_AI_COLUMN_UNFINISHED, table_id, column_id);
        LOG_WARN("ai column strict read rejected", K(ret), "table", entry.table.c_str(),
                 "column", spec.column_name.c_str());
      }
    }
  }
  (void)sql;
  return ret;
}

int rewrite_create_table(const std::string &sql, const std::vector<Token> &tokens,
                         const std::string &default_db, std::vector<Edit> &edits,
                         bool &is_ai_statement)
{
  int ret = OB_SUCCESS;
  int64_t table_tok = -1;
  for (int64_t i = 1; i < static_cast<int64_t>(tokens.size()) && table_tok < 0; ++i) {
    if (is_word(tokens.at(static_cast<size_t>(i)), "table")) {
      table_tok = i;
    } else if (is_word(tokens.at(static_cast<size_t>(i)), "select")
               || is_word(tokens.at(static_cast<size_t>(i)), "like")) {
      break;
    }
  }
  if (table_tok < 0) {
  } else {
    int64_t name_tok = table_tok + 1;
    if (name_tok + 2 < static_cast<int64_t>(tokens.size())
        && is_word(tokens.at(static_cast<size_t>(name_tok)), "if")
        && is_word(tokens.at(static_cast<size_t>(name_tok + 1)), "not")
        && is_word(tokens.at(static_cast<size_t>(name_tok + 2)), "exists")) {
      name_tok += 3;
    }
    TableRef ref;
    if (!parse_table_ref(sql, tokens, name_tok, default_db, ref)) {
    } else {
      const int64_t open_tok = ref.last_tok + 1;
      int64_t close_tok = -1;
      if (open_tok >= static_cast<int64_t>(tokens.size())
          || !is_punct(tokens.at(static_cast<size_t>(open_tok)), '(')
          || !find_matching_paren(tokens, open_tok, close_tok)) {
      } else {
        std::vector<std::pair<int64_t, int64_t> > segments;
        split_top_level(tokens, open_tok + 1, close_tok, segments);
        std::vector<ObAIColumnSpec> specs;
        for (size_t i = 0; OB_SUCC(ret) && i < segments.size(); ++i) {
          const int64_t first = segments.at(i).first;
          const int64_t limit = segments.at(i).second;
          if (first >= limit) {
            continue;
          }
          ObAIColumnSpec spec;
          std::string replacement;
          int64_t after_tok = limit;
          bool is_ai = false;
          if (OB_FAIL(parse_ai_column(sql, tokens, first, limit, "", spec, replacement, after_tok,
                                      is_ai))) {
          } else if (!is_ai) {
          } else {
            Edit edit;
            edit.begin = tokens.at(static_cast<size_t>(first)).begin;
            edit.end = tokens.at(static_cast<size_t>(after_tok - 1)).end;
            edit.text = replacement;
            edits.push_back(edit);
            specs.push_back(spec);
          }
        }
        if (OB_SUCC(ret) && !specs.empty()) {
          is_ai_statement = true;
          if (check_ai_dependency(specs, -1)) {
            ret = OB_NOT_SUPPORTED;
          } else {
            ObAIColumnRegistry::instance().register_table(ref.db, ref.table, specs);
            ObAIColumnWorker::get_instance().start();
          }
        }
      }
    }
  }
  return ret;
}

int rewrite_alter_table(const std::string &sql, const std::vector<Token> &tokens,
                        const std::string &default_db, std::vector<Edit> &edits,
                        bool &is_ai_statement)
{
  int ret = OB_SUCCESS;
  int64_t table_tok = -1;
  for (int64_t i = 1; i < static_cast<int64_t>(tokens.size()) && table_tok < 0; ++i) {
    if (is_word(tokens.at(static_cast<size_t>(i)), "table")) {
      table_tok = i;
    }
  }
  if (table_tok < 0) {
  } else {
    TableRef ref;
    if (!parse_table_ref(sql, tokens, table_tok + 1, default_db, ref)) {
    } else {
      int64_t action_tok = ref.last_tok + 1;
      if (action_tok < static_cast<int64_t>(tokens.size())
          && is_word(tokens.at(static_cast<size_t>(action_tok)), "retry")) {
        is_ai_statement = true;
        ret = rewrite_alter_retry(sql, tokens, ref, action_tok, edits);
      } else if (action_tok < static_cast<int64_t>(tokens.size())
                 && is_word(tokens.at(static_cast<size_t>(action_tok)), "add")) {
        is_ai_statement = true;
        ret = rewrite_alter_add(sql, tokens, ref, action_tok, edits);
      }
    }
  }
  return ret;
}

int rewrite_alter_add(const std::string &sql, const std::vector<Token> &tokens,
                      const TableRef &ref, const int64_t action_tok, std::vector<Edit> &edits)
{
  int ret = OB_SUCCESS;
  int64_t first_tok = action_tok + 1;
  if (first_tok < static_cast<int64_t>(tokens.size())
      && is_word(tokens.at(static_cast<size_t>(first_tok)), "column")) {
    ++first_tok;
  }
  int64_t limit_tok = static_cast<int64_t>(tokens.size());
  while (limit_tok > first_tok && is_punct(tokens.at(static_cast<size_t>(limit_tok - 1)), ';')) {
    --limit_tok;
  }
  bool multi_action = false;
  int64_t depth = 0;
  for (int64_t i = first_tok; i < limit_tok; ++i) {
    if (is_punct(tokens.at(static_cast<size_t>(i)), '(')) {
      ++depth;
    } else if (is_punct(tokens.at(static_cast<size_t>(i)), ')')) {
      --depth;
    } else if (0 == depth && is_punct(tokens.at(static_cast<size_t>(i)), ',')) {
      multi_action = true;
    }
  }
  if (first_tok >= limit_tok || multi_action) {
  } else {
    ObAIColumnSpec spec;
    std::string replacement;
    int64_t after_tok = limit_tok;
    bool is_ai = false;
    if (OB_FAIL(parse_ai_column(sql, tokens, first_tok, limit_tok, "add column ", spec,
                                replacement, after_tok, is_ai))) {
    } else if (!is_ai) {
    } else {
      ObAIColumnEntry entry;
      std::vector<ObAIColumnSpec> specs;
      specs.push_back(spec);
      if (ObAIColumnRegistry::instance().find_table(ref.db, ref.table, entry)) {
        specs.insert(specs.end(), entry.specs.begin(), entry.specs.end());
      }
      if (check_ai_dependency(specs, 0)) {
        ret = OB_NOT_SUPPORTED;
      } else {
        Edit edit;
        edit.begin = tokens.at(static_cast<size_t>(first_tok)).begin;
        edit.end = tokens.at(static_cast<size_t>(after_tok - 1)).end;
        edit.text = replacement;
        edits.push_back(edit);
        ObAIColumnRegistry::instance().register_table(ref.db, ref.table, specs);
        ObAIColumnWorker::get_instance().start();
      }
    }
  }
  return ret;
}

int rewrite_alter_retry(const std::string &sql, const std::vector<Token> &tokens,
                        const TableRef &ref, const int64_t retry_tok, std::vector<Edit> &edits)
{
  int ret = OB_SUCCESS;
  int64_t column_tok = retry_tok + 1;
  if (column_tok + 2 >= static_cast<int64_t>(tokens.size())
      || !is_word(tokens.at(static_cast<size_t>(column_tok)), "ai")
      || !is_word(tokens.at(static_cast<size_t>(column_tok + 1)), "column")
      || !is_name_token(tokens.at(static_cast<size_t>(column_tok + 2)))) {
    ret = OB_NOT_SUPPORTED;
  } else {
    column_tok += 2;
    const std::string column_name = token_name(sql, tokens.at(static_cast<size_t>(column_tok)));
    ObAIColumnEntry entry;
    const ObAIColumnSpec *spec = nullptr;
    if (!ObAIColumnRegistry::instance().find_table(ref.db, ref.table, entry)) {
    } else {
      for (size_t i = 0; i < entry.specs.size() && nullptr == spec; ++i) {
        if (equals_ci(entry.specs.at(i).column_name, column_name)) {
          spec = &entry.specs.at(i);
        }
      }
    }
    if (nullptr == spec) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "retrying a column that is not an AI column");
    } else {
      int64_t predicate_begin = -1;
      int64_t predicate_end = static_cast<int64_t>(tokens.size());
      int64_t depth = 0;
      bool combined = false;
      for (int64_t i = column_tok + 1; i < static_cast<int64_t>(tokens.size()); ++i) {
        const Token &token = tokens.at(static_cast<size_t>(i));
        if (is_punct(token, '(')) {
          ++depth;
        } else if (is_punct(token, ')')) {
          --depth;
        } else if (0 == depth && is_punct(token, ',')) {
          combined = true;
        } else if (0 == depth && is_word(token, "where") && predicate_begin < 0) {
          predicate_begin = i + 1;
        } else if (0 == depth && is_punct(token, ';')) {
          predicate_end = i;
          break;
        }
      }
      if (combined) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "combining AI COLUMN RETRY with another ALTER action");
      } else {
        std::string predicate;
        if (predicate_begin >= 0 && predicate_begin < predicate_end) {
          // STATUS reads the row's task state: a published value is READY, a
          // scheduled retry is RETRY, a terminal failure is FAILED, and the
          // remaining rows still wait for their first answer.
          const std::string status_case =
              std::string("(case when `") + spec->column_name + "` is not null then 'READY' when ("
              + "(`" + spec->state_column() + "` div 1000) = 3) then 'RETRY' when ("
              + "(`" + spec->state_column() + "` div 1000) = 2) then 'FAILED' else 'PENDING' end)";
          std::string built;
          int64_t pos = tokens.at(static_cast<size_t>(predicate_begin)).begin;
          for (int64_t i = predicate_begin; i < predicate_end; ++i) {
            const Token &token = tokens.at(static_cast<size_t>(i));
            built.append(sql, static_cast<size_t>(pos), static_cast<size_t>(token.begin - pos));
            const bool is_status = TOKEN_IDENT == token.type
                                   && 0 == token.text.compare("status")
                                   && !(i > 0 && is_punct(tokens.at(static_cast<size_t>(i - 1)), '.'));
            if (is_status) {
              built.append(status_case);
            } else {
              built.append(sql, static_cast<size_t>(token.begin),
                           static_cast<size_t>(token.end - token.begin));
            }
            pos = token.end;
          }
          built.append(sql, static_cast<size_t>(pos),
                       static_cast<size_t>(tokens.at(static_cast<size_t>(predicate_end - 1)).end - pos));
          predicate = built;
        }
        std::string update("update ");
        update.append(quote_name(ref.db)).append(".").append(quote_name(ref.table))
            .append(" set `").append(spec->column_name).append("` = null, `")
            .append(spec->state_column()).append("` = 0, `").append(spec->request_column())
            .append("` = `").append(spec->request_column()).append("` + 1");
        if (!predicate.empty()) {
          update.append(" where ").append(predicate);
        }
        Edit edit;
        edit.begin = 0;
        edit.end = static_cast<int64_t>(sql.size());
        edit.text = update;
        edits.push_back(edit);
        ObAIColumnRegistry::instance().set_unfinished(ref.db, ref.table, spec->column_name, true);
        ObAIColumnWorker::get_instance().start();
      }
    }
  }
  return ret;
}

int rewrite_update(const std::string &sql, const std::vector<Token> &tokens,
                   const std::string &default_db, std::vector<Edit> &edits)
{
  int ret = OB_SUCCESS;
  int64_t start_tok = 1;
  while (start_tok < static_cast<int64_t>(tokens.size())
         && (is_word(tokens.at(static_cast<size_t>(start_tok)), "low_priority")
             || is_word(tokens.at(static_cast<size_t>(start_tok)), "ignore"))) {
    ++start_tok;
  }
  TableRef ref;
  if (!parse_table_ref(sql, tokens, start_tok, default_db, ref)) {
  } else if (ref.last_tok + 1 >= static_cast<int64_t>(tokens.size())
             || !is_word(tokens.at(static_cast<size_t>(ref.last_tok + 1)), "set")) {
    // multi table update or unsupported shape
  } else {
    const int64_t set_tok = ref.last_tok + 1;
    int64_t assign_end = static_cast<int64_t>(tokens.size());
    int64_t depth = 0;
    for (int64_t i = set_tok + 1; i < static_cast<int64_t>(tokens.size()); ++i) {
      const Token &token = tokens.at(static_cast<size_t>(i));
      if (is_punct(token, '(')) {
        ++depth;
      } else if (is_punct(token, ')')) {
        --depth;
      } else if (0 == depth
                 && (is_word(token, "where") || is_word(token, "order")
                     || is_word(token, "limit") || is_punct(token, ';'))) {
        assign_end = i;
        break;
      }
    }
    std::vector<std::pair<int64_t, int64_t> > assignments;
    split_top_level(tokens, set_tok + 1, assign_end, assignments);
    std::vector<std::string> assigned;
    for (size_t i = 0; i < assignments.size(); ++i) {
      const int64_t from = assignments.at(i).first;
      const int64_t to = assignments.at(i).second;
      int64_t eq_tok = -1;
      int64_t inner = 0;
      for (int64_t j = from; j < to && eq_tok < 0; ++j) {
        const Token &token = tokens.at(static_cast<size_t>(j));
        if (is_punct(token, '(')) {
          ++inner;
        } else if (is_punct(token, ')')) {
          --inner;
        } else if (0 == inner && is_punct(token, '=')) {
          eq_tok = j;
        }
      }
      for (int64_t j = (eq_tok < 0 ? to : eq_tok) - 1; j >= from; --j) {
        if (is_name_token(tokens.at(static_cast<size_t>(j)))) {
          if (!(j > from && is_punct(tokens.at(static_cast<size_t>(j - 1)), '.'))) {
            assigned.push_back(to_lower_copy(token_name(sql, tokens.at(static_cast<size_t>(j)))));
          }
          break;
        }
      }
    }
    ObAIColumnEntry entry;
    if (assigned.empty()
        || !ObAIColumnRegistry::instance().find_table(ref.db, ref.table, entry)) {
    } else {
      const share::schema::ObTableSchema *table_schema = nullptr;
      std::vector<std::string> rowkey_names;
      if (get_table_schema_ptr(ref.db, ref.table, table_schema)) {
        common::ObSEArray<uint64_t, 4> rowkey_ids;
        if (OB_SUCCESS == table_schema->get_rowkey_column_ids(rowkey_ids)) {
          for (int64_t i = 0; i < rowkey_ids.count(); ++i) {
            const share::schema::ObColumnSchemaV2 *column =
                table_schema->get_column_schema(rowkey_ids.at(i));
            if (nullptr != column) {
              rowkey_names.push_back(to_lower_copy(std::string(
                  column->get_column_name_str().ptr(),
                  static_cast<size_t>(column->get_column_name_str().length()))));
            }
          }
        }
      }
      bool pk_assigned = false;
      for (size_t i = 0; !pk_assigned && i < rowkey_names.size(); ++i) {
        if (contains_identifier(assigned, rowkey_names.at(i))) {
          pk_assigned = true;
        }
      }
      std::string invalidation;
      int64_t invalidated = 0;
      for (size_t i = 0; i < entry.specs.size(); ++i) {
        const ObAIColumnSpec &spec = entry.specs.at(i);
        std::vector<std::string> idents;
        collect_identifiers(spec.input_expr, idents);
        bool hit = pk_assigned;
        if (!hit && (contains_identifier(assigned, spec.column_name)
                     || contains_identifier(assigned, spec.state_column())
                     || contains_identifier(assigned, spec.request_column()))) {
          continue;
        }
        for (size_t j = 0; !hit && j < idents.size(); ++j) {
          if (contains_identifier(assigned, idents.at(j))) {
            hit = true;
          }
        }
        if (hit) {
          invalidation.append(", `").append(spec.column_name).append("` = null, `")
              .append(spec.state_column()).append("` = 0, `").append(spec.request_column())
              .append("` = `").append(spec.request_column()).append("` + 1");
          ++invalidated;
          ObAIColumnRegistry::instance().set_unfinished(ref.db, ref.table, spec.column_name, true);
        }
      }
      if (invalidated > 0) {
        Edit edit;
        edit.begin = tokens.at(static_cast<size_t>(assignments.back().second - 1)).end;
        edit.end = edit.begin;
        edit.text = invalidation;
        edits.push_back(edit);
        ObAIColumnWorker::get_instance().start();
      }
    }
  }
  return ret;
}

int rewrite_insert(const std::string &sql, const std::vector<Token> &tokens,
                   const std::string &default_db)
{
  int ret = OB_SUCCESS;
  int64_t start_tok = 1;
  while (start_tok < static_cast<int64_t>(tokens.size())
         && (is_word(tokens.at(static_cast<size_t>(start_tok)), "low_priority")
             || is_word(tokens.at(static_cast<size_t>(start_tok)), "delayed")
             || is_word(tokens.at(static_cast<size_t>(start_tok)), "high_priority")
             || is_word(tokens.at(static_cast<size_t>(start_tok)), "ignore")
             || is_word(tokens.at(static_cast<size_t>(start_tok)), "into"))) {
    ++start_tok;
  }
  TableRef ref;
  if (!parse_table_ref(sql, tokens, start_tok, default_db, ref)) {
  } else if (ObAIColumnRegistry::instance().has_table(ref.db, ref.table)) {
    ObAIColumnRegistry::instance().set_all_unfinished(ref.db, ref.table, true);
    ObAIColumnWorker::get_instance().start();
  }
  (void)sql;
  return ret;
}

} // namespace

int ObAIColumnRewriter::rewrite(common::ObIAllocator &allocator,
                                const ObSQLSessionInfo &session,
                                const common::ObString &sql,
                                common::ObString &rewritten_sql,
                                bool &rewritten)
{
  int ret = OB_SUCCESS;
  rewritten = false;
  bool blocked = false;
  bool strict_read = false;
  std::string text(sql.ptr(), static_cast<size_t>(sql.length()));
  std::vector<Token> tokens;
  std::vector<Edit> edits;
  std::string default_db;
  bool is_ai_statement = false;
  bool needs_rewrite = false;
  if (sql.empty() || session.is_inner()) {
  } else if (OB_FAIL(session.get_ai_column_strict_read(strict_read))) {
  } else if (strict_read) {
    // Strict reads are a session wide promise, so every statement is inspected.
    needs_rewrite = true;
  } else if (contains_ci(text, "ai column") || contains_ci(text, "__all_ai_column")) {
    needs_rewrite = true;
  } else if (starts_with_word_ci(text, "update") || starts_with_word_ci(text, "insert")
             || starts_with_word_ci(text, "replace")) {
    // An ordinary DML statement only matters when it can touch a registered table.
    needs_rewrite = contains_registered_table(text);
  }
  if (!needs_rewrite) {
  } else if (!tokenize(text, tokens) || tokens.empty()) {
  } else {
    const common::ObString session_db = session.get_database_name();
    default_db.assign(session_db.ptr(), static_cast<size_t>(session_db.length()));
    bool is_ddl = is_word(tokens.at(0), "create") || is_word(tokens.at(0), "alter")
                  || is_word(tokens.at(0), "drop") || is_word(tokens.at(0), "truncate");
    if (!is_ddl) {
      if (OB_FAIL(check_strict_read(text, tokens, session, blocked))) {
      }
    }
    if (OB_SUCC(ret) && is_word(tokens.at(0), "create")) {
      ret = rewrite_create_table(text, tokens, default_db, edits, is_ai_statement);
    } else if (OB_SUCC(ret) && is_word(tokens.at(0), "alter")) {
      ret = rewrite_alter_table(text, tokens, default_db, edits, is_ai_statement);
    } else if (OB_SUCC(ret) && is_word(tokens.at(0), "update")) {
      ret = rewrite_update(text, tokens, default_db, edits);
    } else if (OB_SUCC(ret)
               && (is_word(tokens.at(0), "insert") || is_word(tokens.at(0), "replace"))) {
      ret = rewrite_insert(text, tokens, default_db);
    }
    if (OB_SUCC(ret) && !is_ai_statement) {
      collect_view_edits(text, tokens, edits);
    }
    if (OB_SUCC(ret) && !edits.empty()) {
      std::string rewritten_text = apply_edits(text, edits);
      char *buffer = static_cast<char *>(allocator.alloc(rewritten_text.size() + 1));
      if (nullptr == buffer) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(buffer, rewritten_text.c_str(), rewritten_text.size());
        buffer[rewritten_text.size()] = '\0';
        rewritten_sql.assign(buffer, static_cast<int32_t>(rewritten_text.size()));
        rewritten = true;
      }
    }
  }
  return ret;
}

} // namespace ai_column
} // namespace sql
} // namespace oceanbase
