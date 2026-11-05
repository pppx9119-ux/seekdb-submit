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

#include "sql/ob_file_sql_rewriter.h"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <glob.h>
#include <string>
#include <sys/stat.h>
#include <utility>
#include <vector>

#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace sql
{
namespace file_sql_impl
{

// A file is expanded into one SELECT branch per row, so the rewritten text
// grows with the row count. Bound the bytes this reader will hold for a single
// file: a larger scan would exhaust memory in the parser anyway, and a clear
// error is better than an allocation failure.
static const size_t FILE_SQL_MAX_MATERIALIZED_BYTES = 128 * 1024 * 1024;

enum FileSqlType
{
  FILE_SQL_NULL = 0,
  FILE_SQL_BIGINT,
  FILE_SQL_DOUBLE,
  FILE_SQL_BOOLEAN,
  FILE_SQL_DATE,
  FILE_SQL_DATETIME,
  FILE_SQL_VARCHAR
};

enum FileSqlKind
{
  FILE_SQL_CSV = 0,
  FILE_SQL_JSON,
  FILE_SQL_PARQUET,
  FILE_SQL_GLOB
};

struct FileSqlColumn
{
  FileSqlColumn() : type(FILE_SQL_NULL), nullable(false) {}
  std::string name;
  FileSqlType type;
  bool nullable;
};

struct FileSqlCell
{
  FileSqlCell() : is_null(true), type(FILE_SQL_NULL), quoted(false) {}
  bool is_null;
  FileSqlType type;
  std::string text;
  bool quoted;
};

struct FileSqlTable
{
  std::vector<FileSqlColumn> columns;
  std::vector<std::vector<FileSqlCell> > rows;
};

struct FileSqlOptions
{
  FileSqlOptions()
      : header(0),
        has_format(false),
        has_delimiter(false),
        has_quote(false),
        has_escape(false),
        has_nullstr(false) {}
  int header; // 0: auto, 1: header present, 2: no header
  bool has_format;
  std::string format;
  bool has_delimiter;
  std::string delimiter;
  bool has_quote;
  std::string quote;
  bool has_escape;
  std::string escape;
  bool has_nullstr;
  std::string nullstr;
};

static bool is_ident_start(const char c)
{
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
}

static bool is_ident_char(const char c)
{
  return is_ident_start(c) || (c >= '0' && c <= '9') || c == '$';
}

static char lower_char(const char c)
{
  return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
}

static bool equal_ci(const std::string &s, const char *lit)
{
  size_t i = 0;
  for (; i < s.size() && lit[i] != '\0'; ++i) {
    if (lower_char(s[i]) != lit[i]) {
      return false;
    }
  }
  return i == s.size() && lit[i] == '\0';
}

static std::string trim(const std::string &s)
{
  size_t b = 0;
  size_t e = s.size();
  while (b < e && (s[b] == ' ' || s[b] == '\t' || s[b] == '\r' || s[b] == '\n' ||
                   s[b] == '\f' || s[b] == '\v')) {
    ++b;
  }
  while (e > b && (s[e - 1] == ' ' || s[e - 1] == '\t' || s[e - 1] == '\r' ||
                   s[e - 1] == '\n' || s[e - 1] == '\f' || s[e - 1] == '\v')) {
    --e;
  }
  return s.substr(b, e - b);
}

static void skip_space_comments(const std::string &s, size_t &i)
{
  const size_t n = s.size();
  for (;;) {
    while (i < n && (s[i] == ' ' || s[i] == '\t' || s[i] == '\r' || s[i] == '\n' ||
                     s[i] == '\f' || s[i] == '\v')) {
      ++i;
    }
    if (i + 1 < n && s[i] == '-' && s[i + 1] == '-') {
      i += 2;
      while (i < n && s[i] != '\n') {
        ++i;
      }
      continue;
    }
    if (i < n && s[i] == '#') {
      while (i < n && s[i] != '\n') {
        ++i;
      }
      continue;
    }
    if (i + 1 < n && s[i] == '/' && s[i + 1] == '*') {
      i += 2;
      while (i + 1 < n && !(s[i] == '*' && s[i + 1] == '/')) {
        ++i;
      }
      i = (i + 1 < n) ? i + 2 : n;
      continue;
    }
    break;
  }
}

static bool skip_quoted(const std::string &s, size_t &i)
{
  const size_t n = s.size();
  const char quote = s[i];
  ++i;
  while (i < n) {
    if (s[i] == '\\' && quote != '`') {
      i += (i + 1 < n) ? 2 : 1;
      continue;
    }
    if (s[i] == quote) {
      if (i + 1 < n && s[i + 1] == quote) {
        i += 2;
        continue;
      }
      ++i;
      return true;
    }
    ++i;
  }
  return false;
}

static bool parse_string_literal(const std::string &in, std::string &out)
{
  const std::string s = trim(in);
  out.clear();
  if (s.size() < 2) {
    return false;
  }
  const char quote = s[0];
  if (quote != '\'' && quote != '"') {
    return false;
  }
  size_t i = 1;
  while (i < s.size()) {
    const char c = s[i];
    if (c == '\\' && i + 1 < s.size()) {
      const char e = s[i + 1];
      i += 2;
      switch (e) {
        case '0': out.push_back('\0'); break;
        case 'b': out.push_back('\b'); break;
        case 'n': out.push_back('\n'); break;
        case 'r': out.push_back('\r'); break;
        case 't': out.push_back('\t'); break;
        case 'Z': out.push_back(26); break;
        case '%': out.push_back('%'); break;
        case '_': out.push_back('_'); break;
        default: out.push_back(e); break;
      }
      continue;
    }
    if (c == quote) {
      if (i + 1 < s.size() && s[i + 1] == quote) {
        out.push_back(quote);
        i += 2;
        continue;
      }
      ++i;
      return i == s.size();
    }
    out.push_back(c);
    ++i;
  }
  return false;
}

static std::string quote_sql_string(const std::string &v)
{
  std::string out;
  out.reserve(v.size() + 2);
  out.push_back('\'');
  for (size_t i = 0; i < v.size(); ++i) {
    const unsigned char c = static_cast<unsigned char>(v[i]);
    switch (c) {
      case '\0': out += "\\0"; break;
      case '\'': out += "''"; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      case 26: out += "\\Z"; break;
      default: out.push_back(static_cast<char>(c)); break;
    }
  }
  out.push_back('\'');
  return out;
}

static std::string quote_ident(const std::string &v)
{
  std::string out;
  out.reserve(v.size() + 2);
  out.push_back('`');
  for (size_t i = 0; i < v.size(); ++i) {
    out.push_back(v[i]);
    if (v[i] == '`') {
      out.push_back('`');
    }
  }
  out.push_back('`');
  return out;
}

static bool find_matching_paren(const std::string &s, const size_t open, size_t &close)
{
  const size_t n = s.size();
  if (open >= n || s[open] != '(') {
    return false;
  }
  int depth = 0;
  size_t i = open;
  while (i < n) {
    skip_space_comments(s, i);
    if (i >= n) {
      break;
    }
    if (s[i] == '\'' || s[i] == '"' || s[i] == '`') {
      if (!skip_quoted(s, i)) {
        return false;
      }
      continue;
    }
    if (s[i] == '(') {
      ++depth;
      ++i;
    } else if (s[i] == ')') {
      --depth;
      ++i;
      if (depth == 0) {
        close = i - 1;
        return true;
      }
    } else {
      ++i;
    }
  }
  return false;
}

static bool split_top_level(const std::string &s, const char sep, std::vector<std::string> &parts)
{
  parts.clear();
  const size_t n = s.size();
  size_t start = 0;
  int depth = 0;
  size_t i = 0;
  while (i < n) {
    skip_space_comments(s, i);
    if (i >= n) {
      break;
    }
    if (s[i] == '\'' || s[i] == '"' || s[i] == '`') {
      if (!skip_quoted(s, i)) {
        return false;
      }
      continue;
    }
    if (s[i] == '(') {
      ++depth;
      ++i;
    } else if (s[i] == ')') {
      --depth;
      ++i;
    } else if (s[i] == sep && depth == 0) {
      parts.push_back(s.substr(start, i - start));
      ++i;
      start = i;
    } else {
      ++i;
    }
  }
  parts.push_back(s.substr(start));
  return true;
}

static bool find_top_level_equal(const std::string &s, size_t &pos)
{
  const size_t n = s.size();
  int depth = 0;
  size_t i = 0;
  while (i < n) {
    skip_space_comments(s, i);
    if (i >= n) {
      break;
    }
    if (s[i] == '\'' || s[i] == '"' || s[i] == '`') {
      if (!skip_quoted(s, i)) {
        return false;
      }
      continue;
    }
    if (s[i] == '(') {
      ++depth;
      ++i;
    } else if (s[i] == ')') {
      --depth;
      ++i;
    } else if (s[i] == '=' && depth == 0) {
      pos = i;
      return true;
    } else {
      ++i;
    }
  }
  return false;
}

static bool is_integer_text(const std::string &t)
{
  size_t i = 0;
  if (i < t.size() && (t[i] == '+' || t[i] == '-')) {
    ++i;
  }
  if (i >= t.size()) {
    return false;
  }
  for (; i < t.size(); ++i) {
    if (t[i] < '0' || t[i] > '9') {
      return false;
    }
  }
  // An integer outside the BIGINT range cannot be represented by the inferred
  // type. Report it as non-integer so it widens to DOUBLE instead of reaching
  // the engine as a literal it would silently clip.
  errno = 0;
  char *end = NULL;
  ::strtoll(t.c_str(), &end, 10);
  return 0 == errno && NULL != end && '\0' == *end;
}

static bool is_double_text(const std::string &t)
{
  size_t i = 0;
  if (i < t.size() && (t[i] == '+' || t[i] == '-')) {
    ++i;
  }
  bool has_digit = false;
  bool has_dot = false;
  bool has_exp = false;
  for (; i < t.size(); ++i) {
    const char c = t[i];
    if (c >= '0' && c <= '9') {
      has_digit = true;
    } else if (c == '.' && !has_dot && !has_exp) {
      has_dot = true;
    } else if ((c == 'e' || c == 'E') && has_digit && !has_exp) {
      has_exp = true;
      if (i + 1 < t.size() && (t[i + 1] == '+' || t[i + 1] == '-')) {
        ++i;
      }
      if (i + 1 >= t.size()) {
        return false;
      }
    } else {
      return false;
    }
  }
  return has_digit;
}

static bool is_bool_text(const std::string &t)
{
  return equal_ci(t, "true") || equal_ci(t, "false");
}

static bool is_date_text(const std::string &t)
{
  if (t.size() != 10 || t[4] != '-' || t[7] != '-') {
    return false;
  }
  for (size_t i = 0; i < t.size(); ++i) {
    if (i != 4 && i != 7 && (t[i] < '0' || t[i] > '9')) {
      return false;
    }
  }
  const int year = (t[0] - '0') * 1000 + (t[1] - '0') * 100 + (t[2] - '0') * 10 + (t[3] - '0');
  const int month = (t[5] - '0') * 10 + (t[6] - '0');
  const int day = (t[8] - '0') * 10 + (t[9] - '0');
  if (month < 1 || month > 12 || day < 1) {
    return false;
  }
  static const int month_days[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  int max_day = month_days[month - 1];
  const bool leap = (0 == year % 4 && 0 != year % 100) || 0 == year % 400;
  if (2 == month && leap) {
    max_day = 29;
  }
  return day <= max_day;
}

static bool is_datetime_text(const std::string &t)
{
  // Accept a fractional seconds part as well as the plain second precision so
  // timestamps written by other tools keep their DATETIME type.
  size_t second_end = 19;
  if (t.size() > 19 && '.' == t[19]) {
    second_end = 20;
    while (second_end < t.size() && t[second_end] >= '0' && t[second_end] <= '9') {
      ++second_end;
    }
    if (second_end == 20) {
      return false;
    }
  }
  if (t.size() != second_end || (t[10] != ' ' && t[10] != 'T') || t[13] != ':' || t[16] != ':') {
    return false;
  }
  for (size_t i = 0; i < second_end; ++i) {
    if (i == 4 || i == 7 || i == 10 || i == 13 || i == 16 || i == 19) {
      continue;
    }
    if (t[i] < '0' || t[i] > '9') {
      return false;
    }
  }
  const int hour = (t[11] - '0') * 10 + (t[12] - '0');
  const int minute = (t[14] - '0') * 10 + (t[15] - '0');
  const int second = (t[17] - '0') * 10 + (t[18] - '0');
  return is_date_text(t.substr(0, 10)) && hour < 24 && minute < 60 && second < 60;
}

static FileSqlType merge_type(const FileSqlType a, const FileSqlType b)
{
  if (FILE_SQL_NULL == a) {
    return b;
  }
  if (FILE_SQL_NULL == b || a == b) {
    return a;
  }
  if ((FILE_SQL_BIGINT == a && FILE_SQL_DOUBLE == b) ||
      (FILE_SQL_DOUBLE == a && FILE_SQL_BIGINT == b)) {
    return FILE_SQL_DOUBLE;
  }
  return FILE_SQL_VARCHAR;
}

static FileSqlType infer_cell_type(const FileSqlCell &cell)
{
  if (cell.is_null) {
    return FILE_SQL_NULL;
  }
  if (FILE_SQL_VARCHAR == cell.type || cell.quoted) {
    return FILE_SQL_VARCHAR;
  }
  const std::string &t = cell.text;
  if (is_integer_text(t)) {
    return FILE_SQL_BIGINT;
  }
  if (is_double_text(t)) {
    return FILE_SQL_DOUBLE;
  }
  if (is_bool_text(t)) {
    return FILE_SQL_BOOLEAN;
  }
  if (is_date_text(t)) {
    return FILE_SQL_DATE;
  }
  if (is_datetime_text(t)) {
    return FILE_SQL_DATETIME;
  }
  return FILE_SQL_VARCHAR;
}

static const char *type_name(const FileSqlType type)
{
  switch (type) {
    case FILE_SQL_BIGINT: return "BIGINT";
    case FILE_SQL_DOUBLE: return "DOUBLE";
    case FILE_SQL_BOOLEAN: return "BOOLEAN";
    case FILE_SQL_DATE: return "DATE";
    case FILE_SQL_DATETIME: return "DATETIME";
    case FILE_SQL_VARCHAR: return "VARCHAR";
    default: return "VARCHAR";
  }
}

static bool read_whole_file(const std::string &path, std::string &data)
{
  bool ok = false;
  FILE *fp = ::fopen(path.c_str(), "rb");
  if (NULL != fp) {
    if (0 == ::fseek(fp, 0, SEEK_END)) {
      const long len = ::ftell(fp);
      if (len >= 0 && static_cast<size_t>(len) <= FILE_SQL_MAX_MATERIALIZED_BYTES
          && 0 == ::fseek(fp, 0, SEEK_SET)) {
        data.resize(static_cast<size_t>(len));
        if (len == 0 || ::fread(&data[0], 1, static_cast<size_t>(len), fp) == static_cast<size_t>(len)) {
          ok = true;
        }
      }
    }
    ::fclose(fp);
  }
  return ok;
}

static bool file_is_regular(const char *path)
{
  struct stat st;
  return 0 == ::stat(path, &st) && S_ISREG(st.st_mode);
}

// ---------------------------------------------------------------- CSV reader

struct CsvRecord
{
  std::vector<FileSqlCell> cells;
};

static bool parse_csv_records(const std::string &data,
                              const FileSqlOptions &options,
                              std::vector<CsvRecord> &records)
{
  records.clear();
  const size_t n = data.size();
  const char delimiter = (options.has_delimiter && !options.delimiter.empty())
                             ? options.delimiter[0] : ',';
  const char quote = (options.has_quote && !options.quote.empty())
                         ? options.quote[0] : '"';
  const char escape = (options.has_escape && !options.escape.empty())
                          ? options.escape[0] : '\\';
  const bool has_quote = !(options.has_quote && options.quote.empty());
  const bool has_escape = !(options.has_escape && options.escape.empty());
  size_t i = 0;
  while (i < n) {
    CsvRecord record;
    bool line_done = false;
    while (!line_done) {
      FileSqlCell cell;
      if (i < n && has_quote && data[i] == quote) {
        cell.is_null = false;
        cell.quoted = true;
        ++i;
        bool closed = false;
        while (i < n) {
          if (data[i] == quote) {
            if (i + 1 < n && data[i + 1] == quote) {
              cell.text.push_back(quote);
              i += 2;
            } else {
              ++i;
              closed = true;
              break;
            }
          } else if (has_escape && data[i] == escape && i + 1 < n) {
            const char e = data[i + 1];
            i += 2;
            switch (e) {
              case 'n': cell.text.push_back('\n'); break;
              case 'r': cell.text.push_back('\r'); break;
              case 't': cell.text.push_back('\t'); break;
              case '0': cell.text.push_back('\0'); break;
              default: cell.text.push_back(e); break;
            }
          } else {
            cell.text.push_back(data[i]);
            ++i;
          }
        }
        if (!closed) {
          // Unterminated quoted field: report the row instead of quietly
          // treating the rest of the file as one value.
          return false;
        }
        while (i < n && (data[i] == ' ' || data[i] == '\t')) {
          ++i;
        }
        if (i < n && data[i] != delimiter && data[i] != '\n' && data[i] != '\r') {
          // Junk after the closing quote, e.g. "abc"def.
          return false;
        }
      } else {
        const size_t start = i;
        while (i < n && data[i] != delimiter && data[i] != '\n' && data[i] != '\r') {
          ++i;
        }
        cell.text.assign(data, start, i - start);
        cell.is_null = cell.text.empty();
        if (!cell.is_null && options.has_nullstr && cell.text == options.nullstr) {
          cell.is_null = true;
          cell.text.clear();
        } else if (!cell.is_null && has_escape && escape == '\\' && cell.text == "\\N") {
          // MySQL LOAD DATA convention for a NULL written by SELECT INTO OUTFILE.
          cell.is_null = true;
          cell.text.clear();
        }
      }
      if (!cell.is_null) {
        cell.type = infer_cell_type(cell);
      }
      record.cells.push_back(cell);
      if (i < n && data[i] == delimiter) {
        ++i;
      } else {
        if (i < n && data[i] == '\r') {
          ++i;
          if (i < n && data[i] == '\n') {
            ++i;
          }
        } else if (i < n && data[i] == '\n') {
          ++i;
        }
        line_done = true;
      }
    }
    records.push_back(record);
  }
  return true;
}

static bool looks_like_identifier(const std::string &t)
{
  // Header names are frequently UTF-8 (for example Chinese column names), so
  // any non-ASCII byte is accepted as part of a word here even though the SQL
  // tokenizer only handles ASCII identifiers.
  const bool start_ok = !t.empty()
                        && (is_ident_start(t[0]) || static_cast<unsigned char>(t[0]) >= 0x80);
  if (!start_ok) {
    return false;
  }
  for (size_t i = 1; i < t.size(); ++i) {
    const unsigned char c = static_cast<unsigned char>(t[i]);
    if (!is_ident_char(t[i]) && c < 0x80) {
      return false;
    }
  }
  return true;
}

static bool detect_csv_header(const std::vector<CsvRecord> &records)
{
  bool header = false;
  if (!records.empty()) {
    header = true;
    for (size_t i = 0; i < records[0].cells.size(); ++i) {
      const FileSqlCell &cell = records[0].cells[i];
      if (cell.is_null || !looks_like_identifier(cell.text)) {
        header = false;
        break;
      }
    }
  }
  return header;
}

static bool load_csv_table(const std::string &data, const FileSqlOptions &options, FileSqlTable &table)
{
  std::vector<CsvRecord> records;
  if (!parse_csv_records(data, options, records)) {
    return false;
  }
  // A completely empty line is not a row; ignore it as CSV readers do instead
  // of reporting a one-column record against a wider header.
  std::vector<CsvRecord> non_blank;
  non_blank.reserve(records.size());
  for (size_t r = 0; r < records.size(); ++r) {
    const bool blank = 1 == records[r].cells.size() && !records[r].cells[0].quoted
                       && records[r].cells[0].is_null && records[r].cells[0].text.empty();
    if (!blank) {
      non_blank.push_back(records[r]);
    }
  }
  records.swap(non_blank);
  bool has_header = false;
  if (1 == options.header) {
    has_header = true;
  } else if (2 == options.header) {
    has_header = false;
  } else {
    has_header = detect_csv_header(records);
  }
  size_t column_count = 0;
  if (!records.empty()) {
    column_count = records[0].cells.size();
    for (size_t r = 0; r < records.size(); ++r) {
      if (records[r].cells.size() != column_count) {
        // Ragged rows are a file defect rather than data: surface them
        // instead of padding a wrong shape into the result.
        return false;
      }
    }
  }
  if (column_count == 0) {
    return true;
  }
  std::vector<std::string> base_names;
  for (size_t c = 0; c < column_count; ++c) {
    if (has_header && c < records[0].cells.size() && !records[0].cells[c].text.empty()) {
      base_names.push_back(records[0].cells[c].text);
    } else {
      char buf[32];
      ::snprintf(buf, sizeof(buf), "column%lu", static_cast<unsigned long>(c + 1));
      base_names.push_back(buf);
    }
  }
  for (size_t c = 0; c < column_count; ++c) {
    FileSqlColumn column;
    column.name = base_names[c];
    int duplicates = 0;
    for (size_t prev = 0; prev < c; ++prev) {
      if (base_names[prev] == column.name) {
        ++duplicates;
      }
    }
    if (duplicates > 0) {
      char suffix[32];
      ::snprintf(suffix, sizeof(suffix), "__%d", duplicates + 1);
      column.name += suffix;
    }
    table.columns.push_back(column);
  }
  const size_t first_row = has_header ? 1 : 0;
  for (size_t r = first_row; r < records.size(); ++r) {
    std::vector<FileSqlCell> row;
    row.resize(column_count);
    for (size_t c = 0; c < column_count; ++c) {
      if (c < records[r].cells.size()) {
        row[c] = records[r].cells[c];
        row[c].type = infer_cell_type(row[c]);
        if (row[c].is_null) {
          table.columns[c].nullable = true;
        }
        table.columns[c].type = merge_type(table.columns[c].type, row[c].type);
      } else {
        row[c].is_null = true;
        table.columns[c].nullable = true;
      }
    }
    table.rows.push_back(row);
  }
  return true;
}

// --------------------------------------------------------------- JSON reader

struct JsonCursor
{
  explicit JsonCursor(const std::string &text) : s(text), i(0), ok(true) {}
  const std::string &s;
  size_t i;
  bool ok;

  void skip_ws()
  {
    while (i < s.size() && (s[i] == ' ' || s[i] == '\t' || s[i] == '\r' || s[i] == '\n')) {
      ++i;
    }
  }

  bool consume(const char c)
  {
    skip_ws();
    if (i < s.size() && s[i] == c) {
      ++i;
      return true;
    }
    ok = false;
    return false;
  }

  bool parse_string(std::string &out)
  {
    out.clear();
    skip_ws();
    if (i >= s.size() || s[i] != '"') {
      ok = false;
      return false;
    }
    ++i;
    while (i < s.size()) {
      const char c = s[i];
      if (c == '"') {
        ++i;
        return true;
      }
      if (c == '\\') {
        if (i + 1 >= s.size()) {
          ok = false;
          return false;
        }
        const char e = s[i + 1];
        i += 2;
        switch (e) {
          case '"': out.push_back('"'); break;
          case '\\': out.push_back('\\'); break;
          case '/': out.push_back('/'); break;
          case 'b': out.push_back('\b'); break;
          case 'f': out.push_back('\f'); break;
          case 'n': out.push_back('\n'); break;
          case 'r': out.push_back('\r'); break;
          case 't': out.push_back('\t'); break;
          case 'u': {
            if (i + 4 > s.size()) {
              ok = false;
              return false;
            }
            unsigned int cp = 0;
            for (int k = 0; k < 4; ++k) {
              const char h = s[i + k];
              cp <<= 4;
              if (h >= '0' && h <= '9') {
                cp |= static_cast<unsigned int>(h - '0');
              } else if (h >= 'a' && h <= 'f') {
                cp |= static_cast<unsigned int>(h - 'a' + 10);
              } else if (h >= 'A' && h <= 'F') {
                cp |= static_cast<unsigned int>(h - 'A' + 10);
              } else {
                ok = false;
                return false;
              }
            }
            i += 4;
            if (cp >= 0xD800 && cp <= 0xDBFF && i + 6 <= s.size() &&
                s[i] == '\\' && s[i + 1] == 'u') {
              unsigned int low = 0;
              for (int k = 0; k < 4; ++k) {
                const char h = s[i + 2 + k];
                low <<= 4;
                if (h >= '0' && h <= '9') {
                  low |= static_cast<unsigned int>(h - '0');
                } else if (h >= 'a' && h <= 'f') {
                  low |= static_cast<unsigned int>(h - 'a' + 10);
                } else if (h >= 'A' && h <= 'F') {
                  low |= static_cast<unsigned int>(h - 'A' + 10);
                } else {
                  ok = false;
                  return false;
                }
              }
              if (low >= 0xDC00 && low <= 0xDFFF) {
                cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
                i += 6;
              }
            }
            if (cp < 0x80) {
              out.push_back(static_cast<char>(cp));
            } else if (cp < 0x800) {
              out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
              out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            } else if (cp < 0x10000) {
              out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
              out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
              out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            } else {
              out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
              out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
              out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
              out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            }
            break;
          }
          default: out.push_back(e); break;
        }
        continue;
      }
      if (static_cast<unsigned char>(c) < 0x20) {
        ok = false;
        return false;
      }
      out.push_back(c);
      ++i;
    }
    ok = false;
    return false;
  }

  bool parse_number(FileSqlCell &cell)
  {
    skip_ws();
    const size_t start = i;
    if (i < s.size() && s[i] == '-') {
      ++i;
    }
    bool digits = false;
    while (i < s.size() && s[i] >= '0' && s[i] <= '9') {
      digits = true;
      ++i;
    }
    bool is_double = false;
    if (i < s.size() && s[i] == '.') {
      is_double = true;
      ++i;
      while (i < s.size() && s[i] >= '0' && s[i] <= '9') {
        digits = true;
        ++i;
      }
    }
    if (i < s.size() && (s[i] == 'e' || s[i] == 'E')) {
      is_double = true;
      ++i;
      if (i < s.size() && (s[i] == '+' || s[i] == '-')) {
        ++i;
      }
      while (i < s.size() && s[i] >= '0' && s[i] <= '9') {
        digits = true;
        ++i;
      }
    }
    if (!digits) {
      ok = false;
      return false;
    }
    cell.is_null = false;
    cell.text.assign(s, start, i - start);
    cell.type = is_double ? FILE_SQL_DOUBLE : FILE_SQL_BIGINT;
    return true;
  }

  bool parse_value(FileSqlCell &cell)
  {
    skip_ws();
    if (i >= s.size()) {
      ok = false;
      return false;
    }
    if (s[i] == '"') {
      cell.is_null = false;
      cell.type = FILE_SQL_VARCHAR;
      return parse_string(cell.text);
    }
    if (s[i] == '-' || (s[i] >= '0' && s[i] <= '9')) {
      return parse_number(cell);
    }
    if (s.compare(i, 4, "true") == 0) {
      i += 4;
      cell.is_null = false;
      cell.type = FILE_SQL_BOOLEAN;
      cell.text = "1";
      return true;
    }
    if (s.compare(i, 5, "false") == 0) {
      i += 5;
      cell.is_null = false;
      cell.type = FILE_SQL_BOOLEAN;
      cell.text = "0";
      return true;
    }
    if (s.compare(i, 4, "null") == 0) {
      i += 4;
      cell.is_null = true;
      cell.type = FILE_SQL_NULL;
      return true;
    }
    ok = false;
    return false;
  }
};

static bool parse_json_line(const std::string &line,
                            std::vector<std::pair<std::string, FileSqlCell> > &fields)
{
  fields.clear();
  JsonCursor cur(line);
  if (!cur.consume('{')) {
    return false;
  }
  cur.skip_ws();
  if (cur.i < line.size() && line[cur.i] == '}') {
    ++cur.i;
  } else {
    for (;;) {
      std::string key;
      FileSqlCell cell;
      if (!cur.parse_string(key)) {
        return false;
      }
      if (!cur.consume(':')) {
        return false;
      }
      if (!cur.parse_value(cell)) {
        return false;
      }
      fields.push_back(std::make_pair(key, cell));
      cur.skip_ws();
      if (cur.i < line.size() && line[cur.i] == ',') {
        ++cur.i;
        continue;
      }
      if (cur.i < line.size() && line[cur.i] == '}') {
        ++cur.i;
        break;
      }
      return false;
    }
  }
  cur.skip_ws();
  return cur.i == line.size();
}

static bool load_json_table(const std::string &data, FileSqlTable &table)
{
  std::vector<std::vector<std::pair<std::string, FileSqlCell> > > objects;
  size_t begin = 0;
  while (begin <= data.size()) {
    size_t end = data.find('\n', begin);
    if (std::string::npos == end) {
      end = data.size();
    }
    const std::string line = trim(data.substr(begin, end - begin));
    if (!line.empty()) {
      std::vector<std::pair<std::string, FileSqlCell> > fields;
      if (!parse_json_line(line, fields)) {
        return false;
      }
      objects.push_back(fields);
    }
    if (end == data.size()) {
      break;
    }
    begin = end + 1;
  }
  std::vector<std::string> names;
  for (size_t r = 0; r < objects.size(); ++r) {
    for (size_t f = 0; f < objects[r].size(); ++f) {
      bool found = false;
      for (size_t k = 0; k < names.size(); ++k) {
        if (names[k] == objects[r][f].first) {
          found = true;
          break;
        }
      }
      if (!found) {
        names.push_back(objects[r][f].first);
      }
    }
  }
  for (size_t c = 0; c < names.size(); ++c) {
    FileSqlColumn column;
    column.name = names[c];
    table.columns.push_back(column);
  }
  for (size_t r = 0; r < objects.size(); ++r) {
    std::vector<FileSqlCell> row;
    row.resize(names.size());
    for (size_t c = 0; c < names.size(); ++c) {
      for (size_t f = 0; f < objects[r].size(); ++f) {
        if (objects[r][f].first == names[c]) {
          row[c] = objects[r][f].second;
          break;
        }
      }
      if (row[c].is_null) {
        table.columns[c].nullable = true;
      }
      table.columns[c].type = merge_type(table.columns[c].type, row[c].type);
    }
    table.rows.push_back(row);
  }
  return true;
}

// ------------------------------------------------------------ Parquet reader

struct CompactReader
{
  CompactReader(const unsigned char *data, size_t size) : p(data), n(size), i(0), ok(true) {}
  const unsigned char *p;
  size_t n;
  size_t i;
  bool ok;

  bool byte(uint8_t &b)
  {
    if (i >= n) {
      ok = false;
      return false;
    }
    b = p[i++];
    return true;
  }

  bool skip_bytes(const size_t len)
  {
    if (i + len > n) {
      ok = false;
      return false;
    }
    i += len;
    return true;
  }

  bool uvarint(uint64_t &v)
  {
    v = 0;
    int shift = 0;
    while (i < n && shift <= 63) {
      const uint8_t b = p[i++];
      v |= static_cast<uint64_t>(b & 0x7f) << shift;
      if (!(b & 0x80)) {
        return true;
      }
      shift += 7;
    }
    ok = false;
    return false;
  }

  bool zigzag(int64_t &v)
  {
    uint64_t u = 0;
    if (!uvarint(u)) {
      return false;
    }
    v = static_cast<int64_t>(u >> 1) ^ -static_cast<int64_t>(u & 1);
    return true;
  }

  bool binary(std::string &s)
  {
    uint64_t len = 0;
    if (!uvarint(len) || i + len > n) {
      ok = false;
      return false;
    }
    s.assign(reinterpret_cast<const char *>(p + i), static_cast<size_t>(len));
    i += static_cast<size_t>(len);
    return true;
  }

  bool field_header(int16_t &id, uint8_t &type, int16_t &last_id)
  {
    uint8_t b = 0;
    if (!byte(b)) {
      return false;
    }
    type = b & 0x0f;
    if (type == 0) {
      id = 0;
      return true;
    }
    const int16_t delta = static_cast<int16_t>((b & 0xf0) >> 4);
    if (delta == 0) {
      int64_t v = 0;
      if (!zigzag(v)) {
        return false;
      }
      id = static_cast<int16_t>(v);
    } else {
      id = static_cast<int16_t>(last_id + delta);
    }
    last_id = id;
    return true;
  }

  bool list_header(uint8_t &elem_type, int32_t &size)
  {
    uint8_t b = 0;
    if (!byte(b)) {
      return false;
    }
    elem_type = b & 0x0f;
    size = static_cast<int32_t>((b >> 4) & 0x0f);
    if (size == 15) {
      uint64_t v = 0;
      if (!uvarint(v)) {
        return false;
      }
      size = static_cast<int32_t>(v);
    }
    return true;
  }

  bool skip(const uint8_t type)
  {
    bool done = true;
    switch (type) {
      case 1:
      case 2:
        break;
      case 3: {
        uint8_t b = 0;
        done = byte(b);
        break;
      }
      case 4:
      case 5:
      case 6: {
        uint64_t v = 0;
        done = uvarint(v);
        break;
      }
      case 7:
        done = skip_bytes(8);
        break;
      case 8: {
        uint64_t len = 0;
        done = uvarint(len) && skip_bytes(static_cast<size_t>(len));
        break;
      }
      case 9:
      case 10: {
        uint8_t et = 0;
        int32_t sz = 0;
        done = list_header(et, sz);
        for (int32_t k = 0; done && k < sz; ++k) {
          done = skip(et);
        }
        break;
      }
      case 11: {
        uint64_t sz = 0;
        done = uvarint(sz);
        if (done && sz > 0) {
          uint8_t kv = 0;
          done = byte(kv);
          const uint8_t kt = static_cast<uint8_t>(kv >> 4);
          const uint8_t vt = static_cast<uint8_t>(kv & 0x0f);
          for (uint64_t k = 0; done && k < sz; ++k) {
            done = skip(kt) && skip(vt);
          }
        }
        break;
      }
      case 12: {
        int16_t last = 0;
        int16_t id = 0;
        uint8_t ft = 0;
        for (;;) {
          done = field_header(id, ft, last);
          if (!done || ft == 0) {
            break;
          }
          done = skip(ft);
          if (!done) {
            break;
          }
        }
        break;
      }
      default:
        done = false;
        break;
    }
    if (!done) {
      ok = false;
    }
    return done;
  }
};

struct ParquetSchemaElement
{
  ParquetSchemaElement() : type(-1), repetition(0), num_children(0) {}
  int type;
  int repetition;
  std::string name;
  int num_children;
};

struct ParquetColumnChunk
{
  ParquetColumnChunk()
      : type(0), codec(0), num_values(0), data_page_offset(0), dictionary_page_offset(-1) {}
  int type;
  int codec;
  int64_t num_values;
  int64_t data_page_offset;
  int64_t dictionary_page_offset;
  std::string path;
};

static uint32_t read_le32(const unsigned char *p)
{
  return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
         (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

static uint64_t read_le64(const unsigned char *p)
{
  return static_cast<uint64_t>(read_le32(p)) |
         (static_cast<uint64_t>(read_le32(p + 4)) << 32);
}

static bool parse_parquet_schema_element(CompactReader &r, ParquetSchemaElement &el)
{
  int16_t last = 0;
  int16_t id = 0;
  uint8_t ft = 0;
  for (;;) {
    if (!r.field_header(id, ft, last)) {
      return false;
    }
    if (ft == 0) {
      break;
    }
    if (1 == id) {
      int64_t v = 0;
      if (!r.zigzag(v)) {
        return false;
      }
      el.type = static_cast<int>(v);
    } else if (3 == id) {
      int64_t v = 0;
      if (!r.zigzag(v)) {
        return false;
      }
      el.repetition = static_cast<int>(v);
    } else if (4 == id) {
      if (!r.binary(el.name)) {
        return false;
      }
    } else if (5 == id) {
      int64_t v = 0;
      if (!r.zigzag(v)) {
        return false;
      }
      el.num_children = static_cast<int>(v);
    } else if (!r.skip(ft)) {
      return false;
    }
  }
  return true;
}

static bool parse_parquet_column_meta(CompactReader &r, ParquetColumnChunk &chunk)
{
  int16_t last = 0;
  int16_t id = 0;
  uint8_t ft = 0;
  for (;;) {
    if (!r.field_header(id, ft, last)) {
      return false;
    }
    if (ft == 0) {
      break;
    }
    if (1 == id) {
      int64_t v = 0;
      if (!r.zigzag(v)) {
        return false;
      }
      chunk.type = static_cast<int>(v);
    } else if (3 == id) {
      uint8_t et = 0;
      int32_t sz = 0;
      if (!r.list_header(et, sz)) {
        return false;
      }
      chunk.path.clear();
      for (int32_t k = 0; k < sz; ++k) {
        std::string part;
        if (!r.binary(part)) {
          return false;
        }
        if (!chunk.path.empty()) {
          chunk.path.push_back('.');
        }
        chunk.path += part;
      }
    } else if (4 == id) {
      int64_t v = 0;
      if (!r.zigzag(v)) {
        return false;
      }
      chunk.codec = static_cast<int>(v);
    } else if (5 == id) {
      int64_t v = 0;
      if (!r.zigzag(v)) {
        return false;
      }
      chunk.num_values = v;
    } else if (9 == id) {
      int64_t v = 0;
      if (!r.zigzag(v)) {
        return false;
      }
      chunk.data_page_offset = v;
    } else if (11 == id) {
      int64_t v = 0;
      if (!r.zigzag(v)) {
        return false;
      }
      chunk.dictionary_page_offset = v;
    } else if (!r.skip(ft)) {
      return false;
    }
  }
  return true;
}

static bool parse_parquet_column_chunk(CompactReader &r, ParquetColumnChunk &chunk)
{
  int16_t last = 0;
  int16_t id = 0;
  uint8_t ft = 0;
  for (;;) {
    if (!r.field_header(id, ft, last)) {
      return false;
    }
    if (ft == 0) {
      break;
    }
    if (3 == id) {
      if (!parse_parquet_column_meta(r, chunk)) {
        return false;
      }
    } else if (!r.skip(ft)) {
      return false;
    }
  }
  return true;
}

static bool parse_parquet_page_header(CompactReader &r,
                                      int &page_type,
                                      int &compressed_size,
                                      int &num_values,
                                      int &encoding,
                                      int &def_len,
                                      int &rep_len,
                                      bool &is_compressed,
                                      int &dict_num_values)
{
  page_type = -1;
  compressed_size = 0;
  num_values = 0;
  encoding = 0;
  def_len = 0;
  rep_len = 0;
  is_compressed = true;
  dict_num_values = 0;
  int16_t last = 0;
  int16_t id = 0;
  uint8_t ft = 0;
  for (;;) {
    if (!r.field_header(id, ft, last)) {
      return false;
    }
    if (ft == 0) {
      break;
    }
    if (1 == id || 3 == id) {
      int64_t v = 0;
      if (!r.zigzag(v)) {
        return false;
      }
      if (1 == id) {
        page_type = static_cast<int>(v);
      } else {
        compressed_size = static_cast<int>(v);
      }
    } else if (5 == id) {
      // DataPageHeader (v1): NumValues, Encoding, DefinitionLevelEncoding,
      // RepetitionLevelEncoding, Statistics. The level encodings are always
      // RLE/bit-packed for the files this reader accepts.
      int16_t dlast = 0;
      int16_t did = 0;
      uint8_t dft = 0;
      for (;;) {
        if (!r.field_header(did, dft, dlast)) {
          return false;
        }
        if (dft == 0) {
          break;
        }
        if (1 == did || 2 == did) {
          int64_t v = 0;
          if (!r.zigzag(v)) {
            return false;
          }
          if (1 == did) {
            num_values = static_cast<int>(v);
          } else {
            encoding = static_cast<int>(v);
          }
        } else if (!r.skip(dft)) {
          return false;
        }
      }
    } else if (7 == id) {
      // DictionaryPageHeader: NumValues, Encoding, IsSorted.
      int16_t dlast = 0;
      int16_t did = 0;
      uint8_t dft = 0;
      for (;;) {
        if (!r.field_header(did, dft, dlast)) {
          return false;
        }
        if (dft == 0) {
          break;
        }
        if (1 == did || 2 == did) {
          int64_t v = 0;
          if (!r.zigzag(v)) {
            return false;
          }
          if (1 == did) {
            dict_num_values = static_cast<int>(v);
          }
        } else if (!r.skip(dft)) {
          return false;
        }
      }
    } else if (8 == id) {
      int16_t dlast = 0;
      int16_t did = 0;
      uint8_t dft = 0;
      for (;;) {
        if (!r.field_header(did, dft, dlast)) {
          return false;
        }
        if (dft == 0) {
          break;
        }
        if (1 == did || 2 == did || 3 == did || 4 == did || 5 == did || 6 == did) {
          int64_t v = 0;
          if (!r.zigzag(v)) {
            return false;
          }
          if (1 == did) {
            num_values = static_cast<int>(v);
          } else if (4 == did) {
            encoding = static_cast<int>(v);
          } else if (5 == did) {
            def_len = static_cast<int>(v);
          } else if (6 == did) {
            rep_len = static_cast<int>(v);
          }
        } else if (7 == did) {
          if (dft == 1) {
            is_compressed = true;
          } else if (dft == 2) {
            is_compressed = false;
          } else if (!r.skip(dft)) {
            return false;
          }
        } else if (!r.skip(dft)) {
          return false;
        }
      }
    } else if (!r.skip(ft)) {
      return false;
    }
  }
  return true;
}

static bool decode_levels(const unsigned char *p,
                          const size_t len,
                          const int count,
                          const int bit_width,
                          std::vector<int> &out)
{
  out.clear();
  if (bit_width <= 0) {
    out.assign(count, 0);
    return true;
  }
  if (bit_width > 32) {
    return false;
  }
  size_t i = 0;
  while (i < len && static_cast<int>(out.size()) < count) {
    uint64_t header = 0;
    int shift = 0;
    while (i < len) {
      const uint8_t b = p[i++];
      header |= static_cast<uint64_t>(b & 0x7f) << shift;
      if (!(b & 0x80)) {
        break;
      }
      shift += 7;
      if (shift > 63) {
        return false;
      }
    }
    if (header & 1) {
      const uint64_t groups = header >> 1;
      const uint64_t total = groups * 8;
      const size_t nbytes = static_cast<size_t>((total * bit_width + 7) / 8);
      if (i + nbytes > len) {
        return false;
      }
      for (uint64_t k = 0; k < total && static_cast<int>(out.size()) < count; ++k) {
        const uint64_t bit = k * static_cast<uint64_t>(bit_width);
        uint32_t v = 0;
        for (int b = 0; b < bit_width; ++b) {
          const uint64_t idx = bit + static_cast<uint64_t>(b);
          v |= static_cast<uint32_t>((p[i + idx / 8] >> (idx % 8)) & 1u) << b;
        }
        out.push_back(static_cast<int>(v));
      }
      i += nbytes;
    } else {
      const uint64_t run = header >> 1;
      const size_t nbytes = static_cast<size_t>((bit_width + 7) / 8);
      if (i + nbytes > len) {
        return false;
      }
      uint32_t v = 0;
      for (size_t b = 0; b < nbytes; ++b) {
        v |= static_cast<uint32_t>(p[i + b]) << (8 * b);
      }
      i += nbytes;
      for (uint64_t k = 0; k < run && static_cast<int>(out.size()) < count; ++k) {
        out.push_back(static_cast<int>(v));
      }
    }
  }
  while (static_cast<int>(out.size()) < count) {
    out.push_back(0);
  }
  return true;
}

static bool snappy_decompress(const unsigned char *in, const size_t in_len, std::string &out)
{
  out.clear();
  size_t i = 0;
  uint64_t expected = 0;
  int shift = 0;
  while (i < in_len) {
    const uint8_t b = in[i++];
    expected |= static_cast<uint64_t>(b & 0x7f) << shift;
    if (!(b & 0x80)) {
      break;
    }
    shift += 7;
    if (shift > 35) {
      return false;
    }
  }
  if (expected > FILE_SQL_MAX_MATERIALIZED_BYTES) {
    // A corrupted header could claim an enormous expansion; refuse it here
    // instead of letting reserve() exhaust memory.
    return false;
  }
  out.reserve(static_cast<size_t>(expected));
  while (i < in_len) {
    const uint8_t tag = in[i++];
    uint32_t len = 0;
    uint32_t offset = 0;
    switch (tag & 3) {
      case 0: {
        len = tag >> 2;
        if (len < 60) {
          len += 1;
        } else {
          const int extra = static_cast<int>(len) - 59;
          if (i + extra > in_len) {
            return false;
          }
          len = 0;
          for (int k = 0; k < extra; ++k) {
            len |= static_cast<uint32_t>(in[i + k]) << (8 * k);
          }
          i += extra;
          len += 1;
        }
        if (i + len > in_len) {
          return false;
        }
        out.append(reinterpret_cast<const char *>(in + i), len);
        i += len;
        continue;
      }
      case 1: {
        if (i + 1 > in_len) {
          return false;
        }
        len = ((tag >> 2) & 0x7) + 4;
        offset = (static_cast<uint32_t>(tag & 0xe0) << 3) | in[i];
        i += 1;
        break;
      }
      case 2: {
        if (i + 2 > in_len) {
          return false;
        }
        len = (tag >> 2) + 1;
        offset = static_cast<uint32_t>(in[i]) | (static_cast<uint32_t>(in[i + 1]) << 8);
        i += 2;
        break;
      }
      default: {
        if (i + 4 > in_len) {
          return false;
        }
        len = (tag >> 2) + 1;
        offset = read_le32(in + i);
        i += 4;
        break;
      }
    }
    if (offset == 0 || offset > out.size()) {
      return false;
    }
    for (uint32_t k = 0; k < len; ++k) {
      const char c = out[out.size() - offset];
      out.push_back(c);
    }
  }
  return out.size() == expected;
}

static FileSqlType parquet_type(const int physical)
{
  switch (physical) {
    case 0: return FILE_SQL_BOOLEAN;
    case 1:
    case 2: return FILE_SQL_BIGINT;
    case 4:
    case 5: return FILE_SQL_DOUBLE;
    case 6:
    case 7: return FILE_SQL_VARCHAR;
    default: return FILE_SQL_VARCHAR;
  }
}

static bool decode_plain_cell(const int physical,
                              const std::string &values,
                              size_t &vi,
                              const size_t value_index,
                              FileSqlCell &cell)
{
  char buf[64];
  switch (physical) {
    case 0: {
      const size_t byte_pos = value_index / 8;
      if (byte_pos >= values.size()) {
        return false;
      }
      const bool bit = 0 != ((static_cast<unsigned char>(values[byte_pos]) >> (value_index % 8)) & 1u);
      cell.is_null = false;
      cell.text = bit ? "1" : "0";
      break;
    }
    case 1: {
      if (vi + 4 > values.size()) {
        return false;
      }
      const int32_t v = static_cast<int32_t>(read_le32(
          reinterpret_cast<const unsigned char *>(values.data()) + vi));
      vi += 4;
      ::snprintf(buf, sizeof(buf), "%d", v);
      cell.is_null = false;
      cell.text = buf;
      break;
    }
    case 2: {
      if (vi + 8 > values.size()) {
        return false;
      }
      const int64_t v = static_cast<int64_t>(read_le64(
          reinterpret_cast<const unsigned char *>(values.data()) + vi));
      vi += 8;
      ::snprintf(buf, sizeof(buf), "%lld", static_cast<long long>(v));
      cell.is_null = false;
      cell.text = buf;
      break;
    }
    case 4: {
      if (vi + 4 > values.size()) {
        return false;
      }
      const uint32_t bits = read_le32(reinterpret_cast<const unsigned char *>(values.data()) + vi);
      vi += 4;
      float f = 0.0f;
      ::memcpy(&f, &bits, sizeof(f));
      ::snprintf(buf, sizeof(buf), "%.9g", static_cast<double>(f));
      cell.is_null = false;
      cell.text = buf;
      break;
    }
    case 5: {
      if (vi + 8 > values.size()) {
        return false;
      }
      const uint64_t bits = read_le64(reinterpret_cast<const unsigned char *>(values.data()) + vi);
      vi += 8;
      double d = 0.0;
      ::memcpy(&d, &bits, sizeof(d));
      ::snprintf(buf, sizeof(buf), "%.17g", d);
      cell.is_null = false;
      cell.text = buf;
      break;
    }
    case 6: {
      if (vi + 4 > values.size()) {
        return false;
      }
      const uint32_t len = read_le32(reinterpret_cast<const unsigned char *>(values.data()) + vi);
      vi += 4;
      if (vi + len > values.size()) {
        return false;
      }
      cell.is_null = false;
      cell.text.assign(values, vi, len);
      vi += len;
      break;
    }
    default:
      return false;
  }
  return true;
}

// Expand the body of one data page into cells. Dictionary indices use the
// RLE/bit-packed hybrid layout, so a page of indices is decoded in one pass and
// then looked up in the dictionary loaded from the preceding dictionary page.
static bool decode_parquet_page_values(const int physical,
                                       const int encoding,
                                       const std::string &values,
                                       const std::vector<int> &defs,
                                       const int max_def,
                                       const std::vector<FileSqlCell> &dict,
                                       std::vector<FileSqlCell> &cells)
{
  bool ok = true;
  const int count = static_cast<int>(defs.size());
  const FileSqlType type = parquet_type(physical);
  cells.clear();
  cells.reserve(static_cast<size_t>(count));
  if (0 == encoding) {
    size_t vi = 0;
    size_t value_index = 0;
    for (int k = 0; ok && k < count; ++k) {
      FileSqlCell cell;
      if (max_def > 0 && defs[k] != max_def) {
        cell.is_null = true;
      } else if (decode_plain_cell(physical, values, vi, value_index, cell)) {
        cell.type = type;
        ++value_index;
      } else {
        ok = false;
      }
      if (ok) {
        cells.push_back(cell);
      }
    }
  } else if (2 == encoding || 8 == encoding) {
    int non_null = 0;
    for (int k = 0; k < count; ++k) {
      if (max_def <= 0 || defs[k] == max_def) {
        ++non_null;
      }
    }
    std::vector<int> indices;
    if (non_null > 0) {
      if (values.empty()) {
        ok = false;
      } else {
        const int index_width = static_cast<int>(static_cast<unsigned char>(values[0]));
        ok = decode_levels(reinterpret_cast<const unsigned char *>(values.data()) + 1,
                           values.size() - 1, non_null, index_width, indices);
      }
    }
    size_t next_index = 0;
    for (int k = 0; ok && k < count; ++k) {
      FileSqlCell cell;
      if (max_def > 0 && defs[k] != max_def) {
        cell.is_null = true;
      } else if (next_index >= indices.size() || indices[next_index] < 0
                 || static_cast<size_t>(indices[next_index]) >= dict.size()) {
        ok = false;
      } else {
        cell = dict[static_cast<size_t>(indices[next_index])];
        cell.type = type;
        ++next_index;
      }
      if (ok) {
        cells.push_back(cell);
      }
    }
  } else {
    // Only PLAIN and dictionary-encoded pages are supported; reject the page
    // instead of returning values decoded with the wrong layout.
    ok = false;
  }
  return ok;
}

static bool decode_parquet_dictionary(const unsigned char *data,
                                      const size_t len,
                                      const bool compressed,
                                      const int physical,
                                      const int count,
                                      std::vector<FileSqlCell> &dict)
{
  bool ok = true;
  dict.clear();
  std::string plain;
  if (compressed && len > 0) {
    ok = snappy_decompress(data, len, plain);
  } else if (len > 0) {
    plain.assign(reinterpret_cast<const char *>(data), len);
  }
  size_t vi = 0;
  for (int k = 0; ok && k < count; ++k) {
    FileSqlCell cell;
    if (!decode_plain_cell(physical, plain, vi, static_cast<size_t>(k), cell)) {
      ok = false;
    } else {
      cell.type = parquet_type(physical);
      dict.push_back(cell);
    }
  }
  return ok;
}

static bool decode_parquet_column(const std::string &file,
                                  const ParquetColumnChunk &chunk,
                                  const int max_def,
                                  std::vector<FileSqlCell> &cells)
{
  cells.clear();
  if (chunk.num_values <= 0) {
    return true;
  }
  if (chunk.codec != 0 && chunk.codec != 1) {
    // 0: uncompressed, 1: snappy. Other codecs need a decompressor that is not
    // part of this reader, so the column cannot be decoded correctly.
    return false;
  }
  size_t pos = static_cast<size_t>(chunk.data_page_offset);
  if (chunk.dictionary_page_offset >= 0 &&
      static_cast<size_t>(chunk.dictionary_page_offset) < pos) {
    pos = static_cast<size_t>(chunk.dictionary_page_offset);
  }
  int64_t produced = 0;
  const unsigned char *base = reinterpret_cast<const unsigned char *>(file.data());
  const size_t file_size = file.size();
  int bit_width = 0;
  for (int m = max_def; m > 0; m >>= 1) {
    ++bit_width;
  }
  std::vector<FileSqlCell> dict;
  while (pos < file_size && produced < chunk.num_values) {
    CompactReader r(base + pos, file_size - pos);
    int page_type = -1;
    int compressed_size = 0;
    int num_values = 0;
    int encoding = 0;
    int def_len = 0;
    int rep_len = 0;
    bool is_compressed = true;
    int dict_num_values = 0;
    if (!parse_parquet_page_header(r, page_type, compressed_size, num_values,
                                   encoding, def_len, rep_len, is_compressed,
                                   dict_num_values)) {
      return false;
    }
    const size_t header_len = r.i;
    if (compressed_size < 0
        || num_values < 0
        || static_cast<int64_t>(num_values) > chunk.num_values
        || pos + header_len + static_cast<size_t>(compressed_size) > file_size) {
      return false;
    }
    const unsigned char *payload = base + pos + header_len;
    if (2 == page_type && dict_num_values > 0) {
      if (!decode_parquet_dictionary(payload, static_cast<size_t>(compressed_size),
                                     chunk.codec != 0, chunk.type, dict_num_values,
                                     dict)) {
        return false;
      }
    } else if (0 == page_type && num_values > 0) {
      // Data page v1: repetition levels (none for flat columns), definition
      // levels prefixed with their byte length, then the values.
      std::vector<int> defs;
      size_t offset = 0;
      if (max_def > 0) {
        if (compressed_size < 4) {
          return false;
        }
        const uint32_t def_bytes = read_le32(payload);
        if (4 + static_cast<size_t>(def_bytes) > static_cast<size_t>(compressed_size)) {
          return false;
        }
        if (!decode_levels(payload + 4, static_cast<size_t>(def_bytes), num_values,
                           bit_width, defs)) {
          return false;
        }
        offset = 4 + static_cast<size_t>(def_bytes);
      } else {
        defs.assign(num_values, 0);
      }
      const size_t values_len = static_cast<size_t>(compressed_size) - offset;
      std::string values;
      if (chunk.codec != 0 && values_len > 0) {
        if (!snappy_decompress(payload + offset, values_len, values)) {
          return false;
        }
      } else {
        values.assign(reinterpret_cast<const char *>(payload + offset), values_len);
      }
      std::vector<FileSqlCell> page_cells;
      if (!decode_parquet_page_values(chunk.type, encoding, values, defs, max_def,
                                      dict, page_cells)) {
        return false;
      }
      cells.insert(cells.end(), page_cells.begin(), page_cells.end());
      produced += num_values;
    } else if (3 == page_type && num_values > 0) {
      if (rep_len < 0 || def_len < 0 || rep_len + def_len > compressed_size) {
        return false;
      }
      std::vector<int> defs;
      if (max_def > 0) {
        if (!decode_levels(payload + rep_len, static_cast<size_t>(def_len), num_values,
                           bit_width, defs)) {
          return false;
        }
      } else {
        defs.assign(num_values, 0);
      }
      const int values_len = compressed_size - rep_len - def_len;
      std::string values;
      if (is_compressed && chunk.codec != 0 && values_len > 0) {
        if (!snappy_decompress(payload + rep_len + def_len,
                               static_cast<size_t>(values_len), values)) {
          return false;
        }
      } else {
        values.assign(reinterpret_cast<const char *>(payload + rep_len + def_len),
                      static_cast<size_t>(values_len));
      }
      std::vector<FileSqlCell> page_cells;
      if (!decode_parquet_page_values(chunk.type, encoding, values, defs, max_def,
                                      dict, page_cells)) {
        return false;
      }
      cells.insert(cells.end(), page_cells.begin(), page_cells.end());
      produced += num_values;
    }
    pos += header_len + static_cast<size_t>(compressed_size);
  }
  return produced >= chunk.num_values;
}

static bool load_parquet_table(const std::string &path, FileSqlTable &table)
{
  std::string file;
  if (!read_whole_file(path, file) || file.size() < 12) {
    return false;
  }
  if (file.compare(0, 4, "PAR1") != 0 ||
      file.compare(file.size() - 4, 4, "PAR1") != 0) {
    return false;
  }
  const unsigned char *base = reinterpret_cast<const unsigned char *>(file.data());
  const uint32_t footer_len = read_le32(base + file.size() - 8);
  if (static_cast<size_t>(footer_len) + 8 > file.size()) {
    return false;
  }
  const size_t footer_start = file.size() - 8 - static_cast<size_t>(footer_len);
  CompactReader r(base + footer_start, footer_len);
  std::vector<ParquetSchemaElement> schema;
  // A file may contain several row groups; each group stores one chunk per
  // leaf column. Keep the groups apart so the rows are concatenated in file
  // order instead of silently reading only the first group.
  std::vector<std::vector<ParquetColumnChunk> > row_groups;
  int16_t last = 0;
  int16_t id = 0;
  uint8_t ft = 0;
  for (;;) {
    if (!r.field_header(id, ft, last)) {
      return false;
    }
    if (ft == 0) {
      break;
    }
    if (2 == id) {
      uint8_t et = 0;
      int32_t sz = 0;
      if (!r.list_header(et, sz)) {
        return false;
      }
      for (int32_t k = 0; k < sz; ++k) {
        ParquetSchemaElement el;
        if (!parse_parquet_schema_element(r, el)) {
          return false;
        }
        schema.push_back(el);
      }
    } else if (4 == id) {
      uint8_t et = 0;
      int32_t sz = 0;
      if (!r.list_header(et, sz)) {
        return false;
      }
      for (int32_t k = 0; k < sz; ++k) {
        std::vector<ParquetColumnChunk> group;
        int16_t rlast = 0;
        int16_t rid = 0;
        uint8_t rft = 0;
        for (;;) {
          if (!r.field_header(rid, rft, rlast)) {
            return false;
          }
          if (rft == 0) {
            break;
          }
          if (1 == rid) {
            uint8_t cet = 0;
            int32_t csz = 0;
            if (!r.list_header(cet, csz)) {
              return false;
            }
            for (int32_t c = 0; c < csz; ++c) {
              ParquetColumnChunk chunk;
              if (!parse_parquet_column_chunk(r, chunk)) {
                return false;
              }
              group.push_back(chunk);
            }
          } else if (!r.skip(rft)) {
            return false;
          }
        }
        row_groups.push_back(group);
      }
    } else if (!r.skip(ft)) {
      return false;
    }
  }
  std::vector<size_t> leaves;
  for (size_t k = 1; k < schema.size(); ++k) {
    if (schema[k].type >= 0) {
      leaves.push_back(k);
    }
  }
  for (size_t c = 0; c < leaves.size(); ++c) {
    if (2 == schema[leaves[c]].repetition) {
      // Repeated primitive columns need repetition-level handling that this
      // flat reader does not provide.
      return false;
    }
    FileSqlColumn column;
    column.name = schema[leaves[c]].name;
    column.type = parquet_type(schema[leaves[c]].type);
    const int max_def = schema[leaves[c]].repetition == 1 ? 1 : 0;
    if (max_def > 0) {
      column.nullable = true;
    }
    table.columns.push_back(column);
  }
  std::vector<std::vector<FileSqlCell> > columns_data;
  columns_data.resize(leaves.size());
  for (size_t c = 0; c < leaves.size(); ++c) {
    const int max_def = schema[leaves[c]].repetition == 1 ? 1 : 0;
    for (size_t g = 0; g < row_groups.size(); ++g) {
      if (row_groups[g].size() != leaves.size()) {
        return false;
      }
      std::vector<FileSqlCell> group_cells;
      if (!decode_parquet_column(file, row_groups[g][c], max_def, group_cells)) {
        return false;
      }
      columns_data[c].insert(columns_data[c].end(), group_cells.begin(), group_cells.end());
    }
  }
  size_t row_count = 0;
  for (size_t c = 0; c < columns_data.size(); ++c) {
    if (columns_data[c].size() > row_count) {
      row_count = columns_data[c].size();
    }
  }
  for (size_t r = 0; r < row_count; ++r) {
    std::vector<FileSqlCell> row;
    row.resize(leaves.size());
    for (size_t c = 0; c < leaves.size(); ++c) {
      if (r < columns_data[c].size()) {
        row[c] = columns_data[c][r];
        if (row[c].is_null) {
          table.columns[c].nullable = true;
        }
      }
    }
    table.rows.push_back(row);
  }
  return true;
}

// ---------------------------------------------------------------- glob reader

static bool load_glob_table(const std::string &pattern, FileSqlTable &table)
{
  FileSqlColumn column;
  column.name = "file";
  column.type = FILE_SQL_VARCHAR;
  table.columns.push_back(column);
  glob_t matches;
  ::memset(&matches, 0, sizeof(matches));
  const int rc = ::glob(pattern.c_str(), 0, NULL, &matches);
  if (GLOB_NOMATCH == rc) {
    ::globfree(&matches);
    return true;
  }
  if (0 != rc) {
    ::globfree(&matches);
    return false;
  }
  for (size_t i = 0; i < matches.gl_pathc; ++i) {
    const std::string path(matches.gl_pathv[i]);
    struct stat st;
    if (0 == ::stat(path.c_str(), &st) && !S_ISREG(st.st_mode)) {
      continue;
    }
    std::vector<FileSqlCell> row;
    FileSqlCell cell;
    cell.is_null = false;
    cell.type = FILE_SQL_VARCHAR;
    cell.text = path;
    row.push_back(cell);
    table.rows.push_back(row);
  }
  ::globfree(&matches);
  return true;
}

// ---------------------------------------------------------------- secure path

static std::string strip_trailing_slash(const std::string &s)
{
  std::string out = s;
  while (out.size() > 1 && out[out.size() - 1] == '/') {
    out.erase(out.size() - 1);
  }
  return out;
}

static bool path_within(const char *path, const char *root)
{
  const std::string p(path);
  const std::string r(root);
  bool within = p.size() >= r.size() && p.compare(0, r.size(), r) == 0;
  if (within && p.size() > r.size() && '/' != r[r.size() - 1] && '/' != p[r.size()]) {
    within = false;
  }
  return within;
}

static bool path_allowed(const ObSQLSessionInfo &session, const std::string &path)
{
  common::ObString priv;
  bool allowed = true;
  if (OB_SUCCESS == session.get_secure_file_priv(priv) && !priv.empty()) {
    const std::string p = trim(std::string(priv.ptr(), static_cast<size_t>(priv.length())));
    if (!p.empty() && p != "/") {
      const size_t wildcard = path.find_first_of("*?[");
      const bool exact = std::string::npos == wildcard;
      char resolved[PATH_MAX];
      char resolved_priv[PATH_MAX];
      const std::string priv_trim = strip_trailing_slash(p);
      if (NULL == ::realpath(priv_trim.c_str(), resolved_priv)) {
        allowed = false;
      } else if (exact && NULL != ::realpath(path.c_str(), resolved)) {
        // Resolve the file itself, not just its directory, so a symbolic link
        // inside the allowed directory cannot read a file outside of it.
        allowed = path_within(resolved, resolved_priv);
      } else {
        // Glob patterns and COPY targets that do not exist yet are checked
        // through their parent directory.
        std::string dir = path;
        if (!exact) {
          dir = dir.substr(0, wildcard);
        }
        const size_t slash = dir.find_last_of('/');
        dir = (std::string::npos == slash) ? "." : dir.substr(0, slash + 1);
        const std::string dir_trim = strip_trailing_slash(dir);
        if (NULL == ::realpath(dir_trim.c_str(), resolved)) {
          allowed = false;
        } else {
          allowed = path_within(resolved, resolved_priv);
        }
      }
    }
  }
  return allowed;
}

// ------------------------------------------------------------ call parsing

struct FileSqlCall
{
  FileSqlCall() : kind(FILE_SQL_CSV), end(0) {}
  FileSqlKind kind;
  std::string name;
  std::string path;
  FileSqlOptions options;
  size_t end;
};

static bool is_file_function_name(const std::string &name, FileSqlKind &kind)
{
  bool found = true;
  if (equal_ci(name, "read_csv") || equal_ci(name, "read_csv_auto")) {
    kind = FILE_SQL_CSV;
  } else if (equal_ci(name, "read_json") || equal_ci(name, "read_json_auto")
             || equal_ci(name, "read_ndjson") || equal_ci(name, "read_ndjson_auto")
             || equal_ci(name, "read_jsonl")) {
    kind = FILE_SQL_JSON;
  } else if (equal_ci(name, "read_parquet") || equal_ci(name, "read_parquet_auto")) {
    kind = FILE_SQL_PARQUET;
  } else if (equal_ci(name, "glob")) {
    kind = FILE_SQL_GLOB;
  } else {
    found = false;
  }
  return found;
}

static bool parse_bool_option(const std::string &v, int &out)
{
  bool ok = true;
  if (equal_ci(v, "true") || v == "1") {
    out = 1;
  } else if (equal_ci(v, "false") || v == "0") {
    out = 2;
  } else {
    ok = false;
  }
  return ok;
}

static int parse_file_arguments(const std::string &func_name,
                                const FileSqlKind kind,
                                const std::string &args_text,
                                FileSqlCall &call)
{
  int ret = OB_SUCCESS;
  std::vector<std::string> parts;
  std::vector<std::string> args;
  if (!split_top_level(args_text, ',', parts)) {
    ret = OB_ERR_PARSE_SQL;
  } else {
    for (size_t i = 0; i < parts.size(); ++i) {
      const std::string arg = trim(parts[i]);
      if (!arg.empty()) {
        args.push_back(arg);
      }
    }
  }
  if (OB_SUCCESS == ret && args.empty()) {
    ret = OB_ERR_PARSE_SQL;
  }
  if (OB_SUCCESS == ret && (!parse_string_literal(args[0], call.path) || call.path.empty())) {
    ret = OB_ERR_PARSE_SQL;
  }
  for (size_t i = 1; OB_SUCCESS == ret && i < args.size(); ++i) {
    const std::string &arg = args[i];
    if (arg[0] == '\'' || arg[0] == '"') {
      ret = OB_ERR_PARSE_SQL;
      break;
    }
    size_t eq = 0;
    if (!find_top_level_equal(arg, eq)) {
      ret = OB_NOT_SUPPORTED;
      break;
    }
    const std::string opt = trim(arg.substr(0, eq));
    const std::string val = trim(arg.substr(eq + 1));
    if (equal_ci(opt, "header") && FILE_SQL_CSV == kind) {
      if (!parse_bool_option(val, call.options.header)) {
        ret = OB_NOT_SUPPORTED;
      }
    } else if (equal_ci(opt, "sample_size") && FILE_SQL_CSV == kind) {
      if (!is_integer_text(val)) {
        ret = OB_NOT_SUPPORTED;
      } else {
        const long long n = ::strtoll(val.c_str(), NULL, 10);
        // Only -1 (scan the whole file) is supported; any other value would
        // change how types are inferred and is rejected rather than ignored.
        if (-1 != n) {
          ret = OB_NOT_SUPPORTED;
        }
      }
    } else if (equal_ci(opt, "format") && FILE_SQL_JSON == kind) {
      std::string f;
      if (!parse_string_literal(val, f) ||
          !(equal_ci(f, "newline_delimited") || equal_ci(f, "ndjson") ||
            equal_ci(f, "jsonl") || equal_ci(f, "auto") || equal_ci(f, "json"))) {
        ret = OB_NOT_SUPPORTED;
      } else {
        call.options.has_format = true;
        call.options.format = f;
      }
    } else if (equal_ci(opt, "format") && FILE_SQL_CSV == kind) {
      std::string f;
      if (!parse_string_literal(val, f) || !equal_ci(f, "csv")) {
        ret = OB_NOT_SUPPORTED;
      }
    } else if ((equal_ci(opt, "delimiter") || equal_ci(opt, "delim") || equal_ci(opt, "sep"))
               && FILE_SQL_CSV == kind) {
      std::string f;
      if (!parse_string_literal(val, f) || f.size() != 1) {
        ret = OB_NOT_SUPPORTED;
      } else {
        call.options.has_delimiter = true;
        call.options.delimiter = f;
      }
    } else if (equal_ci(opt, "quote") && FILE_SQL_CSV == kind) {
      std::string f;
      if (!parse_string_literal(val, f) || f.size() > 1) {
        ret = OB_NOT_SUPPORTED;
      } else {
        call.options.has_quote = true;
        call.options.quote = f;
      }
    } else if (equal_ci(opt, "escape") && FILE_SQL_CSV == kind) {
      std::string f;
      if (!parse_string_literal(val, f) || f.size() > 1) {
        ret = OB_NOT_SUPPORTED;
      } else {
        call.options.has_escape = true;
        call.options.escape = f;
      }
    } else if ((equal_ci(opt, "nullstr") || equal_ci(opt, "null_string") || equal_ci(opt, "null"))
               && FILE_SQL_CSV == kind) {
      std::string f;
      if (!parse_string_literal(val, f)) {
        ret = OB_NOT_SUPPORTED;
      } else {
        call.options.has_nullstr = true;
        call.options.nullstr = f;
      }
    } else {
      ret = OB_NOT_SUPPORTED;
    }
  }
  (void)func_name;
  return ret;
}

static int load_file_table(const std::string &path, const FileSqlCall &call, FileSqlTable &table)
{
  int ret = OB_SUCCESS;
  if (FILE_SQL_GLOB != call.kind && !file_is_regular(path.c_str())) {
    ret = OB_ERR_PARSE_SQL;
  } else if (FILE_SQL_PARQUET == call.kind) {
    if (!load_parquet_table(path, table)) {
      ret = OB_ERR_PARSE_SQL;
    }
  } else if (FILE_SQL_GLOB == call.kind) {
    if (!load_glob_table(path, table)) {
      ret = OB_ERR_PARSE_SQL;
    }
  } else {
    std::string data;
    bool ok = false;
    if (!read_whole_file(path, data)) {
      ret = OB_ERR_PARSE_SQL;
    } else if (FILE_SQL_CSV == call.kind) {
      // A UTF-8 BOM is an encoding marker, not part of the first header name.
      if (data.size() >= 3 && 0xEF == static_cast<unsigned char>(data[0])
          && 0xBB == static_cast<unsigned char>(data[1])
          && 0xBF == static_cast<unsigned char>(data[2])) {
        data.erase(0, 3);
      }
      ok = load_csv_table(data, call.options, table);
    } else if (FILE_SQL_JSON == call.kind) {
      if (data.size() >= 3 && 0xEF == static_cast<unsigned char>(data[0])
          && 0xBB == static_cast<unsigned char>(data[1])
          && 0xBF == static_cast<unsigned char>(data[2])) {
        data.erase(0, 3);
      }
      ok = load_json_table(data, table);
    }
    if (OB_SUCCESS == ret && !ok) {
      ret = OB_ERR_PARSE_SQL;
    }
  }
  return ret;
}

// ---------------------------------------------------------- materialization

static std::string cell_sql(const FileSqlCell &cell, const FileSqlType column_type)
{
  std::string out;
  if (cell.is_null) {
    out = "null";
  } else {
    switch (column_type) {
      case FILE_SQL_BIGINT:
        out = is_integer_text(cell.text) ? cell.text : quote_sql_string(cell.text);
        break;
      case FILE_SQL_DOUBLE: {
        const bool has_exp = std::string::npos != cell.text.find('e') ||
                             std::string::npos != cell.text.find('E');
        if (is_integer_text(cell.text)) {
          out = cell.text + "e0";
        } else if (has_exp) {
          out = cell.text;
        } else if (is_double_text(cell.text)) {
          out = cell.text + "e0";
        } else {
          out = quote_sql_string(cell.text);
        }
        break;
      }
      case FILE_SQL_BOOLEAN:
        if (equal_ci(cell.text, "true")) {
          out = "1";
        } else if (equal_ci(cell.text, "false")) {
          out = "0";
        } else {
          out = cell.text;
        }
        break;
      case FILE_SQL_DATETIME: {
        std::string v = cell.text;
        if (19 == v.size() && 'T' == v[10]) {
          v[10] = ' ';
        }
        out = quote_sql_string(v);
        break;
      }
      case FILE_SQL_DATE:
      case FILE_SQL_VARCHAR:
      default:
        out = quote_sql_string(cell.text);
        break;
    }
  }
  return out;
}

// A file is expanded into one SELECT branch per row, so its rewritten text
// grows with the row count. Bound that text: a scan this large would exhaust
// memory in the parser anyway, and a clear error is better than an OOM.
static bool materialize_table(const FileSqlTable &table, std::string &out)
{
  bool ok = true;
  out.clear();
  out += "(";
  if (table.columns.empty() || table.rows.empty()) {
    out += "select ";
    if (table.columns.empty()) {
      out += "null as `__file_sql_empty`";
    } else {
      for (size_t c = 0; c < table.columns.size(); ++c) {
        if (c > 0) {
          out += ", ";
        }
        out += "null as ";
        out += quote_ident(table.columns[c].name);
      }
    }
    out += " from dual where 1 = 0)";
  } else {
    for (size_t r = 0; ok && r < table.rows.size(); ++r) {
      if (r > 0) {
        out += " union all ";
      }
      out += "select ";
      for (size_t c = 0; ok && c < table.columns.size(); ++c) {
        if (c > 0) {
          out += ", ";
        }
        out += cell_sql(table.rows[r][c], table.columns[c].type);
        // Only the first branch of the union names the columns; repeating the
        // aliases for every row would needlessly inflate the rewritten text.
        if (0 == r) {
          out += " as ";
          out += quote_ident(table.columns[c].name);
        }
      }
      ok = out.size() <= FILE_SQL_MAX_MATERIALIZED_BYTES;
    }
    out += ")";
  }
  return ok;
}

static bool is_clause_keyword(const std::string &word)
{
  static const char *const keywords[] = {
      "where", "order", "group", "having", "limit", "join", "inner", "left",
      "right", "full", "cross", "on", "union", "using", "natural", "as",
      "lateral", "for", "window", "qualify", "offset", "fetch", "straight_join",
      "partition", "use", "force", "ignore"};
  bool found = false;
  for (size_t i = 0; !found && i < sizeof(keywords) / sizeof(keywords[0]); ++i) {
    found = equal_ci(word, keywords[i]);
  }
  return found;
}

static bool has_table_alias(const std::string &sql, size_t pos)
{
  bool alias = false;
  size_t i = pos;
  skip_space_comments(sql, i);
  if (i < sql.size()) {
    if ('`' == sql[i]) {
      alias = true;
    } else if (is_ident_start(sql[i])) {
      const size_t start = i;
      while (i < sql.size() && is_ident_char(sql[i])) {
        ++i;
      }
      alias = !is_clause_keyword(sql.substr(start, i - start));
    }
  }
  return alias;
}

static int rewrite_file_functions(const ObSQLSessionInfo &session,
                                  const std::string &input,
                                  std::string &output,
                                  std::vector<FileSqlTable> *tables,
                                  bool &found)
{
  int ret = OB_SUCCESS;
  output.clear();
  found = false;
  const size_t n = input.size();
  size_t i = 0;
  size_t copied = 0;
  int alias_seq = 0;
  while (OB_SUCCESS == ret && i < n) {
    const char c = input[i];
    if ('\'' == c || '"' == c || '`' == c) {
      if (!skip_quoted(input, i)) {
        ret = OB_ERR_PARSE_SQL;
      }
      continue;
    }
    if ('-' == c || '#' == c || '/' == c) {
      const size_t before = i;
      skip_space_comments(input, i);
      if (i != before) {
        continue;
      }
    }
    if (!is_ident_start(c)) {
      ++i;
      continue;
    }
    const size_t name_start = i;
    while (i < n && is_ident_char(input[i])) {
      ++i;
    }
    const std::string name = input.substr(name_start, i - name_start);
    FileSqlKind kind = FILE_SQL_CSV;
    if (!is_file_function_name(name, kind)) {
      continue;
    }
    size_t open = i;
    skip_space_comments(input, open);
    if (open >= n || '(' != input[open]) {
      continue;
    }
    size_t close = 0;
    if (!find_matching_paren(input, open, close)) {
      ret = OB_ERR_PARSE_SQL;
      break;
    }
    FileSqlCall call;
    call.kind = kind;
    call.name = name;
    call.end = close + 1;
    ret = parse_file_arguments(name, kind, input.substr(open + 1, close - open - 1), call);
    if (OB_SUCCESS != ret) {
      break;
    }
    if (!path_allowed(session, call.path)) {
      ret = OB_NOT_SUPPORTED;
      break;
    }
    FileSqlTable table;
    ret = load_file_table(call.path, call, table);
    if (OB_SUCCESS != ret) {
      break;
    }
    std::string materialized;
    if (!materialize_table(table, materialized)) {
      ret = OB_SIZE_OVERFLOW;
      break;
    }
    output.append(input, copied, name_start - copied);
    output += materialized;
    if (!has_table_alias(input, close + 1)) {
      char alias[48];
      ::snprintf(alias, sizeof(alias), " __file_sql_%d", alias_seq++);
      output += alias;
    }
    copied = close + 1;
    i = close + 1;
    found = true;
    if (NULL != tables) {
      tables->push_back(table);
    }
  }
  if (OB_SUCCESS == ret) {
    output.append(input, copied, n - copied);
  } else {
    output.clear();
  }
  return ret;
}

// ------------------------------------------------------------ DESCRIBE

static int rewrite_describe(const ObSQLSessionInfo &session,
                            const std::string &sql,
                            std::string &output,
                            bool &rewritten)
{
  int ret = OB_SUCCESS;
  rewritten = false;
  size_t i = 0;
  skip_space_comments(sql, i);
  const size_t word_start = i;
  while (i < sql.size() && is_ident_char(sql[i])) {
    ++i;
  }
  const std::string first = sql.substr(word_start, i - word_start);
  if (!equal_ci(first, "describe") && !equal_ci(first, "desc")) {
    return OB_SUCCESS;
  }
  size_t q = i;
  skip_space_comments(sql, q);
  const size_t second_start = q;
  while (q < sql.size() && is_ident_char(sql[q])) {
    ++q;
  }
  const std::string second = sql.substr(second_start, q - second_start);
  if (!equal_ci(second, "select") && !equal_ci(second, "with")) {
    return OB_SUCCESS;
  }
  std::string ignored;
  std::vector<FileSqlTable> tables;
  bool found = false;
  ret = rewrite_file_functions(session, sql, ignored, &tables, found);
  if (OB_SUCCESS == ret && found && !tables.empty()) {
    const FileSqlTable &table = tables[0];
    output.clear();
    output += "select `column_name`, `column_type`, `null`, `key`, `default`, `extra` from (";
    for (size_t c = 0; c < table.columns.size(); ++c) {
      if (c > 0) {
        output += " union all ";
      }
      char ord[32];
      ::snprintf(ord, sizeof(ord), "%lu", static_cast<unsigned long>(c));
      output += "select ";
      output += ord;
      output += " as `__file_sql_ord`, ";
      output += quote_sql_string(table.columns[c].name);
      output += " as `column_name`, ";
      output += quote_sql_string(type_name(table.columns[c].type));
      output += " as `column_type`, ";
      output += quote_sql_string(table.columns[c].nullable ? "YES" : "NO");
      output += " as `null`, null as `key`, null as `default`, null as `extra`";
    }
    if (table.columns.empty()) {
      output += "select 0 as `__file_sql_ord`, null as `column_name`, null as `column_type`, "
                "null as `null`, null as `key`, null as `default`, null as `extra` "
                "from dual where 1 = 0";
    }
    output += ") __file_sql_desc order by `__file_sql_ord`";
    rewritten = true;
  }
  return ret;
}

// ---------------------------------------------------------------- COPY TO

struct CopyOptions
{
  CopyOptions()
      : header(false),
        delimiter(","),
        quote("\""),
        escape("\\"),
        nullstr("") {}
  bool header;
  std::string delimiter;
  std::string quote;
  std::string escape;
  std::string nullstr;
};

static bool read_identifier_token(const std::string &s, size_t &i, std::string &word)
{
  skip_space_comments(s, i);
  const size_t start = i;
  while (i < s.size() && is_ident_char(s[i])) {
    ++i;
  }
  if (i == start) {
    return false;
  }
  word = s.substr(start, i - start);
  return true;
}

static bool read_string_token(const std::string &s, size_t &i, std::string &value)
{
  skip_space_comments(s, i);
  size_t start = i;
  if (i >= s.size() || (s[i] != '\'' && s[i] != '"')) {
    return false;
  }
  if (!skip_quoted(s, i)) {
    return false;
  }
  return parse_string_literal(s.substr(start, i - start), value);
}

static bool find_top_level_word(const std::string &s,
                                const char *word,
                                const size_t from,
                                size_t &pos)
{
  const size_t n = s.size();
  size_t i = from;
  int depth = 0;
  while (i < n) {
    const char c = s[i];
    if ('\'' == c || '"' == c || '`' == c) {
      if (!skip_quoted(s, i)) {
        return false;
      }
      continue;
    }
    if ('(' == c) {
      ++depth;
      ++i;
      continue;
    }
    if (')' == c) {
      --depth;
      ++i;
      continue;
    }
    if (depth > 0) {
      ++i;
      continue;
    }
    if (is_ident_start(c)) {
      const size_t start = i;
      while (i < n && is_ident_char(s[i])) {
        ++i;
      }
      if (equal_ci(s.substr(start, i - start), word)) {
        pos = start;
        return true;
      }
      continue;
    }
    ++i;
  }
  return false;
}

static bool read_option_string(const std::string &part, size_t &p, std::string &value)
{
  skip_space_comments(part, p);
  if (p < part.size() && '=' == part[p]) {
    ++p;
  }
  if (!read_string_token(part, p, value)) {
    return false;
  }
  skip_space_comments(part, p);
  return p == part.size();
}

static bool parse_header_option(const std::string &part, size_t &p, bool &value)
{
  skip_space_comments(part, p);
  if (p >= part.size()) {
    value = true;
    return true;
  }
  if ('=' == part[p]) {
    ++p;
    skip_space_comments(part, p);
  }
  std::string word;
  if (!read_identifier_token(part, p, word)) {
    return false;
  }
  skip_space_comments(part, p);
  if (p != part.size()) {
    return false;
  }
  if (equal_ci(word, "true") || "1" == word) {
    value = true;
  } else if (equal_ci(word, "false") || "0" == word) {
    value = false;
  } else {
    return false;
  }
  return true;
}

static int parse_copy_options(const std::string &text, CopyOptions &opts)
{
  int ret = OB_SUCCESS;
  std::vector<std::string> parts;
  if (!split_top_level(text, ',', parts)) {
    return OB_ERR_PARSE_SQL;
  }
  for (size_t i = 0; OB_SUCCESS == ret && i < parts.size(); ++i) {
    const std::string part = trim(parts[i]);
    if (part.empty()) {
      continue;
    }
    size_t p = 0;
    std::string name;
    if (!read_identifier_token(part, p, name)) {
      ret = OB_ERR_PARSE_SQL;
    } else if (equal_ci(name, "header")) {
      if (!parse_header_option(part, p, opts.header)) {
        ret = OB_ERR_PARSE_SQL;
      }
    } else if (equal_ci(name, "format")) {
      std::string v;
      bool ok = read_identifier_token(part, p, v) || read_string_token(part, p, v);
      if (!ok || !equal_ci(v, "csv")) {
        ret = OB_NOT_SUPPORTED;
      }
    } else if (equal_ci(name, "delimiter") || equal_ci(name, "field_delimiter")
               || equal_ci(name, "sep")) {
      std::string v;
      if (!read_option_string(part, p, v) || v.size() != 1) {
        ret = OB_NOT_SUPPORTED;
      } else {
        opts.delimiter = v;
      }
    } else if (equal_ci(name, "quote") || equal_ci(name, "quote_char")
               || equal_ci(name, "field_optionally_enclosed_by")) {
      std::string v;
      if (!read_option_string(part, p, v) || v.size() > 1) {
        ret = OB_NOT_SUPPORTED;
      } else {
        opts.quote = v;
      }
    } else if (equal_ci(name, "escape") || equal_ci(name, "escape_char")) {
      std::string v;
      if (!read_option_string(part, p, v) || v.size() > 1) {
        ret = OB_NOT_SUPPORTED;
      } else {
        opts.escape = v;
      }
    } else if (equal_ci(name, "nullstr") || equal_ci(name, "null_string")
               || equal_ci(name, "null")) {
      if (!read_option_string(part, p, opts.nullstr)) {
        ret = OB_NOT_SUPPORTED;
      }
    } else {
      ret = OB_NOT_SUPPORTED;
    }
  }
  return ret;
}

static bool normalize_column_name(const std::string &raw, std::string &name)
{
  std::string text = trim(raw);
  if (text.size() >= 2 && '`' == text[0] && '`' == text[text.size() - 1]) {
    std::string unquoted;
    for (size_t i = 1; i + 1 < text.size(); ++i) {
      unquoted.push_back(text[i]);
      if ('`' == text[i] && i + 2 < text.size() && '`' == text[i + 1]) {
        ++i;
      }
    }
    text = unquoted;
  }
  const size_t dot = text.find_last_of('.');
  if (std::string::npos != dot) {
    text = trim(text.substr(dot + 1));
  }
  name = text;
  return !name.empty();
}

static bool find_table_column(const FileSqlTable &table, const std::string &raw, size_t &index)
{
  std::string name;
  if (!normalize_column_name(raw, name)) {
    return false;
  }
  bool found = false;
  for (size_t c = 0; !found && c < table.columns.size(); ++c) {
    if (equal_ci(table.columns[c].name, name.c_str()) ||
        table.columns[c].name == name) {
      index = c;
      found = true;
    }
  }
  return found;
}

static bool parse_projection(const std::string &select_list,
                             const FileSqlTable &table,
                             std::vector<size_t> &projection)
{
  projection.clear();
  const std::string list = trim(select_list);
  if ("*" == list) {
    for (size_t c = 0; c < table.columns.size(); ++c) {
      projection.push_back(c);
    }
    return true;
  }
  std::vector<std::string> items;
  if (!split_top_level(list, ',', items)) {
    return false;
  }
  for (size_t i = 0; i < items.size(); ++i) {
    std::string item = trim(items[i]);
    if (item.empty()) {
      return false;
    }
    size_t as_pos = 0;
    if (find_top_level_word(item, "as", 0, as_pos)) {
      item = trim(item.substr(0, as_pos));
    }
    size_t index = 0;
    if (!find_table_column(table, item, index)) {
      return false;
    }
    projection.push_back(index);
  }
  return true;
}

static bool parse_order_keys(const std::vector<std::string> &items,
                             const FileSqlTable &table,
                             std::vector<std::pair<size_t, bool> > &keys)
{
  bool ok = true;
  for (size_t i = 0; ok && i < items.size(); ++i) {
    std::string item = trim(items[i]);
    if (item.empty()) {
      ok = false;
      break;
    }
    std::string column_text;
    size_t p = 0;
    if (!read_identifier_token(item, p, column_text)) {
      // allow quoted identifiers
      const size_t start = p;
      if (p < item.size() && '`' == item[p]) {
        if (!skip_quoted(item, p)) {
          ok = false;
          break;
        }
        column_text = item.substr(start, p - start);
      } else {
        ok = false;
        break;
      }
    }
    bool ascending = true;
    std::string tail = trim(item.substr(p));
    if (equal_ci(tail, "desc")) {
      ascending = false;
    } else if (!tail.empty() && !equal_ci(tail, "asc")) {
      ok = false;
      break;
    }
    size_t index = 0;
    if (!find_table_column(table, column_text, index)) {
      ok = false;
      break;
    }
    keys.push_back(std::make_pair(index, ascending));
  }
  return ok;
}

static int compare_cells(const FileSqlCell &a, const FileSqlCell &b)
{
  int result = 0;
  if (a.is_null && b.is_null) {
    result = 0;
  } else if (a.is_null) {
    result = -1;
  } else if (b.is_null) {
    result = 1;
  } else if ((is_integer_text(a.text) || is_double_text(a.text)) &&
             (is_integer_text(b.text) || is_double_text(b.text))) {
    const long double left = ::strtold(a.text.c_str(), NULL);
    const long double right = ::strtold(b.text.c_str(), NULL);
    result = (left < right) ? -1 : ((left > right) ? 1 : 0);
  } else {
    const int c = a.text.compare(b.text);
    result = (c < 0) ? -1 : ((c > 0) ? 1 : 0);
  }
  return result;
}

static bool path_exists(const std::string &path)
{
  struct stat st;
  return 0 == ::stat(path.c_str(), &st);
}

static std::string csv_field(const std::string &v, const CopyOptions &opts, const bool force_quote)
{
  const char delimiter = opts.delimiter.empty() ? ',' : opts.delimiter[0];
  const bool has_quote = !opts.quote.empty();
  const char quote = has_quote ? opts.quote[0] : '"';
  const bool has_escape = !opts.escape.empty();
  const char escape = has_escape ? opts.escape[0] : '\\';
  bool need_quote = has_quote && (v.empty() || force_quote);
  for (size_t i = 0; !need_quote && i < v.size(); ++i) {
    const char c = v[i];
    if (c == delimiter || c == '\n' || c == '\r') {
      need_quote = true;
    } else if (has_quote && (c == quote || (has_escape && c == escape))) {
      need_quote = true;
    }
  }
  if (!need_quote) {
    return v;
  }
  std::string out;
  out.push_back(quote);
  for (size_t i = 0; i < v.size(); ++i) {
    const char c = v[i];
    if (has_escape && (c == escape || c == quote)) {
      out.push_back(escape);
      out.push_back(c);
    } else if (!has_escape && c == quote) {
      // Without an escape character the only portable way to keep a quote
      // inside a quoted field is to double it.
      out.push_back(quote);
      out.push_back(quote);
    } else {
      out.push_back(c);
    }
  }
  out.push_back(quote);
  return out;
}

static bool write_copy_file(const std::string &path,
                            const FileSqlTable &table,
                            const std::vector<size_t> &projection,
                            const std::vector<size_t> &order,
                            const CopyOptions &opts)
{
  bool ok = true;
  const char delimiter = opts.delimiter.empty() ? ',' : opts.delimiter[0];
  // Write next to the target and rename, so a failed export never leaves a
  // half-written file under the requested name.
  const std::string tmp_path = path + ".file_sql_tmp";
  FILE *fp = ::fopen(tmp_path.c_str(), "wb");
  if (NULL == fp) {
    return false;
  }
  if (opts.header) {
    for (size_t c = 0; ok && c < projection.size(); ++c) {
      const std::string field = csv_field(table.columns[projection[c]].name, opts, false);
      if (c > 0 && ::fputc(delimiter, fp) == EOF) {
        ok = false;
      }
      if (ok && ::fwrite(field.data(), 1, field.size(), fp) != field.size()) {
        ok = false;
      }
    }
    if (ok && ::fputc('\n', fp) == EOF) {
      ok = false;
    }
  }
  for (size_t r = 0; ok && r < order.size(); ++r) {
    const std::vector<FileSqlCell> &row = table.rows[order[r]];
    for (size_t c = 0; ok && c < projection.size(); ++c) {
      const FileSqlCell &cell = row[projection[c]];
      std::string field;
      if (cell.is_null) {
        field = opts.nullstr;
      } else {
        // A text value that happens to spell the NULL marker must stay quoted,
        // otherwise reading the file back would turn it into a NULL.
        const bool force_quote = opts.nullstr.empty() ? (cell.text == "\\N")
                                                      : (cell.text == opts.nullstr);
        field = csv_field(cell.text, opts, force_quote);
      }
      if (c > 0 && ::fputc(delimiter, fp) == EOF) {
        ok = false;
      }
      if (ok && ::fwrite(field.data(), 1, field.size(), fp) != field.size()) {
        ok = false;
      }
    }
    if (ok && ::fputc('\n', fp) == EOF) {
      ok = false;
    }
  }
  if (0 != ::fclose(fp)) {
    ok = false;
  }
  if (ok && 0 != ::rename(tmp_path.c_str(), path.c_str())) {
    ok = false;
  }
  if (!ok) {
    ::remove(tmp_path.c_str());
  }
  return ok;
}

static int evaluate_copy_source(const ObSQLSessionInfo &session,
                                const std::string &query,
                                const std::string &out_path,
                                const CopyOptions &opts)
{
  int ret = OB_SUCCESS;
  size_t i = 0;
  std::string first;
  if (!read_identifier_token(query, i, first) || !equal_ci(first, "select")) {
    return OB_NOT_SUPPORTED;
  }
  size_t from_pos = 0;
  if (!find_top_level_word(query, "from", i, from_pos)) {
    return OB_NOT_SUPPORTED;
  }
  const std::string select_list = trim(query.substr(i, from_pos - i));
  size_t src = from_pos + 4;
  skip_space_comments(query, src);
  size_t name_start = src;
  while (src < query.size() && is_ident_char(query[src])) {
    ++src;
  }
  const std::string fname = query.substr(name_start, src - name_start);
  FileSqlKind kind = FILE_SQL_CSV;
  if (!is_file_function_name(fname, kind)) {
    return OB_NOT_SUPPORTED;
  }
  size_t open = src;
  skip_space_comments(query, open);
  if (open >= query.size() || '(' != query[open]) {
    return OB_NOT_SUPPORTED;
  }
  size_t close = 0;
  if (!find_matching_paren(query, open, close)) {
    return OB_ERR_PARSE_SQL;
  }
  FileSqlCall call;
  call.kind = kind;
  call.name = fname;
  call.end = close + 1;
  ret = parse_file_arguments(fname, kind, query.substr(open + 1, close - open - 1), call);
  if (OB_SUCCESS != ret) {
    return ret;
  }
  if (!path_allowed(session, call.path)) {
    return OB_NOT_SUPPORTED;
  }
  size_t tail = close + 1;
  skip_space_comments(query, tail);
  if (tail < query.size() && is_ident_start(query[tail])) {
    size_t w = tail;
    while (w < query.size() && is_ident_char(query[w])) {
      ++w;
    }
    const std::string word = query.substr(tail, w - tail);
    if (equal_ci(word, "as")) {
      tail = w;
      std::string alias;
      if (!read_identifier_token(query, tail, alias)) {
        return OB_ERR_PARSE_SQL;
      }
    } else if (!is_clause_keyword(word)) {
      tail = w;
    }
  }
  skip_space_comments(query, tail);
  std::vector<std::string> order_items;
  if (tail < query.size()) {
    std::string word;
    if (!read_identifier_token(query, tail, word) || !equal_ci(word, "order")) {
      return OB_NOT_SUPPORTED;
    }
    std::string by_word;
    if (!read_identifier_token(query, tail, by_word) || !equal_ci(by_word, "by")) {
      return OB_ERR_PARSE_SQL;
    }
    if (!split_top_level(trim(query.substr(tail)), ',', order_items)) {
      return OB_ERR_PARSE_SQL;
    }
  }
  FileSqlTable table;
  ret = load_file_table(call.path, call, table);
  if (OB_SUCCESS != ret) {
    return ret;
  }
  std::vector<size_t> projection;
  if (!parse_projection(select_list, table, projection)) {
    return OB_NOT_SUPPORTED;
  }
  std::vector<std::pair<size_t, bool> > keys;
  if (!parse_order_keys(order_items, table, keys)) {
    return OB_NOT_SUPPORTED;
  }
  std::vector<size_t> order(table.rows.size());
  for (size_t r = 0; r < order.size(); ++r) {
    order[r] = r;
  }
  std::stable_sort(order.begin(), order.end(), [&](const size_t x, const size_t y) {
    for (size_t k = 0; k < keys.size(); ++k) {
      const int c = compare_cells(table.rows[x][keys[k].first],
                                  table.rows[y][keys[k].first]);
      if (0 != c) {
        return keys[k].second ? (c < 0) : (c > 0);
      }
    }
    return x < y;
  });
  if (!write_copy_file(out_path, table, projection, order, opts)) {
    ret = OB_IO_ERROR;
  }
  return ret;
}

static bool contains_file_function(const std::string &sql);

static bool is_table_reference(const std::string &text)
{
  bool ok = !text.empty();
  for (size_t i = 0; ok && i < text.size(); ++i) {
    const char c = text[i];
    if (!is_ident_char(c) && '.' != c && '`' != c && ' ' != c && '\t' != c
        && '\n' != c && '\r' != c) {
      ok = false;
    }
  }
  return ok;
}

static void append_copy_format(std::string &sql, const CopyOptions &opts)
{
  sql += " format = (type = 'CSV', field_delimiter = ";
  sql += quote_sql_string(opts.delimiter.empty() ? std::string(",") : opts.delimiter);
  if (!opts.quote.empty()) {
    sql += ", field_optionally_enclosed_by = ";
    sql += quote_sql_string(opts.quote);
  }
  if (!opts.escape.empty()) {
    sql += ", escape = ";
    sql += quote_sql_string(opts.escape);
  }
  if (opts.header) {
    sql += ", parse_header = true";
  }
  // Always pin the NULL text, including the empty string, so a NULL column is
  // distinct from an empty string when the file is read back.
  sql += ", null_if = (";
  sql += quote_sql_string(opts.nullstr);
  sql += "))";
}

// Build the engine statement for COPY sources that are not a single file
// function table. Executing through SELECT ... INTO OUTFILE keeps the export
// inside the session (same database, transaction and privilege context), lets
// the engine stream the rows, and reuses the CSV writer options.
static int build_copy_outfile(const ObSQLSessionInfo &session,
                              const std::string &inner,
                              const std::string &path,
                              const CopyOptions &opts,
                              std::string &output)
{
  int ret = OB_SUCCESS;
  std::string select_sql = trim(inner);
  if (select_sql.empty()) {
    ret = OB_ERR_PARSE_SQL;
  } else {
    size_t first = 0;
    std::string word;
    bool is_query = false;
    if (!read_identifier_token(select_sql, first, word)) {
      // Not an identifier: fall through to the table-reference check below.
    } else if (equal_ci(word, "select") || equal_ci(word, "values")) {
      is_query = true;
    } else if (equal_ci(word, "table")) {
      // DuckDB's TABLE t shorthand: only valid as a query in its own dialect,
      // so answer it with the equivalent SELECT.
      select_sql = "select * from " + trim(select_sql.substr(first));
      is_query = true;
    } else if (equal_ci(word, "with")) {
      // The select_into production does not accept a WITH clause before its
      // INTO clause, so nest the query instead of appending it directly.
      select_sql = "select * from (" + select_sql + ") __ob_file_sql_copy";
      is_query = true;
    }
    if (!is_query && !select_sql.empty() && '(' == select_sql[0]) {
      // A parenthesised query expression is not accepted where a table name is
      // expected, so make it the derived table of a plain SELECT.
      select_sql = "select * from (" + select_sql + ") __ob_file_sql_copy";
      is_query = true;
    }
    if (is_query) {
      // keep the query text as written
    } else if (!is_table_reference(select_sql)) {
      ret = OB_ERR_PARSE_SQL;
    } else {
      select_sql = "select * from " + select_sql;
    }
  }
  if (OB_SUCC(ret) && contains_file_function(select_sql)) {
    std::string rewritten_select;
    bool found = false;
    ret = rewrite_file_functions(session, select_sql, rewritten_select, NULL, found);
    if (OB_SUCC(ret) && found) {
      select_sql = rewritten_select;
    }
  }
  if (OB_SUCC(ret)) {
    output = select_sql;
    output += " into outfile ";
    output += quote_sql_string(path);
    append_copy_format(output, opts);
  }
  return ret;
}

static int rewrite_copy(const ObSQLSessionInfo &session,
                        const std::string &sql,
                        std::string &output,
                        bool &rewritten)
{
  int ret = OB_SUCCESS;
  rewritten = false;
  size_t i = 0;
  std::string word;
  if (!read_identifier_token(sql, i, word) || !equal_ci(word, "copy")) {
    return OB_SUCCESS;
  }
  skip_space_comments(sql, i);
  if (i >= sql.size() || '(' != sql[i]) {
    return OB_ERR_PARSE_SQL;
  }
  size_t inner_close = 0;
  if (!find_matching_paren(sql, i, inner_close)) {
    return OB_ERR_PARSE_SQL;
  }
  const std::string inner = sql.substr(i + 1, inner_close - i - 1);
  size_t p = inner_close + 1;
  skip_space_comments(sql, p);
  if (!read_identifier_token(sql, p, word) || !equal_ci(word, "to")) {
    return OB_ERR_PARSE_SQL;
  }
  skip_space_comments(sql, p);
  std::string path;
  if (!read_string_token(sql, p, path) || path.empty()) {
    return OB_ERR_PARSE_SQL;
  }
  if (!path_allowed(session, path)) {
    return OB_NOT_SUPPORTED;
  }
  CopyOptions opts;
  skip_space_comments(sql, p);
  if (p < sql.size() && '(' == sql[p]) {
    size_t opt_close = 0;
    if (!find_matching_paren(sql, p, opt_close)) {
      return OB_ERR_PARSE_SQL;
    }
    ret = parse_copy_options(sql.substr(p + 1, opt_close - p - 1), opts);
    p = opt_close + 1;
  }
  if (OB_SUCC(ret) && path_exists(path)) {
    // SELECT INTO OUTFILE refuses to clobber an existing file; COPY keeps the
    // same contract instead of silently replacing someone else's data.
    ret = OB_FILE_ALREADY_EXIST;
  }
  if (OB_SUCC(ret)) {
    // A plain scan of one file function is answered in place: the file is read
    // once here and never has to travel through the SQL layer. Any other
    // source (real tables, joins, aggregates) is handed to the engine as
    // SELECT ... INTO OUTFILE so it streams instead of being materialized.
    const int local_ret = evaluate_copy_source(session, inner, path, opts);
    if (OB_SUCCESS == local_ret) {
      // The file is already written above, so the statement sent on to the
      // executor only has to succeed. It must not return a result set: clients
      // print the column header even when a result set has no rows, and COPY is
      // expected to answer with a plain OK packet.
      output = "set @ob_file_sql_copy_done = 1";
      rewritten = true;
    } else if (OB_NOT_SUPPORTED == local_ret) {
      ret = build_copy_outfile(session, inner, path, opts, output);
      rewritten = (OB_SUCCESS == ret);
    } else {
      ret = local_ret;
    }
  }
  return ret;
}

// --------------------------------------------------------------- entry point

static bool contains_file_function(const std::string &sql)
{
  bool found = false;
  const size_t n = sql.size();
  size_t i = 0;
  while (!found && i < n) {
    const char c = sql[i];
    if ('\'' == c || '"' == c || '`' == c) {
      if (!skip_quoted(sql, i)) {
        break;
      }
      continue;
    }
    if (!is_ident_start(c)) {
      ++i;
      continue;
    }
    const size_t start = i;
    while (i < n && is_ident_char(sql[i])) {
      ++i;
    }
    FileSqlKind kind = FILE_SQL_CSV;
    if (is_file_function_name(sql.substr(start, i - start), kind)) {
      size_t open = i;
      skip_space_comments(sql, open);
      if (open < n && '(' == sql[open]) {
        found = true;
      }
    }
  }
  return found;
}

static bool is_copy_statement(const std::string &sql)
{
  size_t i = 0;
  std::string word;
  bool is_copy = false;
  if (read_identifier_token(sql, i, word)) {
    is_copy = equal_ci(word, "copy");
  }
  return is_copy;
}

static bool is_describe_statement(const std::string &sql)
{
  size_t i = 0;
  std::string first;
  bool described = false;
  if (read_identifier_token(sql, i, first) &&
      (equal_ci(first, "describe") || equal_ci(first, "desc"))) {
    std::string second;
    if (read_identifier_token(sql, i, second) &&
        (equal_ci(second, "select") || equal_ci(second, "with"))) {
      described = true;
    }
  }
  return described;
}

static void log_file_sql_error(const int code)
{
  if (OB_NOT_SUPPORTED == code) {
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "file table function");
  } else if (OB_FILE_ALREADY_EXIST == code) {
    LOG_USER_ERROR(OB_FILE_ALREADY_EXIST);
  } else if (OB_IO_ERROR == code) {
    LOG_USER_ERROR(OB_IO_ERROR);
  } else {
    const char *msg = "invalid file table function";
    LOG_USER_ERROR(OB_ERR_PARSE_SQL, ob_errpkt_strerror(OB_ERR_PARSER_SYNTAX),
                   static_cast<int>(::strlen(msg)), msg, 0);
  }
}

} // namespace

int ObFileSqlRewriter::rewrite(common::ObIAllocator &allocator,
                               const ObSQLSessionInfo &session,
                               const common::ObString &sql,
                               common::ObString &rewritten_sql,
                               bool &rewritten)
{
  int ret = OB_SUCCESS;
  rewritten = false;
  rewritten_sql.reset();
  if (sql.empty()) {
  } else {
    const std::string text(sql.ptr(), static_cast<size_t>(sql.length()));
    const bool copy_stmt = file_sql_impl::is_copy_statement(text);
    if (!copy_stmt && !file_sql_impl::contains_file_function(text)) {
      // Ordinary statement: leave it untouched.
    } else {
      bool enabled = false;
      if (OB_FAIL(session.get_enable_file_sql(enabled))) {
      } else if (!enabled) {
        ret = OB_NOT_SUPPORTED;
      } else {
        std::string output;
        bool changed = false;
        if (copy_stmt) {
          ret = file_sql_impl::rewrite_copy(session, text, output, changed);
        } else if (file_sql_impl::is_describe_statement(text)) {
          ret = file_sql_impl::rewrite_describe(session, text, output, changed);
          if (OB_SUCCESS == ret && !changed) {
            std::vector<file_sql_impl::FileSqlTable> tables;
            bool found = false;
            ret = file_sql_impl::rewrite_file_functions(session, text, output, &tables, found);
            changed = found;
          }
        } else {
          std::vector<file_sql_impl::FileSqlTable> tables;
          bool found = false;
          ret = file_sql_impl::rewrite_file_functions(session, text, output, &tables, found);
          changed = found;
        }
        if (OB_SUCCESS == ret && changed) {
          char *buffer = static_cast<char *>(allocator.alloc(output.size()));
          if (NULL == buffer) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            ::memcpy(buffer, output.data(), output.size());
            rewritten_sql.assign_ptr(buffer, static_cast<int32_t>(output.size()));
            rewritten = true;
          }
        }
      }
      if (OB_SUCCESS != ret) {
        file_sql_impl::log_file_sql_error(ret);
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
