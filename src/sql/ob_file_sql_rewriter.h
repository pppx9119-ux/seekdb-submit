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

#ifndef OCEANBASE_SQL_OB_FILE_SQL_REWRITER_
#define OCEANBASE_SQL_OB_FILE_SQL_REWRITER_

#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;

// Rewrites file-table statements into plain SQL that the regular resolver and
// executor already understand:
//   read_csv/read_json/read_ndjson/read_parquet/glob(...) -> derived table of literals
//   DESCRIBE SELECT ... FROM file(...)                   -> metadata result set
//   COPY (query) TO 'path' (format csv, ...)             -> SELECT ... INTO OUTFILE
class ObFileSqlRewriter
{
public:
  static int rewrite(common::ObIAllocator &allocator,
                     const ObSQLSessionInfo &session,
                     const common::ObString &sql,
                     common::ObString &rewritten_sql,
                     bool &rewritten);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_FILE_SQL_REWRITER_
