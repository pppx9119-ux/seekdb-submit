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

#ifndef OCEANBASE_SQL_OB_AI_COLUMN_REWRITER_H_
#define OCEANBASE_SQL_OB_AI_COLUMN_REWRITER_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;

// Rewrites AI COLUMN statements into plain SQL before the statement reaches the
// parser:
//   CREATE TABLE ... <col> <type> AI COLUMN (func(...)) WITH (...)
//       -> the result column plus three hidden companion columns
//   ALTER TABLE ... ADD COLUMN ... AI COLUMN (...)
//       -> the same, appended to an existing table
//   ALTER TABLE ... RETRY AI COLUMN <col> [WHERE ...]
//       -> UPDATE resetting the column with the STATUS pseudo column expanded
//   UPDATE ... / INSERT ...
//       -> appended invalidation of the AI columns whose inputs changed
// It also expands oceanbase.__all_ai_column and oceanbase.__all_ai_column_task
// into derived tables and enforces ai_column_strict_read.
class ObAIColumnRewriter
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

#endif // OCEANBASE_SQL_OB_AI_COLUMN_REWRITER_H_
