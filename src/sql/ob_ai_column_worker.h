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

#ifndef OCEANBASE_SQL_OB_AI_COLUMN_WORKER_H_
#define OCEANBASE_SQL_OB_AI_COLUMN_WORKER_H_

#include <atomic>
#include <cstdint>
#include <map>
#include <string>
#include <thread>

#include "sql/ob_ai_column_common.h"

namespace oceanbase
{
namespace sql
{
namespace ai_column
{

// Runs the asynchronous side of an AI column. It scans the rows that still
// need a value, calls the model endpoint and publishes the answer guarded by
// the request generation of the row, so a response that arrives after the row
// changed is dropped instead of overwriting the newer value.
class ObAIColumnWorker
{
public:
  static ObAIColumnWorker &get_instance();
  // Starts the background thread. Called when the first AI column is created,
  // so repeated calls are cheap.
  int start();
  // Stops and joins the background thread. Called from ObServer::destroy().
  void stop();

private:
  ObAIColumnWorker();
  ObAIColumnWorker(const ObAIColumnWorker &) = delete;
  ObAIColumnWorker &operator=(const ObAIColumnWorker &) = delete;

  void run();
  void tick();
  int process_table(const ObAIColumnEntry &entry);
  int call_model(const ObAIColumnSpec &spec,
                 const std::string &input,
                 std::string &value);
  int refresh_unfinished(const ObAIColumnEntry &entry);

  std::atomic<bool> started_;
  std::atomic<bool> stopping_;
  std::thread thread_;
  // Retry backoff keyed by database, table, column and row key. Only the
  // background thread touches this map.
  std::map<std::string, int64_t> retry_due_us_;
  // The last completeness check of a table, so an idle table is not scanned on
  // every tick.
  std::map<std::string, int64_t> last_refresh_us_;
};

} // namespace ai_column
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_AI_COLUMN_WORKER_H_
