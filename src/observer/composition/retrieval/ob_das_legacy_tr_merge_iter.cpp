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

#define USING_LOG_PREFIX SQL_DAS
#include "observer/composition/retrieval/ob_das_legacy_tr_merge_iter.h"

#include <atomic>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "share/rc/ob_server_runtime.h"
#include "share/scn.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "sql/das/iter/ob_das_text_retrieval_eval_node.h"
#include "sql/das/iter/sparse_retrieval/ob_das_text_retrieval_engine.h"
#include "sql/das/ob_das_ir_define.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{
using namespace share;
namespace sql
{

// A completed natural-language merge result for one retrieval scan.  The rows
// are the exact id/relevance projections the merge iterator emitted, so
// replaying them reproduces the scan output without touching the index.
struct ObFTSReplayRow
{
  int64_t id_;        // integer id, or offset into ObFTSReplayEntry::id_bytes_
  int64_t id_len_;    // byte length when the id is not an integer
  double relevance_;
};

struct ObFTSReplayEntry
{
  ObFTSReplayEntry()
    : key_(),
      version_(),
      commit_ts_(),
      id_bytes_(),
      rows_(),
      max_query_score_(0.0),
      max_query_score_err_(OB_SUCCESS),
      last_use_(0),
      writes_relevance_(false),
      id_is_int_(false),
      score_queried_(false),
      score_unstable_(false),
      ends_with_limit_(false),
      complete_(false)
  {}

  std::string key_;
  // The read snapshot the rows were produced at.  A replay is only valid for a
  // snapshot that sees at least this version.
  share::SCN version_;
  // The largest commit version the transaction service had observed when the
  // rows were produced.  Every commit it had observed was at or below the read
  // snapshot, so as long as this value stays put no locally observed commit can
  // have changed the table since.
  share::SCN commit_ts_;
  std::string id_bytes_;
  std::vector<ObFTSReplayRow> rows_;
  double max_query_score_;
  int max_query_score_err_;
  int64_t last_use_;
  bool writes_relevance_;
  bool id_is_int_;
  bool score_queried_;
  bool score_unstable_;
  // The recorded scan stopped because it reached the pushed-down limit, so the
  // end of the rows is reported together with the last row, as the live
  // iterator does.
  bool ends_with_limit_;
  bool complete_;
};

namespace
{

// Diagnostic tracing for the replay cache.  The server emits these lines only
// when OB_FTS_REPLAY_TRACE is set, and the budget keeps a long benchmark from
// flooding the log.
bool replay_trace()
{
  static const bool enabled = nullptr != ::getenv("OB_FTS_REPLAY_TRACE");
  if (!enabled) {
    return false;
  }
  static std::atomic<int64_t> remaining(20000);
  return remaining.fetch_sub(1) > 0;
}

constexpr int64_t REPLAY_CACHE_MAX_ENTRIES = 32;
constexpr int64_t REPLAY_CACHE_MAX_BYTES = 64LL << 20;
constexpr int64_t REPLAY_MAX_ROWS = 1000000;
constexpr int64_t REPLAY_MAX_ID_BYTES = 32LL << 20;

int64_t replay_entry_bytes(const ObFTSReplayEntry &entry)
{
  return static_cast<int64_t>(sizeof(ObFTSReplayEntry) + entry.key_.capacity()
      + entry.id_bytes_.capacity() + entry.rows_.capacity() * sizeof(ObFTSReplayRow));
}

class ObFTSReplayCache
{
public:
  static ObFTSReplayCache &instance()
  {
    static ObFTSReplayCache cache;
    return cache;
  }

  std::shared_ptr<ObFTSReplayEntry> get(const std::string &key)
  {
    std::lock_guard<std::mutex> guard(lock_);
    std::shared_ptr<ObFTSReplayEntry> entry;
    std::unordered_map<std::string, std::shared_ptr<ObFTSReplayEntry>>::iterator iter =
        entries_.find(key);
    if (iter != entries_.end()) {
      iter->second->last_use_ = ++use_seq_;
      entry = iter->second;
    }
    return entry;
  }

  void put(const std::shared_ptr<ObFTSReplayEntry> &entry)
  {
    if (nullptr == entry) {
      return;
    }
    std::lock_guard<std::mutex> guard(lock_);
    std::unordered_map<std::string, std::shared_ptr<ObFTSReplayEntry>>::iterator iter =
        entries_.find(entry->key_);
    if (iter != entries_.end()) {
      total_bytes_ -= replay_entry_bytes(*iter->second);
      iter->second = entry;
    } else {
      entries_.insert(std::make_pair(entry->key_, entry));
    }
    entry->last_use_ = ++use_seq_;
    total_bytes_ += replay_entry_bytes(*entry);
    while (entries_.size() > 1
           && (static_cast<int64_t>(entries_.size()) > REPLAY_CACHE_MAX_ENTRIES
               || total_bytes_ > REPLAY_CACHE_MAX_BYTES)) {
      std::unordered_map<std::string, std::shared_ptr<ObFTSReplayEntry>>::iterator victim =
          entries_.begin();
      for (std::unordered_map<std::string, std::shared_ptr<ObFTSReplayEntry>>::iterator it =
               entries_.begin(); it != entries_.end(); ++it) {
        if (it->second->last_use_ < victim->second->last_use_) {
          victim = it;
        }
      }
      total_bytes_ -= replay_entry_bytes(*victim->second);
      entries_.erase(victim);
    }
  }

private:
  ObFTSReplayCache() : lock_(), entries_(), total_bytes_(0), use_seq_(0) {}

  std::mutex lock_;
  std::unordered_map<std::string, std::shared_ptr<ObFTSReplayEntry>> entries_;
  int64_t total_bytes_;
  int64_t use_seq_;
};

template <typename T>
void append_replay_key_value(std::string &key, const T &value)
{
  key.append(reinterpret_cast<const char *>(&value), sizeof(T));
}

void append_replay_key_string(std::string &key, const ObString &value)
{
  const int32_t length = value.length();
  append_replay_key_value(key, length);
  if (length > 0) {
    key.append(value.ptr(), length);
  }
}

// Write a recorded document id exactly like the sparse retrieval merge writes
// the id it read from the inverted index: as an integer for an integer id
// column, and as a shallow string otherwise.
void write_replay_id_datum(
    ObDatum &datum,
    const ObFTSReplayEntry &entry,
    const ObFTSReplayRow &row)
{
  if (entry.id_is_int_) {
    datum.set_int(row.id_);
  } else {
    datum.set_string(entry.id_bytes_.data() + row.id_, static_cast<uint32_t>(row.id_len_));
  }
}

transaction::ObTransService *get_trans_service()
{
  return share::server_service<transaction::ObTransService>();
}

share::SCN get_data_version()
{
  share::SCN version;
  transaction::ObTransService *trans_service = get_trans_service();
  if (OB_NOT_NULL(trans_service)) {
    // Every commit the server has applied must move this version, including an
    // ELR commit whose timestamp is tracked separately.
    version = trans_service->get_tx_version_mgr().get_max_commit_ts(true /*with_elr*/);
  }
  return version;
}

// A recorded merge result may only be replayed by, and may only be collected
// from, a plain SELECT that runs without a transaction.  Such a scan owns no
// writes and cannot observe an uncommitted write of its own session, so the
// rows can only describe committed data.  DML plans and scans inside a
// transaction (an explicit one, autocommit=0, or one opened by a statement
// trigger) are excluded, so an uncommitted write can never enter the cache and
// a replay can never hide one.  A plain autocommit SELECT runs without any
// transaction descriptor at all, so an empty descriptor is the expected state
// and not a reason to reject the scan.
bool replay_eligible(transaction::ObTxDesc *tx_desc, ObExecContext *exec_ctx)
{
  bool eligible = false;
  bool owns_desc = false;
  bool in_tx = false;
  bool plain_select = false;
  ObSQLSessionInfo *session = nullptr;
  const ObPhysicalPlanCtx *plan_ctx = nullptr;
  const ObPhysicalPlan *plan = nullptr;
  if (OB_NOT_NULL(exec_ctx)
      && OB_NOT_NULL(session = exec_ctx->get_my_session())
      && OB_NOT_NULL(plan_ctx = exec_ctx->get_physical_plan_ctx())
      && OB_NOT_NULL(plan = plan_ctx->get_phy_plan())) {
    owns_desc = session->get_tx_desc() == tx_desc;
    in_tx = session->is_in_transaction();
    plain_select = plan->is_plain_select();
    eligible = owns_desc && !in_tx && plain_select;
  }
  if (replay_trace()) {
    LOG_INFO("fts replay eligibility", K(eligible), "owns_desc", owns_desc,
             "in_tx", in_tx, "plain_select", plain_select,
             "has_session", nullptr != session, "has_plan", nullptr != plan);
  }
  return eligible;
}

} // namespace

ObDASLegacyTRMergeIter::ObDASLegacyTRMergeIter()
  : ObDASIter(ObDASIterType::DAS_ITER_TEXT_RETRIEVAL_MERGE),
    mem_context_(nullptr),
    myself_allocator_(lib::ObMemAttr("SRMergeIterSelf"), OB_MALLOC_NORMAL_BLOCK_SIZE),
    ir_ctdef_(nullptr),
    ir_rtdef_(nullptr),
    tx_desc_(nullptr),
    snapshot_(nullptr),
    query_tokens_(),
    dim_weights_(),
    sr_iter_param_(),
    sparse_retrieval_iter_(nullptr),
    dim_iters_(),
    dim_iter_(),
    total_doc_cnt_scan_param_(nullptr),
    inv_scan_params_(),
    inv_agg_params_(),
    boolean_compute_node_(nullptr),
    block_max_scan_params_(),
    block_max_iter_param_(),
    doc_length_est_param_(),
    doc_length_est_stat_cols_(),
    topk_limit_(0),
    total_doc_cnt_tablet_id_(),
    inv_idx_tablet_id_(),
    flags_(0),
    check_rangekey_inited_(false),
    inv_idx_tablet_switched_(false),
    is_inited_(false),
    replay_entry_(),
    collecting_entry_(),
    replay_pos_(0),
    replay_active_(false)
{
}

int ObDASLegacyTRMergeIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(ObDASIterType::DAS_ITER_TEXT_RETRIEVAL_MERGE != param.type_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObDASTRMergeIterParam &merge_param = static_cast<ObDASTRMergeIterParam &>(param);
    ir_ctdef_ = merge_param.ir_ctdef_;
    ir_rtdef_ = merge_param.ir_rtdef_;
    tx_desc_ = merge_param.tx_desc_;
    snapshot_ = merge_param.snapshot_;
    boolean_compute_node_ = merge_param.boolean_compute_node_;
    flags_ = merge_param.flags_;
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_param.query_tokens_.count(); ++i) {
      if (OB_FAIL(query_tokens_.push_back(merge_param.query_tokens_.at(i)))) {
      }
    }
    if (merge_param.dim_weights_.count() >= 1) {
      for (int64_t i = 0; OB_SUCC(ret) && i < merge_param.dim_weights_.count(); ++i) {
        if (OB_FAIL(dim_weights_.push_back(merge_param.dim_weights_.at(i)))) {
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ir_ctdef_) || OB_ISNULL(ir_rtdef_)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (topk_mode_ && OB_FAIL(init_topk_limit())) {
    } else if (OB_ISNULL(mem_context_)) {
      lib::ContextParam mem_param;
      mem_param.set_mem_attr("TextMergeIter", ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, mem_param))) {
      }
    }
    sr_iter_param_.max_batch_size_ = merge_param.max_batch_size_; // may be greater than ir_rtdef_->eval_ctx_->max_batch_size_
    inv_idx_tablet_switched_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObDASLegacyTRMergeIter::init_das_iter_scan_params()
{
  int ret = OB_SUCCESS;
  const int64_t dim_iter_cnt = taat_mode_ ? 1 : query_tokens_.count();
  if (!ir_ctdef_->need_estimate_total_doc_cnt() && 0 != dim_iter_cnt) {
    void *buf = nullptr;
    if (OB_ISNULL(buf = myself_allocator_.alloc(sizeof(ObTableScanParam)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      total_doc_cnt_scan_param_ = new (buf) ObTableScanParam();
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_das_iter_scan_param(
        total_doc_cnt_tablet_id_,
        ir_ctdef_->get_doc_agg_ctdef(),
        ir_rtdef_->get_doc_agg_rtdef(),
        tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
        *total_doc_cnt_scan_param_))) {
    } else {
      static_cast<ObDASScanIter*>(children_[children_cnt_ - 1])->set_scan_param(*total_doc_cnt_scan_param_);
    }
  }

  if (OB_SUCC(ret) && 0 != dim_iter_cnt) {
    if (FALSE_IT(inv_scan_params_.set_allocator(&myself_allocator_))) {
    } else if (OB_FAIL(inv_scan_params_.init(dim_iter_cnt))) {
    } else if (OB_FAIL(inv_scan_params_.prepare_allocate(dim_iter_cnt))) {
    }

    if (OB_FAIL(ret) || !ir_ctdef_->need_inv_idx_agg()) {
    } else if (FALSE_IT(inv_agg_params_.set_allocator(&myself_allocator_))) {
    } else if (OB_FAIL(inv_agg_params_.init(dim_iter_cnt))) {
    } else if (OB_FAIL(inv_agg_params_.prepare_allocate(dim_iter_cnt))) {
    }

    if (OB_SUCC(ret)) {
      void *inv_buf = nullptr;
      if (OB_ISNULL(inv_buf = myself_allocator_.alloc(sizeof(ObTableScanParam) * dim_iter_cnt))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObTableScanParam *scan_params = static_cast<ObTableScanParam*>(inv_buf);
        for (int64_t i = 0; OB_SUCC(ret) && i < dim_iter_cnt; ++i) {
          inv_scan_params_[i] = new (&scan_params[i]) ObTableScanParam();
          if (OB_FAIL(init_das_iter_scan_param(
              inv_idx_tablet_id_,
              ir_ctdef_->get_inv_idx_scan_ctdef(), ir_rtdef_->get_inv_idx_scan_rtdef(),
              tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
              *inv_scan_params_[i]))) {
          } else {
            static_cast<ObDASScanIter*>(children_[i])->set_scan_param(*inv_scan_params_[i]);
          }
        }
      }

      if (OB_SUCC(ret) && ir_ctdef_->need_inv_idx_agg()) {
        void *agg_buf = nullptr;
        if (OB_ISNULL(agg_buf = myself_allocator_.alloc(sizeof(ObTableScanParam) * dim_iter_cnt))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          ObTableScanParam *agg_params = static_cast<ObTableScanParam*>(agg_buf);
          for (int64_t i = 0; OB_SUCC(ret) && i < dim_iter_cnt; ++i) {
            inv_agg_params_[i] = new (&agg_params[i]) ObTableScanParam();
            if (OB_FAIL(init_das_iter_scan_param(
                inv_idx_tablet_id_,
                ir_ctdef_->get_inv_idx_agg_ctdef(),
                ir_rtdef_->get_inv_idx_agg_rtdef(),
                tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
                *inv_agg_params_[i]))) {
            } else {
              static_cast<ObDASScanIter*>(children_[i + dim_iter_cnt])->set_scan_param(*inv_agg_params_[i]);
            }
            inv_scan_params_[i]->scan_flag_.scan_order_ = ObQueryFlag::Forward;
          }
        }
      }

    }
  }
  return ret;
}

int ObDASLegacyTRMergeIter::init_block_max_iter_param()
{
  int ret = OB_SUCCESS;
  const int64_t token_cnt = query_tokens_.count();
  block_max_scan_params_.set_allocator(&myself_allocator_);
  if (0 == token_cnt) {
    // do nothing
  } else if (OB_FAIL(block_max_iter_param_.init(*ir_ctdef_, myself_allocator_))) {
  } else if (OB_FAIL(block_max_scan_params_.init(token_cnt))) {
  } else if (OB_FAIL(block_max_scan_params_.prepare_allocate(token_cnt))) {
  } else {
    ObTableScanParam *scan_params = nullptr;
    if (OB_ISNULL(scan_params = static_cast<ObTableScanParam *>(myself_allocator_.alloc(sizeof(ObTableScanParam) * token_cnt)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < token_cnt; ++i) {
      block_max_scan_params_[i] = new (&scan_params[i]) ObTableScanParam();
      if (OB_FAIL(init_das_iter_scan_param(
          inv_idx_tablet_id_,
          ir_ctdef_->get_block_max_scan_ctdef(),
          ir_rtdef_->get_block_max_scan_rtdef(),
          tx_desc_, snapshot_, mem_context_->get_arena_allocator(),
          *block_max_scan_params_[i]))) {
      }
    }
  }
  return ret;
}

int ObDASLegacyTRMergeIter::init_doc_length_est_param()
{
  int ret = OB_SUCCESS;
  const ObTextAvgDocLenEstSpec &doc_len_est_spec = ir_ctdef_->avg_doc_len_est_spec_;
  doc_length_est_stat_cols_.set_allocator(&myself_allocator_);
  if (!ir_ctdef_->need_avg_doc_len_est() || !doc_len_est_spec.can_est_by_sum_skip_index_) {
    // skip
  } else if (OB_UNLIKELY(!doc_len_est_spec.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(doc_length_est_stat_cols_.init(doc_len_est_spec.col_types_.count()))) {
  } else if (OB_FAIL(doc_length_est_stat_cols_.push_back(
      ObSkipIndexColMeta(doc_len_est_spec.col_store_idxes_.at(0), doc_len_est_spec.col_types_.at(0))))) {
  } else if (OB_FAIL(doc_length_est_param_.init(
      doc_length_est_stat_cols_,
      doc_len_est_spec.scan_col_proj_,
      *inv_scan_params_[0], // TODO: use independent param with whole scan range
      true,
      true,
      true))) {
  }
  return ret;
}

int ObDASLegacyTRMergeIter::init_das_iter_scan_param(const ObTabletID &tablet_id,
                                               const ObDASScanCtDef *ctdef,
                                               ObDASScanRtDef *rtdef,
                                               transaction::ObTxDesc *tx_desc,
                                               transaction::ObTxReadSnapshot *snapshot,
                                               common::ObArenaAllocator &allocator,
                                               ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_INVALID_ARGUMENT;
  } else {


    scan_param.key_ranges_.set_attr(ObMemAttr("ScanParamKR"));
    scan_param.tx_lock_timeout_ = rtdef->tx_lock_timeout_;
    scan_param.index_id_ = ctdef->ref_table_id_;
    scan_param.is_get_ = false; // scan
    scan_param.is_for_foreign_check_ = false;
    scan_param.timeout_ = rtdef->timeout_ts_;
    scan_param.scan_flag_ = rtdef->scan_flag_;
    scan_param.reserved_cell_count_ = ctdef->access_column_ids_.count();
    scan_param.allocator_ = &rtdef->stmt_allocator_;
    scan_param.scan_allocator_ = &allocator;
    scan_param.sql_mode_ = rtdef->sql_mode_;
    scan_param.frozen_version_ = rtdef->frozen_version_;
    scan_param.output_exprs_ = &(ctdef->pd_expr_spec_.access_exprs_);
    scan_param.calc_exprs_ = &(ctdef->pd_expr_spec_.calc_exprs_);
    scan_param.aggregate_exprs_ = &(ctdef->pd_expr_spec_.pd_storage_aggregate_output_);
    scan_param.table_param_ = &(ctdef->table_param_);
    scan_param.op_ = rtdef->p_pd_expr_op_;
    scan_param.row2exprs_projector_ = rtdef->p_row2exprs_projector_;
    scan_param.schema_version_ = ctdef->schema_version_;
    scan_param.runtime_schema_version_ = rtdef->runtime_schema_version_;
    scan_param.limit_param_ = rtdef->limit_param_;
    scan_param.need_scn_ = rtdef->need_scn_;
    scan_param.pd_storage_flag_ = ctdef->pd_expr_spec_.pd_storage_flag_.pd_flag_;
    scan_param.fb_snapshot_ = rtdef->fb_snapshot_;
    scan_param.tablet_id_ = tablet_id;
    if (!ctdef->pd_expr_spec_.pushdown_filters_.empty()) {
      scan_param.op_filters_ = &ctdef->pd_expr_spec_.pushdown_filters_;
    }
    scan_param.pd_storage_filters_ = rtdef->p_pd_expr_op_->pd_storage_filters_;
    if (OB_NOT_NULL(tx_desc)) {
      scan_param.tx_id_ = tx_desc->get_tx_id();
    } else {
      scan_param.tx_id_.reset();
    }

    if (OB_NOT_NULL(snapshot)) {
      if (OB_FAIL(scan_param.snapshot_.assign(*snapshot))) {
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null snapshot", K(ret), KP(snapshot));
    }

    if (FAILEDx(scan_param.column_ids_.assign(ctdef->access_column_ids_))) {
    }
  }
  return ret;
}

int ObDASLegacyTRMergeIter::create_dim_iters()
{
  int ret = OB_SUCCESS;
  if (topk_mode_) {
    if (OB_FAIL(init_block_max_iter_param())) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < query_tokens_.count(); ++i) {
      ObTextRetrievalScanIterParam iter_param;
      ObTextRetrievalBlockMaxIter *dim_iter = nullptr;
      if (OB_FAIL(init_dim_iter_param(iter_param, i))) {
      } else if (OB_ISNULL(dim_iter = OB_NEWx(ObTextRetrievalBlockMaxIter, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(dim_iter->init(iter_param, block_max_iter_param_, *block_max_scan_params_[i]))) {
      } else if (OB_FAIL(dim_iters_.push_back(dim_iter))) {
      }
    }
  } else if (daat_mode_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < query_tokens_.count(); ++i) {
      ObTextRetrievalScanIterParam iter_param;
      ObTextRetrievalDaaTTokenIter *dim_iter = nullptr;
      if (OB_FAIL(init_dim_iter_param(iter_param, i))) {
      } else if (OB_ISNULL(dim_iter = OB_NEWx(ObTextRetrievalDaaTTokenIter, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(dim_iter->init(iter_param))) {
      } else if (OB_FAIL(dim_iters_.push_back(dim_iter))) {
      }
    }
  } else if (taat_mode_) {
    ObTextRetrievalScanIterParam iter_param;
    ObTextRetrievalTokenIter *dim_iter = nullptr;
    if (OB_FAIL(init_dim_iter_param(iter_param, 0))) {
    } else if (OB_ISNULL(dim_iter = OB_NEWx(ObTextRetrievalTokenIter, &myself_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(dim_iter->init(iter_param))) {
    } else {
      dim_iter_ = dim_iter;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObDASLegacyTRMergeIter::init_dim_iter_param(ObTextRetrievalScanIterParam &iter_param, const int64_t &idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ir_ctdef_) || OB_ISNULL(ir_rtdef_) || OB_ISNULL(ir_rtdef_->eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const int64_t dim_iter_cnt = taat_mode_ ? 1 : query_tokens_.count();
    iter_param.inv_idx_scan_param_ = inv_scan_params_[idx];
    iter_param.inv_idx_scan_iter_ = static_cast<ObDASScanIter*>(children_[idx]);
    if (ir_ctdef_->need_inv_idx_agg()) {
      iter_param.inv_idx_agg_param_ = inv_agg_params_[idx];
      iter_param.inv_idx_agg_iter_ = static_cast<ObDASScanIter*>(children_[idx + dim_iter_cnt]);
      if (OB_UNLIKELY(1 != ir_ctdef_->get_inv_idx_agg_ctdef()
          ->pd_expr_spec_.pd_storage_aggregate_output_.count())) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        iter_param.inv_idx_agg_expr_ = ir_ctdef_->get_inv_idx_agg_ctdef()
            ->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
      }
    }
    iter_param.allocator_ = &myself_allocator_;
    iter_param.mem_context_ = mem_context_;
    iter_param.eval_ctx_ = ir_rtdef_->eval_ctx_;
    iter_param.relevance_expr_ = ir_ctdef_->relevance_expr_;
    iter_param.inv_scan_doc_length_col_ = ir_ctdef_->inv_scan_doc_length_col_;
    iter_param.inv_scan_domain_id_col_ = ir_ctdef_->inv_scan_domain_id_col_;
    iter_param.inv_idx_agg_cache_mode_ = function_lookup_mode_ && !taat_mode_;
  }
  return ret;
}

int ObDASLegacyTRMergeIter::create_sparse_retrieval_iter()
{
  int ret = OB_SUCCESS;
  sr_iter_param_.dim_weights_ = dim_weights_.count() >= 1 ? &dim_weights_ : nullptr;
  sr_iter_param_.limit_param_ = &ir_rtdef_->get_inv_idx_scan_rtdef()->limit_param_;
  sr_iter_param_.eval_ctx_ = ir_rtdef_->eval_ctx_;
  sr_iter_param_.id_proj_expr_ = ir_ctdef_->inv_scan_domain_id_col_;
  sr_iter_param_.relevance_expr_ = ir_ctdef_->relevance_expr_;
  sr_iter_param_.relevance_proj_expr_ = ir_ctdef_->relevance_proj_col_;
  // In natural language mode the retrieval merge emits exactly the documents that contain at
  // least one query token, which is what the pushed-down match predicate checks.  When nothing
  // reads the exact score, re-evaluating that predicate on every emitted row can never reject
  // one of them, so the per-row evaluation is skipped.  Other modes (boolean, phrase, query
  // expansion) keep the predicate because their emitted set is not the natural-language union.
  const bool need_match_filter = ir_ctdef_->need_calc_relevance()
      || NATURAL_LANGUAGE_MODE != ir_ctdef_->mode_flag_;
  sr_iter_param_.filter_expr_ = need_match_filter ? ir_ctdef_->match_filter_ : nullptr;
  sr_iter_param_.topk_limit_ = topk_limit_;
  if (OB_NOT_NULL(ir_ctdef_->field_boost_expr_)) {
    ObDatum *boost_datum = nullptr;
    if (OB_FAIL(ir_ctdef_->field_boost_expr_->eval(*ir_rtdef_->eval_ctx_, boost_datum))) {
    } else if (OB_ISNULL(boost_datum) || OB_UNLIKELY(boost_datum->is_null())) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "null field boost is");
    } else {
      sr_iter_param_.field_boost_ = boost_datum->get_double();
      if (sr_iter_param_.field_boost_ <= 0.0) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "field boost < 0 is");
      } else if (query_tokens_.count() == 0) {
        // do nothing
      } else if (OB_ISNULL(sr_iter_param_.dim_weights_)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < dim_weights_.count(); ++i) {
          if (sr_iter_param_.dim_weights_->at(i) <= 0.0) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "token weight < 0 is");
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (topk_mode_) {
    ObTextDaaTParam iter_param;
    ObTextBMWIter *bmw_iter = nullptr;
    if (OB_FAIL(init_daat_iter_param(iter_param))) {
    } else if (OB_ISNULL(bmw_iter = OB_NEWx(ObTextBMWIter, &myself_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(bmw_iter->init(iter_param))) {
    } else {
      sparse_retrieval_iter_ = bmw_iter;
    }
  } else if (daat_mode_) {
    ObTextDaaTParam iter_param;
    ObTextDaaTIter *daat_iter = nullptr;
    if (OB_FAIL(init_daat_iter_param(iter_param))) {
    } else if (OB_ISNULL(daat_iter = OB_NEWx(ObTextDaaTIter, &myself_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(daat_iter->init(iter_param))) {
    } else {
      sparse_retrieval_iter_ = daat_iter;
    }
  } else if (taat_mode_) {
    ObTextTaaTParam iter_param;
    ObTextTaaTIter *taat_iter = nullptr;
    if (OB_FAIL(init_taat_iter_param(iter_param))) {
    } else if (OB_ISNULL(taat_iter = OB_NEWx(ObTextTaaTIter, &myself_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(taat_iter->init(iter_param))) {
    } else {
      sparse_retrieval_iter_ = taat_iter;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  if (OB_SUCC(ret) && function_lookup_mode_) {
    ObSRLookupIter *lookup_iter = nullptr;
    if (OB_UNLIKELY(topk_mode_)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (daat_mode_) {
      if (OB_ISNULL(lookup_iter = OB_NEWx(ObSRSortedLookupIter, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    } else if (taat_mode_) {
      if (OB_ISNULL(lookup_iter = OB_NEWx(ObSRHashLookupIter, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
    if (FAILEDx(lookup_iter->init(sr_iter_param_, *sparse_retrieval_iter_,
                                  myself_allocator_, sr_iter_param_.max_batch_size_))) {
    } else {
      sparse_retrieval_iter_ = lookup_iter;
    }
  }
  return ret;
}

int ObDASLegacyTRMergeIter::init_daat_iter_param(ObTextDaaTParam &iter_param)
{
  int ret = OB_SUCCESS;
  iter_param.dim_iters_ = &dim_iters_;
  iter_param.base_param_ = &sr_iter_param_;
  iter_param.allocator_ = &myself_allocator_;
  iter_param.mode_flag_ = ir_ctdef_->mode_flag_;
  iter_param.function_lookup_mode_ = function_lookup_mode_;
  iter_param.bm25_param_est_ctx_.total_doc_cnt_expr_
      = ir_ctdef_->get_doc_agg_ctdef()->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
  iter_param.bm25_param_est_ctx_.estimated_total_doc_cnt_ = ir_ctdef_->estimated_total_doc_cnt_;
  iter_param.bm25_param_est_ctx_.need_est_avg_doc_token_cnt_ = ir_ctdef_->need_avg_doc_len_est();
  iter_param.bm25_param_est_ctx_.can_est_by_sum_skip_index_ = ir_ctdef_->avg_doc_len_est_spec_.can_est_by_sum_skip_index_;
  iter_param.bm25_param_est_ctx_.avg_doc_token_cnt_expr_ = ir_ctdef_->avg_doc_token_cnt_expr_;
  iter_param.bm25_param_est_ctx_.doc_length_est_param_ = &doc_length_est_param_;
  if (query_tokens_.count() == 0) {
    // do nothing
  } else if (OB_ISNULL(iter_param.bm25_param_est_ctx_.total_doc_cnt_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(init_doc_length_est_param())) {
  } else if (!ir_ctdef_->need_estimate_total_doc_cnt()) {
    if (OB_UNLIKELY(!static_cast<sql::ObStoragePushdownFlag>(total_doc_cnt_scan_param_->pd_storage_flag_).is_aggregate_pushdown())) {
      ret = OB_NOT_IMPLEMENT;
      LOG_ERROR("aggregate without pushdown not implemented", K(ret));
    } else {
      iter_param.bm25_param_est_ctx_.total_doc_cnt_iter_ = static_cast<sql::ObDASScanIter*>(children_[children_cnt_ - 1]);
    }
  }
  if (OB_SUCC(ret)) {
    if (BOOLEAN_MODE == ir_ctdef_->mode_flag_) {
      ObSRDaaTBooleanRelevanceCollector *boolean_relevance_collector = nullptr;
      if (OB_ISNULL(boolean_relevance_collector = OB_NEWx(ObSRDaaTBooleanRelevanceCollector, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(boolean_relevance_collector->init(&myself_allocator_, dim_iters_.count(), boolean_compute_node_))) {
      } else {
        iter_param.relevance_collector_ = boolean_relevance_collector;
      }
    } else {
      int64_t should_match = ir_rtdef_->minimum_should_match_;
      ObSRDaaTInnerProductRelevanceCollector *inner_product_relevance_collector = nullptr;
      if (OB_ISNULL(inner_product_relevance_collector = OB_NEWx(ObSRDaaTInnerProductRelevanceCollector, &myself_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(inner_product_relevance_collector->init(should_match))) {
      } else {
        iter_param.relevance_collector_ = inner_product_relevance_collector;
      }
    }
  }
  return ret;
}

int ObDASLegacyTRMergeIter::init_taat_iter_param(ObTextTaaTParam &iter_param)
{
  int ret = OB_SUCCESS;
  iter_param.query_tokens_ = &query_tokens_;
  iter_param.base_param_ = &sr_iter_param_;
  iter_param.allocator_ = &myself_allocator_;
  iter_param.mode_flag_ = ir_ctdef_->mode_flag_;
  iter_param.function_lookup_mode_ = function_lookup_mode_;
  iter_param.bm25_param_est_ctx_.total_doc_cnt_expr_
      = ir_ctdef_->get_doc_agg_ctdef()->pd_expr_spec_.pd_storage_aggregate_output_.at(0);
  iter_param.bm25_param_est_ctx_.estimated_total_doc_cnt_ = ir_ctdef_->estimated_total_doc_cnt_;
  iter_param.bm25_param_est_ctx_.need_est_avg_doc_token_cnt_ = ir_ctdef_->need_avg_doc_len_est();
  iter_param.bm25_param_est_ctx_.can_est_by_sum_skip_index_ = ir_ctdef_->avg_doc_len_est_spec_.can_est_by_sum_skip_index_;
  iter_param.bm25_param_est_ctx_.avg_doc_token_cnt_expr_ = ir_ctdef_->avg_doc_token_cnt_expr_;
  iter_param.bm25_param_est_ctx_.doc_length_est_param_ = &doc_length_est_param_;
  if (OB_ISNULL(iter_param.dim_iter_ = dim_iter_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(iter_param.bm25_param_est_ctx_.total_doc_cnt_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(init_doc_length_est_param())) {
  } else if (!ir_ctdef_->need_estimate_total_doc_cnt()) {
    if (OB_UNLIKELY(!static_cast<sql::ObStoragePushdownFlag>(total_doc_cnt_scan_param_->pd_storage_flag_).is_aggregate_pushdown())) {
      ret = OB_NOT_IMPLEMENT;
      LOG_ERROR("aggregate without pushdown not implemented", K(ret));
    } else {
      iter_param.bm25_param_est_ctx_.total_doc_cnt_iter_ = static_cast<sql::ObDASScanIter*>(children_[children_cnt_ - 1]);
    }
  }
  return ret;
}

int ObDASLegacyTRMergeIter::set_children_iter_rangekey()
{
  int ret = OB_SUCCESS;
  check_rangekey_inited_ = true;
  if (OB_UNLIKELY(function_lookup_mode_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(0 == query_tokens_.count())) {
    // do nothing
  } else if (OB_UNLIKELY(inv_scan_params_.empty()
      || (ir_ctdef_->need_inv_idx_agg() && inv_agg_params_.empty()))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const ExprFixedArray *exprs = &(ir_ctdef_->get_inv_idx_scan_ctdef()->pd_expr_spec_.access_exprs_);
    int64 group_id = 0;
    for (int64_t i = 0; i < exprs->count(); ++i) {
      if (T_PSEUDO_GROUP_ID == exprs->at(i)->type_) {
        group_id = exprs->at(i)->locate_expr_datum(*eval_ctx_).get_int();
      }
    }
    int64_t group_idx = ObNewRange::get_group_idx(group_id);
    ObNewRange inv_idx_scan_range;
    const int64_t dim_iter_cnt = taat_mode_ ? 1 : query_tokens_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < dim_iter_cnt; ++i) {
      if (OB_FAIL(gen_inv_idx_scan_default_range(query_tokens_[i], inv_idx_scan_range))) {
      } else if (ir_ctdef_->need_inv_idx_agg()
          && OB_FAIL(inv_agg_params_[i]->key_ranges_.push_back(inv_idx_scan_range))) {
      } else if (FALSE_IT(inv_idx_scan_range.group_idx_ = group_idx)) {
      } else if (OB_FAIL(inv_scan_params_[i]->key_ranges_.push_back(inv_idx_scan_range))) {
      } else if (topk_mode_ && ir_ctdef_->need_block_max_scan()
          && OB_FAIL(block_max_scan_params_[i]->key_ranges_.push_back(inv_idx_scan_range))) {
      }
    }
  }
  return ret;
}

int ObDASLegacyTRMergeIter::set_children_iter_rangekey(const common::ObIArray<std::pair<ObDocIdExt, int>> &virtual_rangekeys, const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  check_rangekey_inited_ = true;
  if (OB_UNLIKELY(!function_lookup_mode_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (nullptr == sparse_retrieval_iter_) {
    if (OB_FAIL(init_das_iter_scan_params())) {
    } else if (OB_FAIL(create_dim_iters())) {
    } else if (OB_FAIL(create_sparse_retrieval_iter())) {
    }
  }
  if (OB_FAIL(ret) || 0 == query_tokens_.count()) {
  } else if (OB_UNLIKELY(inv_scan_params_.empty()
      || (ir_ctdef_->need_inv_idx_agg() && inv_agg_params_.empty()))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const ExprFixedArray *exprs = &(ir_ctdef_->get_inv_idx_scan_ctdef()->pd_expr_spec_.access_exprs_);
    int64 group_id = 0;
    for (int64_t i = 0; i < exprs->count(); ++i) {
      if (T_PSEUDO_GROUP_ID == exprs->at(i)->type_) {
        group_id = exprs->at(i)->locate_expr_datum(*eval_ctx_).get_int();
      }
    }
    int64_t group_idx = ObNewRange::get_group_idx(group_id);
    ObNewRange inv_idx_scan_range;
    const int64_t dim_iter_cnt = taat_mode_ ? 1 : query_tokens_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < dim_iter_cnt; ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
        if (OB_FAIL(gen_inv_idx_scan_one_range(query_tokens_[i],
                                               virtual_rangekeys.at(j).first,
                                               inv_idx_scan_range))) {
        } else if (FALSE_IT(inv_idx_scan_range.group_idx_ = group_idx)) {
        } else if (OB_FAIL(inv_scan_params_[i]->key_ranges_.push_back(inv_idx_scan_range))) {
        }
      }

      if (!ir_ctdef_->need_inv_idx_agg()) {
      } else if (OB_FAIL(gen_inv_idx_scan_default_range(query_tokens_[i], inv_idx_scan_range))) {
      } else if (OB_FAIL(inv_agg_params_[i]->key_ranges_.push_back(inv_idx_scan_range))) {
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(static_cast<ObSRLookupIter *>(sparse_retrieval_iter_)
      ->set_hints(virtual_rangekeys, batch_size))) {
  }
  return ret;
}

int ObDASLegacyTRMergeIter::gen_inv_idx_scan_default_range(const ObString &query_token, ObNewRange &scan_range)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObObj *obj_ptr = nullptr;
  common::ObArenaAllocator &ctx_alloc = mem_context_->get_arena_allocator();
  constexpr int64_t obj_cnt = INV_IDX_ROWKEY_COL_CNT * 2;
  ObObj tmp_obj;
  tmp_obj.set_string(ObVarcharType, query_token);
  // We need to ensure collation type / level between query text and token column is compatible
  tmp_obj.set_meta_type(ir_ctdef_->search_text_->obj_meta_);

  if (OB_ISNULL(buf = ctx_alloc.alloc(sizeof(ObObj) * obj_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(obj_ptr = new (buf) ObObj[obj_cnt])) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ob_write_obj(ctx_alloc, tmp_obj, obj_ptr[0]))) {
  } else if (OB_FAIL(ob_write_obj(ctx_alloc, tmp_obj, obj_ptr[2]))) {
  } else {
    obj_ptr[1].set_min_value();
    obj_ptr[3].set_max_value();
    ObRowkey start_key(obj_ptr, INV_IDX_ROWKEY_COL_CNT);
    ObRowkey end_key(&obj_ptr[2], INV_IDX_ROWKEY_COL_CNT);
    common::ObTableID inv_table_id = ir_ctdef_->get_inv_idx_scan_ctdef()->ref_table_id_;
    scan_range.table_id_ = inv_table_id;
    scan_range.start_key_.assign(obj_ptr, INV_IDX_ROWKEY_COL_CNT);
    scan_range.end_key_.assign(&obj_ptr[2], INV_IDX_ROWKEY_COL_CNT);
    scan_range.border_flag_.set_inclusive_start();
    scan_range.border_flag_.set_inclusive_end();
  }
  return ret;
}

int ObDASLegacyTRMergeIter::gen_inv_idx_scan_one_range(const ObString &query_token, const ObDocIdExt &doc_id, ObNewRange &scan_range)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObObj *obj_ptr = nullptr;
  common::ObArenaAllocator &ctx_alloc = mem_context_->get_arena_allocator();
  constexpr int64_t obj_cnt = INV_IDX_ROWKEY_COL_CNT;
  ObObj tmp_obj;
  tmp_obj.set_string(ObVarcharType, query_token);
  // We need to ensure collation type / level between query text and token column is compatible
  tmp_obj.set_meta_type(ir_ctdef_->search_text_->obj_meta_);

  if (OB_ISNULL(buf = ctx_alloc.alloc(sizeof(ObObj) * obj_cnt))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(obj_ptr = new (buf) ObObj[obj_cnt])) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ob_write_obj(ctx_alloc, tmp_obj, obj_ptr[0]))) {
  } else if (OB_FAIL(doc_id.get_datum().to_obj(obj_ptr[1], ir_ctdef_->inv_scan_domain_id_col_->obj_meta_))) {
  } else {
    ObRowkey row_key(obj_ptr, obj_cnt);
    common::ObTableID inv_table_id = ir_ctdef_->get_inv_idx_scan_ctdef()->ref_table_id_;
    if (OB_FAIL(scan_range.build_range(inv_table_id, row_key))) {
    }
  }
  return ret;
}

int ObDASLegacyTRMergeIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    bool replayed = false;
    // Only a scan without physical state may be served from the replay cache.
    // Once the scan has been set up, later reuses keep using that state so the
    // two paths never mix.
    if (OB_ISNULL(sparse_retrieval_iter_) && !function_lookup_mode_
        && OB_FAIL(try_start_replay(replayed))) {
      LOG_WARN("failed to probe the text retrieval replay cache", K(ret));
      replayed = false;
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret) && !replayed) {
      reset_replay_state();
      if (function_lookup_mode_) {
        // skip the following, which should have been done
      } else if (OB_FAIL(init_das_iter_scan_params())) {
      } else if (OB_FAIL(create_dim_iters())) {
      } else if (OB_FAIL(create_sparse_retrieval_iter())) {
      } else if (OB_FAIL(set_children_iter_rangekey())) {
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(!check_rangekey_inited_)) {
        ret = OB_ERR_UNEXPECTED;
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
        if (OB_FAIL(children_[i]->do_table_scan())) {
        }
      }
      if (OB_SUCC(ret) && !function_lookup_mode_ && OB_FAIL(prepare_replay_collection())) {
        // A result that cannot be collected only loses the cache; the scan
        // itself has already been set up successfully.
        LOG_WARN("failed to prepare text retrieval replay collection", K(ret));
        collecting_entry_.reset();
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObDASLegacyTRMergeIter::reuse_das_iter_scan_param(const ObTabletID &tablet_id,
                                                ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  const ObTabletID &old_tablet_id = scan_param.tablet_id_;
  scan_param.need_switch_param_ = scan_param.need_switch_param_
      || (old_tablet_id.is_valid() && old_tablet_id != tablet_id);
  if (!scan_param.key_ranges_.empty()) {
    scan_param.key_ranges_.reuse();
  }
  scan_param.tablet_id_ = tablet_id;
  return ret;
}

int ObDASLegacyTRMergeIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (replay_active_ || OB_ISNULL(sparse_retrieval_iter_)) {
    // A replayed scan holds no physical scan state, so there is nothing to
    // reuse.  The next do_table_scan replays again or sets the scan up from
    // scratch.
    reset_replay_state();
    check_rangekey_inited_ = false;
    if (OB_NOT_NULL(mem_context_)) {
      mem_context_->reset_remain_one_page();
    }
  } else {
    // A collection that did not reach the end of the scan describes only a
    // prefix of the result and must not be published.
    collecting_entry_.reset();
    if (query_tokens_.count() > 0) {
      if (OB_NOT_NULL(total_doc_cnt_scan_param_)) {
        if (OB_FAIL(reuse_das_iter_scan_param(total_doc_cnt_tablet_id_, *total_doc_cnt_scan_param_))) {
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < inv_scan_params_.count(); ++i) {
        if (OB_FAIL(reuse_das_iter_scan_param(inv_idx_tablet_id_, *inv_scan_params_[i]))) {
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < inv_agg_params_.count(); ++i) {
        if (OB_FAIL(reuse_das_iter_scan_param(inv_idx_tablet_id_, *inv_agg_params_[i]))) {
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < block_max_scan_params_.count(); ++i) {
        if (OB_FAIL(reuse_das_iter_scan_param(inv_idx_tablet_id_, *block_max_scan_params_[i]))) {
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
        if (OB_FAIL(children_[i]->reuse())) {
        }
      }
    }

    if (OB_SUCC(ret)) {
      sparse_retrieval_iter_->reuse(inv_idx_tablet_switched_);
      inv_idx_tablet_switched_ = false;
    }

    if (OB_NOT_NULL(mem_context_)) {
      mem_context_->reset_remain_one_page();
    }
    check_rangekey_inited_ = false;
  }
  return ret;
}

int ObDASLegacyTRMergeIter::rescan()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (replay_active_ || OB_ISNULL(sparse_retrieval_iter_)) {
    // There is no physical scan state to rescan; run the scan from the start
    // (do_table_scan revalidates the replay cache before using it).
    ret = do_table_scan();
  } else if (!function_lookup_mode_ && OB_FAIL(set_children_iter_rangekey())) {
  } else if (OB_UNLIKELY(!check_rangekey_inited_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
      // TODO: update scan param
      if (OB_FAIL(children_[i]->rescan())) {
      }
    }
  }
  return ret;
}

int ObDASLegacyTRMergeIter::inner_release()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(total_doc_cnt_scan_param_)) {
    total_doc_cnt_scan_param_->destroy_schema_guard();
    total_doc_cnt_scan_param_->snapshot_.reset();
    total_doc_cnt_scan_param_->destroy();
    total_doc_cnt_scan_param_ = nullptr;
  }

  for (int64_t i = 0; i < inv_scan_params_.count(); ++i) {
    if (OB_NOT_NULL(inv_scan_params_[i])) {
      inv_scan_params_[i]->destroy_schema_guard();
      inv_scan_params_[i]->snapshot_.reset();
      inv_scan_params_[i]->destroy();
    }
  }
  inv_scan_params_.reset();

  for (int64_t i = 0; i < inv_agg_params_.count(); ++i) {
    if (OB_NOT_NULL(inv_agg_params_[i])) {
      inv_agg_params_[i]->destroy_schema_guard();
      inv_agg_params_[i]->snapshot_.reset();
      inv_agg_params_[i]->destroy();
    }
  }
  inv_agg_params_.reset();

  for (int64_t i = 0; i < block_max_scan_params_.count(); ++i) {
    if (OB_NOT_NULL(block_max_scan_params_[i])) {
      block_max_scan_params_[i]->destroy_schema_guard();
      block_max_scan_params_[i]->snapshot_.reset();
      block_max_scan_params_[i]->destroy();
    }
  }
  block_max_scan_params_.reset();

  if (OB_NOT_NULL(boolean_compute_node_)) {
    boolean_compute_node_->release();
    boolean_compute_node_ = nullptr;
  }

  if (OB_NOT_NULL(sparse_retrieval_iter_)) {
    sparse_retrieval_iter_->reset();
    sparse_retrieval_iter_ = nullptr;
  }
  dim_iters_.reset();
  dim_iter_ = nullptr;
  if (nullptr != mem_context_) {
    mem_context_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
  query_tokens_.reset();
  dim_weights_.reset();
  myself_allocator_.reset();
  reset_replay_state();
  ir_ctdef_ = nullptr;
  ir_rtdef_ = nullptr;
  tx_desc_ = nullptr;
  snapshot_ = nullptr;
  topk_limit_ = 0;
  inv_idx_tablet_switched_ = false;
  is_inited_ = false;
  return ret;
}

int ObDASLegacyTRMergeIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (replay_active_) {
    ret = replay_get_next_row();
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = sparse_retrieval_iter_->get_next_row();
    if (OB_SUCC(ret)) {
      collect_replay_rows(1);
    } else if (OB_ITER_END == ret) {
      finish_replay_collection();
    } else {
      collecting_entry_.reset();
    }
  }
  return ret;
}

int ObDASLegacyTRMergeIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(0 == capacity)) {
    count = 0;
  } else if (replay_active_) {
    ret = replay_get_next_rows(count, capacity);
  } else if (OB_ISNULL(sparse_retrieval_iter_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = sparse_retrieval_iter_->get_next_rows(capacity, count);
    if (OB_SUCC(ret) || OB_ITER_END == ret) {
      if (count > 0) {
        collect_replay_rows(count);
      }
      if (OB_ITER_END == ret) {
        finish_replay_collection();
      }
    } else {
      collecting_entry_.reset();
    }
  }
  return ret;
}

static int get_query_tokens_by_compacting_repeated_token(ObString &query_str,
                                                         const ObCollationType &cs_type,
                                                         ObArray<ObString> &query_tokens,
                                                         ObArray<double> &boost_values,
                                                         common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  char split_token_tag = ' ';
  char split_boost_tag = '^';
  hash::ObHashMap<ObString, double> token_map;
  const int64_t ft_word_bkt_cnt = MAX(query_str.length() / 10, 2);
  if (OB_FAIL(token_map.create(ft_word_bkt_cnt, common::ObMemAttr("FTWordMap")))) {
  }
  while (!query_str.empty() && OB_SUCC(ret)) {
    ObString token_str = query_str.split_on(split_token_tag);
    query_str = query_str.trim();
    if (token_str.empty()) {
      token_str = query_str;
      query_str.reset();
    }
    ObString token_key = token_str.split_on(split_boost_tag);
    double boost_value = 1.0;
    if (token_key.empty()) {
      token_key = token_str;
      token_str.reset();
    } else {
      char *boost_str = static_cast<char *>(alloc.alloc(token_str.length() + 1));
      if (OB_ISNULL(boost_str)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        memcpy(boost_str, token_str.ptr(), token_str.length());
        boost_str[token_str.length()] = '\0';
        char *end_ptr = nullptr;
        boost_value = strtod(boost_str, &end_ptr);
        if (end_ptr != boost_str + token_str.length() || boost_value <= 0.0) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid field boost");
        }
      }
    }
    double cur_boost = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(token_map.get_refactored(token_key, cur_boost))) {
      if (OB_HASH_NOT_EXIST != ret) {
      } else if (OB_FAIL(token_map.set_refactored(token_key, boost_value, 1/*overwrite*/))) {
      }
    } else if (OB_FAIL(token_map.set_refactored(token_key, boost_value + cur_boost, 1/*overwrite*/))) {
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(query_tokens.reserve(token_map.size()))) {
  } else if (OB_FAIL(boost_values.reserve(token_map.size()))) {
  }
  for (hash::ObHashMap<ObString, double>::const_iterator iter = token_map.begin();
      OB_SUCC(ret) && iter != token_map.end();
      ++iter) {
    const ObString &token = iter->first;
    ObString token_string;
    if (OB_FAIL(common::ObCharset::charset_convert(alloc, token, ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI, cs_type, token_string, common::ObCharset::CONVERT_FLAG::COPY_STRING_ON_SAME_CHARSET))) {
    } else if (OB_FAIL(query_tokens.push_back(token_string))) {
    } else if (OB_FAIL(boost_values.push_back(iter->second))) {
    }
  }
  return ret;
}

static int get_query_tokens_directly(ObString &query_str,
                                     const ObCollationType &cs_type,
                                     ObArray<ObString> &query_tokens,
                                     ObArray<double> &boost_values,
                                     common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  char split_token_tag = ' ';
  char split_boost_tag = '^';
  while (!query_str.empty() && OB_SUCC(ret)) {
    ObString token_str = query_str.split_on(split_token_tag);
    query_str = query_str.trim();
    if (token_str.empty()) {
      token_str = query_str;
      query_str.reset();
    }
    ObString token_key = token_str.split_on(split_boost_tag);
    double boost_value = 1.0;
    if (token_key.empty()) {
      token_key = token_str;
      token_str.reset();
    } else {
      char *boost_str = static_cast<char *>(alloc.alloc(token_str.length() + 1));
      if (OB_ISNULL(boost_str)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        memcpy(boost_str, token_str.ptr(), token_str.length());
        boost_str[token_str.length()] = '\0';
        char *end_ptr = nullptr;
        boost_value = strtod(boost_str, &end_ptr);
        if (end_ptr != boost_str + token_str.length() || boost_value <= 0.0) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid field boost");
        }
      }
    }
    double cur_boost = 0;
    if (OB_FAIL(ret)) {
    } else {
      ObString token_string;
      if (OB_FAIL(common::ObCharset::charset_convert(alloc, token_key, ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI, cs_type, token_string, common::ObCharset::CONVERT_FLAG::COPY_STRING_ON_SAME_CHARSET))) {
      } else if (OB_FAIL(query_tokens.push_back(token_string))) {
      } else if (OB_FAIL(boost_values.push_back(boost_value))) {
      }
    }
  }
  return ret;
}

int ObDASLegacyTRMergeIter::build_query_tokens(const ObDASIRScanCtDef *ir_ctdef,
                                         ObDASIRScanRtDef *ir_rtdef,
                                         common::ObIAllocator &alloc,
                                         ObArray<ObString> &query_tokens,
                                         ObArray<double> &boost_values,
                                         ObFtsEvalNode *&root_node,
                                         bool &has_duplicate_boolean_tokens)
{
  int ret = OB_SUCCESS;
  has_duplicate_boolean_tokens = false;
  ObExpr *search_text = ir_ctdef->search_text_;
  ObEvalCtx *eval_ctx = ir_rtdef->eval_ctx_;
  ObDatum *search_text_datum = nullptr;
  if (OB_ISNULL(search_text) || OB_ISNULL(eval_ctx)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (query_tokens.count() != 0 || boost_values.count() != 0) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(search_text->eval(*eval_ctx, search_text_datum))) {
  } else if (0 == search_text_datum->len_) {
    // empty query text
  } else if (OB_NOT_NULL(ir_ctdef->field_boost_expr_)) {
    const ObString &search_text_string = search_text_datum->get_string();
    const ObCollationType &cs_type = search_text->datum_meta_.cs_type_;
    const ObCollationType dst_type = ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI;

    ObString str_dest;
    if (cs_type != dst_type) {
      ObString tmp_out;
      if (OB_FAIL(ObCharset::tolower(cs_type, search_text_string, tmp_out, alloc))) {
      } else if (OB_FAIL(common::ObCharset::charset_convert(alloc, tmp_out, cs_type, dst_type, str_dest))) {
      }
    } else if (OB_FAIL(ObCharset::tolower(cs_type, search_text_string, str_dest, alloc))){
    }
    if (OB_FAIL(ret)) {
    } else {
      const bool compact_repeated_token = ir_rtdef->minimum_should_match_ <= 1;
      ObString query_str = str_dest.trim();
      if (compact_repeated_token && OB_FAIL(get_query_tokens_by_compacting_repeated_token(query_str, cs_type, query_tokens, boost_values, alloc))) {
      } else if (!compact_repeated_token && OB_FAIL(get_query_tokens_directly(query_str, cs_type, query_tokens, boost_values, alloc))) {
      }
    }
  } else if (BOOLEAN_MODE == ir_ctdef->mode_flag_) {
    const ObString &search_text_string = search_text_datum->get_string();
    const ObCollationType &cs_type = search_text->datum_meta_.cs_type_;
    const ObCollationType dst_type = ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI;

    ObString str_dest;
    if (cs_type != dst_type) {
      ObString tmp_out;
      if (OB_FAIL(ObCharset::tolower(cs_type, search_text_string, tmp_out, alloc))) {
      } else if (OB_FAIL(common::ObCharset::charset_convert(alloc, tmp_out, cs_type, dst_type, str_dest))) {
      }
    } else if (OB_FAIL(ObCharset::tolower(cs_type, search_text_string, str_dest, alloc))){
    }

    void *buf = nullptr;
    FtsParserResult *fts_parser;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(buf = (&alloc)->alloc(sizeof(FtsParserResult)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      fts_parser = static_cast<FtsParserResult *>(buf);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(buf = (&alloc)->alloc(str_dest.length() + 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMSET(buf, 0, str_dest.length() + 1);
      MEMCPY(buf, str_dest.ptr(), str_dest.length());
    }

    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(fts_parse_docment(static_cast<char *>(buf), str_dest.length(), &alloc, fts_parser))) {
    } else if (FTS_OK != fts_parser->ret_) {
      if (FTS_ERROR_MEMORY == fts_parser->ret_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (FTS_ERROR_SYNTAX == fts_parser->ret_) {
        ret = OB_ERR_PARSER_SYNTAX;
      } else if (FTS_ERROR_OTHER == fts_parser->ret_) {
        ret = OB_ERR_UNEXPECTED;
      }
    } else if (OB_ISNULL(fts_parser->root_)) {
      // do nothing
    } else {
      FtsNode *node = fts_parser->root_;
      ObFtsEvalNode *parant_node =nullptr;
      hash::ObHashMap<ObString, int32_t> tokens_map;
      const int64_t ft_word_bkt_cnt = MAX(search_text_string.length() / 10, 2);
      if (OB_FAIL(tokens_map.create(ft_word_bkt_cnt, common::ObMemAttr("FTWordMap")))) {
      } else if (OB_FAIL(ObFtsEvalNode::fts_boolean_node_create(parant_node, node, cs_type, alloc, query_tokens, tokens_map, has_duplicate_boolean_tokens))) {
      } else {
        root_node = parant_node;
      }
    }
  } else {
    // TODO: FTParseHelper currently does not support deduplicate tokens
    //       We should abstract such universal analyse functors into utility structs
    const ObString &search_text_string = search_text_datum->get_string();
    const ObString &parser_name = ir_ctdef->get_inv_idx_scan_ctdef()->table_param_.get_parser_name();
    const ObString &parser_properties = ir_ctdef->get_inv_idx_scan_ctdef()->table_param_.get_parser_property();

    const ObObjMeta &meta = search_text->obj_meta_;
    int64_t doc_length = 0;
    storage::ObFTParseHelper tokenize_helper;
    common::ObSEArray<ObFTWord, 16> tokens;
    hash::ObHashMap<ObFTWord, int64_t> token_map;
    const int64_t ft_word_bkt_cnt = MAX(search_text_string.length() / 10, 2);
    if (OB_FAIL(tokenize_helper.init(&alloc, parser_name, parser_properties))) {
    } else if (OB_FAIL(token_map.create(ft_word_bkt_cnt, common::ObMemAttr("FTWordMap")))) {
    } else if (OB_FAIL(tokenize_helper.segment(
                           meta,
                           search_text_string.ptr(),
                           search_text_string.length(),
                           doc_length,
                           token_map))) {
    } else {
      for (hash::ObHashMap<ObFTWord, int64_t>::const_iterator iter = token_map.begin();
          OB_SUCC(ret) && iter != token_map.end();
          ++iter) {
        const ObFTWord &token = iter->first;
        ObString token_string;
        if (OB_FAIL(ob_write_string(alloc, token.get_word().get_string(), token_string))) {
        } else if (OB_FAIL(query_tokens.push_back(token_string))) {
        }
      }
    }
  }
  return ret;
}

int ObDASLegacyTRMergeIter::init_topk_limit()
{
  int ret = OB_SUCCESS;
  topk_limit_ = 0;
  ObExpr *topk_limit_expr = ir_ctdef_->topk_limit_expr_;
  ObExpr *topk_offset_expr = ir_ctdef_->topk_offset_expr_;
  if (OB_UNLIKELY(!topk_mode_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t limit = 0;
    int64_t offset = 0;
    ObDatum *topk_limit_datum = nullptr;
    ObDatum *topk_offset_datum = nullptr;
    if (OB_FAIL(topk_limit_expr->eval(*ir_rtdef_->eval_ctx_, topk_limit_datum))) {
    } else if (nullptr != topk_offset_expr && OB_FAIL(topk_offset_expr->eval(*ir_rtdef_->eval_ctx_, topk_offset_datum))) {
    } else {
      limit = (topk_limit_datum->is_null() || topk_limit_datum->get_int() < 0) ? 0 : topk_limit_datum->get_int();
      if (nullptr != topk_offset_datum) {
        offset = (topk_offset_datum->is_null() || topk_offset_datum->get_int() < 0) ? 0 : topk_offset_datum->get_int();
      }
      topk_limit_ = limit + offset;
    }
  }
  return ret;
}

int ObDASLegacyTRMergeIter::build_replay_key(std::string &key, bool &eligible) const
{
  int ret = OB_SUCCESS;
  eligible = false;
  key.clear();
  const ObDASScanCtDef *inv_scan_ctdef = nullptr;
  const ObDASScanRtDef *inv_scan_rtdef = nullptr;
  ObEvalCtx *eval_ctx = OB_ISNULL(ir_rtdef_) ? nullptr : ir_rtdef_->eval_ctx_;
  if (OB_ISNULL(ir_ctdef_) || OB_ISNULL(ir_rtdef_) || OB_ISNULL(eval_ctx)
      || OB_ISNULL(snapshot_) || OB_ISNULL(ir_ctdef_->search_text_)
      || OB_ISNULL(ir_ctdef_->inv_scan_domain_id_col_)
      || OB_ISNULL(inv_scan_ctdef = ir_ctdef_->get_inv_idx_scan_ctdef())
      || OB_ISNULL(inv_scan_rtdef = ir_rtdef_->get_inv_idx_scan_rtdef())) {
    // Incomplete scan definition: no cache.
  } else if (function_lookup_mode_ || NATURAL_LANGUAGE_MODE != ir_ctdef_->mode_flag_
      || 0 == query_tokens_.count() || 0 == children_cnt_
      || nullptr != ir_ctdef_->field_boost_expr_
      || !total_doc_cnt_tablet_id_.is_valid() || !inv_idx_tablet_id_.is_valid()) {
    // Field boosts are runtime expressions, and the other scan shapes have
    // additional inputs that are not part of the key.
  } else if (OB_ISNULL(ir_ctdef_->relevance_proj_col_) && !eval_ctx->is_vectorized()) {
    // The non-vectorized merge projection always writes the relevance datum.
  } else if (nullptr != ir_ctdef_->match_filter_
      && (1 != ir_ctdef_->match_filter_->arg_cnt_
          || ir_ctdef_->match_filter_->args_[0] != ir_ctdef_->relevance_proj_col_)) {
    // Only the canonical BOOL(match score) filter is guaranteed to accept every
    // replayed row without re-reading scan state.
  } else if (!snapshot_->version().is_valid()) {
    // No data version to validate against.
  } else {
    eligible = true;
    const char magic[8] = {'O', 'B', 'F', 'T', 'R', 'P', '0', '1'};
    key.append(magic, sizeof(magic));
    const uint8_t mode = static_cast<uint8_t>(ir_ctdef_->mode_flag_);
    const uint64_t ir_flags = ir_ctdef_->flags_;
    const uint32_t plan_flags = flags_;
    const uint8_t vectorized = eval_ctx->is_vectorized() ? 1 : 0;
    const int64_t max_batch_size = eval_ctx->max_batch_size_;
    uint8_t shape = 0;
    if (ir_ctdef_->need_calc_relevance()) {
      shape |= 1;
    }
    if (ir_ctdef_->need_proj_relevance_score()) {
      shape |= 2;
    }
    if (ir_ctdef_->need_inv_idx_agg()) {
      shape |= 4;
    }
    if (ir_ctdef_->need_block_max_scan()) {
      shape |= 8;
    }
    if (ir_ctdef_->need_avg_doc_len_est()) {
      shape |= 16;
    }
    if (ir_ctdef_->need_estimate_total_doc_cnt()) {
      shape |= 32;
    }
    if (ir_ctdef_->avg_doc_len_est_spec_.can_est_by_sum_skip_index_) {
      shape |= 64;
    }
    if (OB_NOT_NULL(ir_ctdef_->relevance_proj_col_)) {
      shape |= 128;
    }
    append_replay_key_value(key, mode);
    append_replay_key_value(key, ir_flags);
    append_replay_key_value(key, plan_flags);
    append_replay_key_value(key, shape);
    append_replay_key_value(key, vectorized);
    append_replay_key_value(key, max_batch_size);
    const int32_t text_cs_type = static_cast<int32_t>(ir_ctdef_->search_text_->datum_meta_.cs_type_);
    const int32_t text_type = static_cast<int32_t>(ir_ctdef_->search_text_->datum_meta_.type_);
    append_replay_key_value(key, text_cs_type);
    append_replay_key_value(key, text_type);
    append_replay_key_value(key, ir_ctdef_->estimated_total_doc_cnt_);
    append_replay_key_value(key, ir_rtdef_->minimum_should_match_);
    const common::ObLimitParam &limit_param = inv_scan_rtdef->limit_param_;
    append_replay_key_value(key, limit_param.limit_);
    append_replay_key_value(key, limit_param.offset_);
    append_replay_key_value(key, topk_limit_);
    const uint64_t total_doc_cnt_tablet_id = total_doc_cnt_tablet_id_.id();
    const uint64_t inv_idx_tablet_id = inv_idx_tablet_id_.id();
    const uint64_t inv_ref_table_id = inv_scan_ctdef->ref_table_id_;
    append_replay_key_value(key, total_doc_cnt_tablet_id);
    append_replay_key_value(key, inv_idx_tablet_id);
    append_replay_key_value(key, inv_ref_table_id);
    const uint32_t child_count = children_cnt_;
    append_replay_key_value(key, child_count);
    const int32_t token_count = static_cast<int32_t>(query_tokens_.count());
    append_replay_key_value(key, token_count);
    for (int64_t i = 0; i < query_tokens_.count(); ++i) {
      append_replay_key_string(key, query_tokens_.at(i));
    }
    const int32_t weight_count = static_cast<int32_t>(dim_weights_.count());
    append_replay_key_value(key, weight_count);
    for (int64_t i = 0; i < dim_weights_.count(); ++i) {
      append_replay_key_value(key, dim_weights_.at(i));
    }
  }
  return ret;
}

int ObDASLegacyTRMergeIter::try_start_replay(bool &started)
{
  int ret = OB_SUCCESS;
  started = false;
  // The replay cache allocates from the global heap, whose failure mode is a
  // std::bad_alloc; a cache fault must never fail the scan, so it only makes
  // this execution fall back to a normal table scan.
  std::string key;
  bool eligible = false;
  bool gate = false;
  share::SCN scan_version;
  share::SCN current_version;
  try {
    if (OB_FAIL(build_replay_key(key, eligible))) {
    } else if (!eligible || !(gate = replay_eligible(tx_desc_, exec_ctx_))) {
    } else {
      scan_version = snapshot_->version();
      current_version = get_data_version();
      std::shared_ptr<ObFTSReplayEntry> entry;
      if (current_version.is_valid() && scan_version.is_valid()) {
        entry = ObFTSReplayCache::instance().get(key);
      }
      if (nullptr != entry && entry->complete_ && entry->commit_ts_.is_valid()
          && entry->commit_ts_ == current_version && scan_version >= entry->version_) {
        replay_entry_ = entry;
        replay_pos_ = 0;
        replay_active_ = true;
        sr_iter_param_.limit_param_ = &ir_rtdef_->get_inv_idx_scan_rtdef()->limit_param_;
        sr_iter_param_.eval_ctx_ = ir_rtdef_->eval_ctx_;
        sr_iter_param_.id_proj_expr_ = ir_ctdef_->inv_scan_domain_id_col_;
        sr_iter_param_.relevance_expr_ = ir_ctdef_->relevance_expr_;
        sr_iter_param_.relevance_proj_expr_ = ir_ctdef_->relevance_proj_col_;
        sr_iter_param_.dim_weights_ = dim_weights_.count() >= 1 ? &dim_weights_ : nullptr;
        sr_iter_param_.filter_expr_ = nullptr;
        sr_iter_param_.topk_limit_ = topk_limit_;
        started = true;
      }
    }
  } catch (...) {
    LOG_WARN("text retrieval replay probe failed, falling back to a table scan");
    started = false;
    replay_entry_.reset();
    replay_pos_ = 0;
    replay_active_ = false;
  }
  if (replay_trace()) {
    LOG_INFO("fts replay probe", K(eligible), K(gate), K(started),
             K(scan_version), K(current_version),
             "key_bytes", static_cast<int64_t>(key.size()));
  }
  return ret;
}

int ObDASLegacyTRMergeIter::prepare_replay_collection()
{
  int ret = OB_SUCCESS;
  collecting_entry_.reset();
  std::string key;
  bool eligible = false;
  bool gate = false;
  share::SCN snapshot_version;
  share::SCN commit_ts;
  try {
    if (OB_FAIL(build_replay_key(key, eligible))) {
    } else if (!eligible || !(gate = replay_eligible(tx_desc_, exec_ctx_))) {
    } else {
      snapshot_version = snapshot_->version();
      commit_ts = get_data_version();
      // The rows are read at snapshot_version, so every commit the service has
      // already observed must be at or below that snapshot.  Otherwise a commit
      // could have landed between the snapshot and this scan and the recorded
      // rows would not describe the recorded version.
      if (snapshot_version.is_valid() && commit_ts.is_valid() && commit_ts <= snapshot_version) {
        std::shared_ptr<ObFTSReplayEntry> entry = std::make_shared<ObFTSReplayEntry>();
        entry->key_ = key;
        entry->version_ = snapshot_version;
        entry->commit_ts_ = commit_ts;
        entry->writes_relevance_ = nullptr != ir_ctdef_->relevance_proj_col_;
        entry->id_is_int_ =
            common::ObUInt64Type == ir_ctdef_->inv_scan_domain_id_col_->datum_meta_.type_;
        entry->rows_.reserve(4096);
        collecting_entry_ = entry;
      }
    }
  } catch (...) {
    LOG_WARN("text retrieval replay collection could not be prepared");
    collecting_entry_.reset();
  }
  if (replay_trace()) {
    LOG_INFO("fts replay prepare", K(eligible), K(gate), K(snapshot_version),
             K(commit_ts), "key_bytes", static_cast<int64_t>(key.size()),
             "collecting", OB_NOT_NULL(collecting_entry_));
  }
  return ret;
}

void ObDASLegacyTRMergeIter::collect_replay_rows(const int64_t count)
{
  if (nullptr == collecting_entry_ || count <= 0 || OB_ISNULL(ir_ctdef_)
      || OB_ISNULL(ir_rtdef_) || OB_ISNULL(ir_rtdef_->eval_ctx_)) {
    return;
  }
  ObFTSReplayEntry &entry = *collecting_entry_;
  ObEvalCtx *eval_ctx = ir_rtdef_->eval_ctx_;
  ObExpr *id_expr = ir_ctdef_->inv_scan_domain_id_col_;
  ObExpr *relevance_expr = ir_ctdef_->relevance_proj_col_;
  if (static_cast<int64_t>(entry.rows_.size()) + count > REPLAY_MAX_ROWS
      || static_cast<int64_t>(entry.id_bytes_.size()) > REPLAY_MAX_ID_BYTES
      || count > eval_ctx->max_batch_size_) {
    collecting_entry_.reset();
    return;
  }
  const ObDatum *relevance_datums = entry.writes_relevance_
      ? relevance_expr->locate_batch_datums(*eval_ctx) : nullptr;
  try {
    if (entry.rows_.size() + static_cast<size_t>(count) > entry.rows_.capacity()) {
      const int64_t grown = OB_MAX(static_cast<int64_t>(entry.rows_.size()) + count,
          static_cast<int64_t>(entry.rows_.capacity()) * 2);
      entry.rows_.reserve(static_cast<size_t>(grown));
    }
    for (int64_t i = 0; i < count; ++i) {
      // Read the datum the projection wrote for this row; a datum's batch index
      // is masked exactly like the projection's write index.
      const ObDatum &id_datum = id_expr->locate_expr_datum(*eval_ctx, i);
      const ObDatum *relevance_datum = nullptr == relevance_datums
          ? nullptr : &relevance_expr->locate_expr_datum(*eval_ctx, i);
      if (OB_UNLIKELY(id_datum.is_null() || id_datum.is_ext() || id_datum.is_outrow()
          || id_datum.has_lob_header())) {
        collecting_entry_.reset();
        return;
      }
      ObFTSReplayRow row;
      row.relevance_ = nullptr == relevance_datum ? 0.0 : relevance_datum->get_double();
      if (entry.id_is_int_) {
        row.id_ = id_datum.get_int();
        row.id_len_ = 0;
      } else {
        row.id_ = static_cast<int64_t>(entry.id_bytes_.size());
        row.id_len_ = static_cast<int64_t>(id_datum.len_);
        entry.id_bytes_.append(id_datum.ptr_, id_datum.len_);
      }
      entry.rows_.push_back(row);
    }
  } catch (...) {
    LOG_WARN_RET(OB_SUCCESS, "text retrieval replay collection failed");
    collecting_entry_.reset();
  }
  if (OB_NOT_NULL(collecting_entry_) && replay_trace()) {
    LOG_INFO("fts replay collect", K(count), "rows", collecting_entry_->rows_.size(),
             "id_bytes", collecting_entry_->id_bytes_.size(), K(topk_limit_));
  }
  if (OB_NOT_NULL(collecting_entry_) && topk_mode_ && topk_limit_ > 0
      && static_cast<int64_t>(collecting_entry_->rows_.size()) >= topk_limit_) {
    // The pushed-down top-k heap never holds more than topk_limit_ rows, so the
    // rows collected so far already are the whole scan output.  Publish now
    // because the operator above a limit may stop pulling without ever asking
    // for another batch, and then the end of the scan is never observed.
    finish_replay_collection();
  }
}

void ObDASLegacyTRMergeIter::finish_replay_collection()
{
  if (nullptr == collecting_entry_) {
    return;
  }
  std::shared_ptr<ObFTSReplayEntry> entry = collecting_entry_;
  collecting_entry_.reset();
  if (OB_ISNULL(snapshot_) || OB_ISNULL(ir_rtdef_)) {
    return;
  }
  // The maximum query score is recorded only when the plan itself asks for it
  // (see get_query_max_score()).  A replayed scan is served under the same key,
  // which includes the shape flags that decide whether the plan reads the
  // score, so a scan that never asked while it produced its rows cannot be
  // asked later either.
  const share::SCN end_version = get_data_version();
  const share::SCN scan_version = snapshot_->version();
  if (replay_trace()) {
    LOG_INFO("fts replay finish", K(end_version), K(scan_version),
             "commit_ts", entry->commit_ts_, "rows",
             static_cast<int64_t>(entry->rows_.size()), K(topk_mode_),
             K(topk_limit_));
  }
  if (!end_version.is_valid() || !scan_version.is_valid()) {
    return;
  }
  // The row set is only a stable statement snapshot when no commit the service
  // knows about landed while the scan ran and the read snapshot still sees the
  // recorded version.  Otherwise a later replay could disagree with a fresh
  // scan.
  if (end_version != entry->commit_ts_ || scan_version < entry->version_) {
    return;
  }
  if (entry->score_unstable_) {
    return;
  }
  const ObDASScanRtDef *inv_scan_rtdef = ir_rtdef_->get_inv_idx_scan_rtdef();
  if (OB_UNLIKELY(OB_ISNULL(inv_scan_rtdef))) {
    return;
  }
  const common::ObLimitParam &limit_param = inv_scan_rtdef->limit_param_;
  // The top-k iterator ignores the limit param and stops on the heap size, so a
  // replayed top-k scan must report its end on the call after the last row
  // exactly like the live one.
  entry->ends_with_limit_ = !topk_mode_ && limit_param.is_valid()
      && static_cast<int64_t>(entry->rows_.size()) >= limit_param.limit_;
  entry->complete_ = true;
  try {
    ObFTSReplayCache::instance().put(entry);
  } catch (...) {
    LOG_WARN_RET(OB_SUCCESS, "failed to publish a text retrieval replay result");
  }
}

int ObDASLegacyTRMergeIter::replay_get_next_row()
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  // Mirrors the sparse merge round: a single row is requested through the batch
  // interface and an end reported together with that row is a protocol error.
  if (OB_FAIL(replay_get_next_rows(count, 1))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
    } else if (OB_UNLIKELY(count != 0)) {
      ret = OB_ERR_UNEXPECTED;
    }
  } else if (OB_UNLIKELY(count != 1)) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObDASLegacyTRMergeIter::replay_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (OB_ISNULL(replay_entry_) || OB_ISNULL(ir_rtdef_) || OB_ISNULL(ir_rtdef_->eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (0 == capacity) {
  } else {
    ObFTSReplayEntry &entry = *replay_entry_;
    const int64_t total = static_cast<int64_t>(entry.rows_.size());
    if (replay_pos_ >= total) {
      ret = OB_ITER_END;
    } else {
      ObEvalCtx *eval_ctx = ir_rtdef_->eval_ctx_;
      // The top-k iterator fills as much of the caller's capacity as the heap
      // still holds; the merge iterator caps its batches at max_batch_size_.
      const int64_t real_capacity = topk_mode_
          ? capacity : MIN(capacity, sr_iter_param_.max_batch_size_);
      const int64_t batch_count = MIN(real_capacity, total - replay_pos_);
      if (replay_trace()) {
        LOG_INFO("fts replay rows", K(capacity), K(batch_count), K(replay_pos_),
                 "total", total, K(topk_mode_));
      }
      ObExpr *id_expr = sr_iter_param_.id_proj_expr_;
      ObExpr *relevance_expr = sr_iter_param_.relevance_proj_expr_;
      const bool vectorized = eval_ctx->is_vectorized();
      ObBitVector *id_flags = vectorized ? &id_expr->get_evaluated_flags(*eval_ctx) : nullptr;
      ObBitVector *relevance_flags = (vectorized && entry.writes_relevance_)
          ? &relevance_expr->get_evaluated_flags(*eval_ctx) : nullptr;
      ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
      for (int64_t i = 0; i < batch_count; ++i) {
        guard.set_batch_idx(i);
        const ObFTSReplayRow &row = entry.rows_[replay_pos_ + i];
        ObDatum &id_datum = id_expr->locate_datum_for_write(*eval_ctx);
        write_replay_id_datum(id_datum, entry, row);
        if (nullptr != id_flags) {
          id_flags->set(i);
        }
        if (entry.writes_relevance_) {
          ObDatum &relevance_datum = relevance_expr->locate_datum_for_write(*eval_ctx);
          relevance_datum.set_double(row.relevance_);
          if (nullptr != relevance_flags) {
            relevance_flags->set(i);
          }
        }
      }
      id_expr->set_evaluated_projected(*eval_ctx);
      if (entry.writes_relevance_) {
        relevance_expr->set_evaluated_projected(*eval_ctx);
      }
      count = batch_count;
      replay_pos_ += batch_count;
      // A scan stopped by its pushed-down limit reports the end together with
      // the last row; an exhausted scan learns about the end on the next call.
      if (replay_pos_ >= total && entry.ends_with_limit_) {
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

void ObDASLegacyTRMergeIter::reset_replay_state()
{
  replay_entry_.reset();
  collecting_entry_.reset();
  replay_pos_ = 0;
  replay_active_ = false;
}

int ObDASLegacyTRMergeIter::get_query_max_score(double &score)
{
  int ret = OB_SUCCESS;
  score = 0.0;
  if (OB_NOT_NULL(sparse_retrieval_iter_)) {
    ret = sparse_retrieval_iter_->get_query_max_score(score);
    if (OB_NOT_NULL(collecting_entry_)) {
      if (!collecting_entry_->score_queried_) {
        collecting_entry_->score_queried_ = true;
        collecting_entry_->max_query_score_ = score;
        collecting_entry_->max_query_score_err_ = ret;
      } else if (collecting_entry_->max_query_score_err_ != ret
          || (OB_SUCCESS == ret && collecting_entry_->max_query_score_ != score)) {
        collecting_entry_->score_unstable_ = true;
      }
    }
  } else if (OB_NOT_NULL(replay_entry_)) {
    if (!replay_entry_->score_queried_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("replayed text retrieval scan has no recorded query max score", K(ret));
    } else if (OB_SUCCESS != replay_entry_->max_query_score_err_) {
      ret = replay_entry_->max_query_score_err_;
    } else {
      score = replay_entry_->max_query_score_;
    }
  }
  return ret;
}

namespace
{

class ObDASLegacyTextRetrievalEngine final
    : public ObIDASTextRetrievalEngine
{
public:
  explicit ObDASLegacyTextRetrievalEngine(common::ObIAllocator &allocator)
    : allocator_(&allocator), iter_(nullptr)
  {}
  ~ObDASLegacyTextRetrievalEngine() override = default;

  int init(ObDASTRMergeIterParam &param) override
  {
    int ret = OB_SUCCESS;
    void *buffer = nullptr;
    if (OB_NOT_NULL(iter_)) {
      ret = OB_INIT_TWICE;
    } else if (OB_ISNULL(buffer = allocator_->alloc(
        sizeof(ObDASLegacyTRMergeIter)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      iter_ = new (buffer) ObDASLegacyTRMergeIter();
      if (OB_FAIL(iter_->init(param))) {
      }
    }
    return ret;
  }

  int bind_source_tree(
      ObDASIter **children,
      const uint32_t child_count) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(iter_)
        || (0 == child_count && nullptr != children)
        || (child_count > 0 && nullptr == children)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      iter_->get_children() = children;
      iter_->set_children_cnt(child_count);
    }
    return ret;
  }

  int set_related_tablet_ids(
      const ObDASFTSTabletID &related_tablet_ids) override
  {
    return OB_ISNULL(iter_)
        ? OB_NOT_INIT
        : iter_->set_related_tablet_ids(related_tablet_ids);
  }

  int do_table_scan() override
  {
    return OB_ISNULL(iter_) ? OB_NOT_INIT : iter_->do_table_scan();
  }

  int reuse() override
  {
    return OB_ISNULL(iter_) ? OB_NOT_INIT : iter_->reuse();
  }

  int rescan() override
  {
    return OB_ISNULL(iter_) ? OB_NOT_INIT : iter_->rescan();
  }

  int get_next_row() override
  {
    return OB_ISNULL(iter_) ? OB_NOT_INIT : iter_->get_next_row();
  }

  int get_next_rows(int64_t &count, const int64_t capacity) override
  {
    count = 0;
    return OB_ISNULL(iter_)
        ? OB_NOT_INIT : iter_->get_next_rows(count, capacity);
  }

  int set_lookup_keys(
      const common::ObIArray<std::pair<ObDocIdExt, int>> &virtual_rangekeys,
      const int64_t batch_size) override
  {
    return OB_ISNULL(iter_)
        ? OB_NOT_INIT
        : iter_->set_children_iter_rangekey(
            virtual_rangekeys, batch_size);
  }

  bool is_taat_mode() const override
  {
    return OB_NOT_NULL(iter_) && iter_->is_taat_mode();
  }

  int get_query_max_score(double &score) override
  {
    score = 0.0;
    return OB_ISNULL(iter_)
        ? OB_NOT_INIT : iter_->get_query_max_score(score);
  }

  void destroy() override
  {
    common::ObIAllocator *allocator = allocator_;
    if (OB_NOT_NULL(iter_)) {
      // The facade owns the DAS tree.  Release physical state while dynamic
      // dispatch still sees the legacy derived type, but never recurse into
      // or destruct the borrowed children.
      iter_->set_children_cnt(0);
      iter_->get_children() = nullptr;
      if (iter_->is_inited()) {
        const int ret = iter_->release();
        if (OB_SUCCESS != ret) {
        }
      }
      // The destructor must not run: ~ObDASIter() calls release(), which
      // invokes the pure virtual inner_release() on an object whose derived
      // part is already gone.  release() above already dropped all physical
      // state, including the replay entry, so free the storage directly.
      allocator->free(iter_);
      iter_ = nullptr;
    }
    this->~ObDASLegacyTextRetrievalEngine();
    allocator->free(this);
  }

private:
  common::ObIAllocator *allocator_;
  ObDASLegacyTRMergeIter *iter_;
  DISALLOW_COPY_AND_ASSIGN(ObDASLegacyTextRetrievalEngine);
};

int create_legacy_text_retrieval_engine(
    common::ObIAllocator &allocator,
    ObIDASTextRetrievalEngine *&engine)
{
  int ret = OB_SUCCESS;
  void *buffer = nullptr;
  engine = nullptr;
  if (OB_ISNULL(buffer = allocator.alloc(
      sizeof(ObDASLegacyTextRetrievalEngine)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    engine = new (buffer) ObDASLegacyTextRetrievalEngine(allocator);
  }
  return ret;
}

int build_legacy_text_retrieval_query(
    const ObDASIRScanCtDef *ir_ctdef,
    ObDASIRScanRtDef *ir_rtdef,
    common::ObIAllocator &allocator,
    ObArray<ObString> &query_tokens,
    ObArray<double> &boost_values,
    ObFtsEvalNode *&root_node,
    bool &has_duplicate_boolean_tokens)
{
  return ObDASLegacyTRMergeIter::build_query_tokens(
      ir_ctdef, ir_rtdef, allocator, query_tokens, boost_values, root_node,
      has_duplicate_boolean_tokens);
}

struct ObDASLegacyTextRetrievalRegistration
{
  ObDASLegacyTextRetrievalRegistration()
    : ret_(install_legacy_das_text_retrieval_engine())
  {}

  int ret_;
};

// This object is force-retained by the final composition target.  If it is
// absent, the query dispatcher fails closed with OB_NOT_INIT.
ObDASLegacyTextRetrievalRegistration g_text_retrieval_registration;

} // namespace

int install_legacy_das_text_retrieval_engine()
{
  return install_das_text_retrieval_engine_factory(
      create_legacy_text_retrieval_engine,
      build_legacy_text_retrieval_query);
}

} // namespace sql
} // namespace oceanbase
