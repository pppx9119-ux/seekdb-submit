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

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/dict/ob_ft_dict_hub.h"

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/dict/ob_ft_cache_container.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "storage/fts/dict/ob_ft_range_dict.h"
namespace oceanbase
{
namespace storage
{
int ObFTDictHub::init()
{
  static constexpr int K_MAX_DICT_BUCKET = 128; // for now, only built-in dicts.
  int ret = OB_SUCCESS;
  if (OB_FAIL(dict_map_.create(K_MAX_DICT_BUCKET, "dict_map"))) {
  } else if (OB_FAIL(rw_dict_lock_.init(K_MAX_DICT_BUCKET))) {
  } else if (OB_FAIL(shared_dict_map_.create(K_MAX_DICT_BUCKET, "shared_dict_map"))) {
  } else {
    is_inited_ = true;
  }
  return ret;
};

int ObFTDictHub::destroy()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  if (shared_dict_map_.created()) {
    for (hash::ObHashMap<ObFTDictInfoKey, ObFTSharedDict>::iterator iter = shared_dict_map_.begin();
         iter != shared_dict_map_.end();
         ++iter) {
      ObFTSharedDict &shared_dict = iter->second;
      if (OB_NOT_NULL(shared_dict.dict_)) {
        static_cast<ObFTRangeDict *>(shared_dict.dict_)->~ObFTRangeDict();
      }
      if (OB_NOT_NULL(shared_dict.container_)) {
        shared_dict.container_->~ObFTCacheRangeContainer();
      }
    }
    shared_dict_map_.destroy();
  }
  shared_dict_alloc_.reset();
  return ret;
}

int ObFTDictHub::get_shared_dict(const ObFTDictDesc &desc, ObIFTDict *&dict)
{
  int ret = OB_SUCCESS;
  ObFTDictInfoKey key(static_cast<uint64_t>(desc.type_));
  ObFTSharedDict shared_dict;
  dict = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    {
      ObBucketHashRLockGuard guard(rw_dict_lock_, key.hash());
      if (OB_FAIL(shared_dict_map_.get_refactored(key, shared_dict))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("failed to get shared dict", K(ret), K(desc.type_));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }

    if (OB_SUCC(ret) && OB_ISNULL(shared_dict.dict_)) {
      ObBucketHashWLockGuard guard(rw_dict_lock_, key.hash());
      // another thread may have built it while we waited for the lock
      if (OB_FAIL(shared_dict_map_.get_refactored(key, shared_dict))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("failed to get shared dict", K(ret), K(desc.type_));
        } else {
          ret = OB_SUCCESS;
        }
      }
      if (OB_SUCC(ret) && OB_ISNULL(shared_dict.dict_)) {
        if (OB_FAIL(create_shared_dict(desc, shared_dict))) {
          LOG_WARN("failed to create shared dict", K(ret), K(desc.type_));
        } else if (OB_FAIL(shared_dict_map_.set_refactored(key, shared_dict))) {
          LOG_WARN("failed to save shared dict", K(ret), K(desc.type_));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(shared_dict.dict_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("shared dict is null", K(ret), K(desc.type_));
      } else {
        dict = shared_dict.dict_;
      }
    }
  }

  return ret;
}

int ObFTDictHub::build_cache_locked(const ObFTDictDesc &desc, ObFTCacheRangeContainer &container)
{
  int ret = OB_SUCCESS;
  ObFTDictInfoKey key(static_cast<uint64_t>(desc.type_));
  ObFTDictInfo info;
  container.reset();

  // try if valid with no recursive lock
  if (OB_FAIL(get_dict_info(key, info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // dict not exist, make new one here
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("failed to get dict info", K(ret), K(desc.type_));
    }
  } else if (OB_FAIL(ObFTRangeDict::try_load_cache(desc, info.range_count_, container))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to load dict cache", K(ret), K(desc.type_));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (OB_FAIL(ObFTRangeDict::build_cache_from_ik_dict(desc, container))) {
        LOG_WARN("failed to build dict cache", K(ret), K(desc.type_));
      } else if (FALSE_IT(info.range_count_ = container.get_handles().size())) {
      } else if (OB_FAIL(put_dict_info(key, info))) {
        LOG_WARN("failed to save dict info", K(ret), K(desc.type_));
      }
    }
  }

  return ret;
}

int ObFTDictHub::create_shared_dict(const ObFTDictDesc &desc, ObFTSharedDict &shared_dict)
{
  int ret = OB_SUCCESS;
  ObFTCacheRangeContainer *container = nullptr;
  ObFTRangeDict *range_dict = nullptr;

  if (OB_ISNULL(container = OB_NEWx(ObFTCacheRangeContainer, &shared_dict_alloc_, shared_dict_alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate cache range container", K(ret));
  } else if (OB_FAIL(build_cache_locked(desc, *container))) {
    LOG_WARN("failed to build dict cache", K(ret), K(desc.type_));
  } else if (OB_ISNULL(range_dict = OB_NEWx(ObFTRangeDict,
                                            &shared_dict_alloc_,
                                            shared_dict_alloc_,
                                            container,
                                            desc))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate range dict", K(ret));
  } else if (OB_FAIL(range_dict->init())) {
    LOG_WARN("failed to init range dict", K(ret), K(desc.type_));
  } else {
    shared_dict.dict_ = range_dict;
    shared_dict.container_ = container;
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(range_dict)) {
      range_dict->~ObFTRangeDict();
    }
    if (OB_NOT_NULL(container)) {
      container->~ObFTCacheRangeContainer();
    }
  }

  return ret;
}


int ObFTDictHub::get_dict_info(const ObFTDictInfoKey &key, ObFTDictInfo &info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(dict_map_.get_refactored(key, info))) {
    if (OB_HASH_NOT_EXIST != ret) {
    }
  }

  return ret;
}

int ObFTDictHub::put_dict_info(const ObFTDictInfoKey &key, const ObFTDictInfo &info)
{
  int ret = OB_SUCCESS;
  const int cover_exist_flag = 1;
  if (OB_FAIL(dict_map_.set_refactored(key, info, cover_exist_flag))) {
  }

  return ret;
}
} //  namespace storage
} //  namespace oceanbase
