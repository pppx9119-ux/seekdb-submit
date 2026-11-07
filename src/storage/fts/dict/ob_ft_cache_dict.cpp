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

#include <cstdint>
#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/dict/ob_ft_cache_dict.h"

#include "lib/allocator/ob_allocator.h"
#include "lib/charset/ob_charset.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/rc/ob_server_runtime.h"
#include "storage/fts/dict/ob_ft_cache.h"
#include "storage/fts/dict/ob_ft_dict_def.h"

namespace oceanbase
{
namespace storage
{
int ObFTCacheDict::init()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObFTCacheDict::match(const ObString &single_word, ObDATrieHit &hit) const
{
  int ret = OB_SUCCESS;
  ObFTWordCode code = 0;
  bool found = false;
  if (OB_FAIL(resolve_char(single_word, code, found))) {
  } else {
    ret = reader_.match_with_code(single_word.length(), code, found, hit, hit);
  }
  return ret;
}

uint32_t ObFTCacheDict::char_code_key(const ObString &single_word)
{
  // The memo only applies to a single character; pack its bytes into the key so
  // that equal keys always mean the same character.
  const uint8_t *ptr = reinterpret_cast<const uint8_t *>(single_word.ptr());
  uint32_t key = 0;
  switch (single_word.length()) {
    case 1:
      key = ptr[0];
      break;
    case 2:
      key = (static_cast<uint32_t>(ptr[0]) << 8) | static_cast<uint32_t>(ptr[1]);
      break;
    case 3:
      key = (static_cast<uint32_t>(ptr[0]) << 16) | (static_cast<uint32_t>(ptr[1]) << 8)
            | static_cast<uint32_t>(ptr[2]);
      break;
    case 4:
      key = (static_cast<uint32_t>(ptr[0]) << 24) | (static_cast<uint32_t>(ptr[1]) << 16)
            | (static_cast<uint32_t>(ptr[2]) << 8) | static_cast<uint32_t>(ptr[3]);
      break;
    default:
      key = 0;
      break;
  }
  return key;
}

int ObFTCacheDict::resolve_char(const ObString &single_word,
                                ObFTWordCode &code,
                                bool &found) const
{
  int ret = OB_SUCCESS;
  code = 0;
  found = false;
  const uint32_t key = char_code_key(single_word);
  if (0 == key) {
    ret = reader_.find_code(single_word, code, found);
  } else {
    const uint32_t mask = static_cast<uint32_t>(CHAR_CODE_CACHE_SIZE) - 1;
    uint32_t slot = (key * 2654435761u) >> 20;
    bool cached = false;
    for (int64_t probe = 0; !cached && probe < 4; ++probe) {
      const uint64_t entry = char_code_cache_[slot].load(std::memory_order_relaxed);
      const uint32_t entry_key = static_cast<uint32_t>(entry >> 32);
      if (0 == entry) {
        break;
      } else if (entry_key == key) {
        const uint32_t stored = static_cast<uint32_t>(entry);
        if (CHAR_CODE_NOT_FOUND != stored) {
          code = static_cast<ObFTWordCode>(stored);
          found = true;
        }
        cached = true;
      } else {
        slot = (slot + 1) & mask;
      }
    }
    if (!cached) {
      if (OB_FAIL(reader_.find_code(single_word, code, found))) {
      } else {
        const uint32_t stored = found ? static_cast<uint32_t>(code) : CHAR_CODE_NOT_FOUND;
        char_code_cache_[slot].store((static_cast<uint64_t>(key) << 32) | stored,
                                     std::memory_order_relaxed);
      }
    }
  }
  return ret;
}

int ObFTCacheDict::match(const ObString &words, bool &is_match) const
{
  int ret = OB_SUCCESS;

  int64_t char_len = 0;
  ObDATrieHit hit(this, 0);
  for (int64_t offset = 0; OB_SUCC(ret) && offset < words.length(); offset += char_len) {
    if (OB_FAIL(ObCharset::first_valid_char(coll_type_,
                                            words.ptr() + offset,
                                            words.length() - offset,
                                            char_len))) {
    } else if (OB_FAIL(match_with_hit(ObString(char_len, words.ptr() + offset), hit, hit))) {
    } else if (hit.is_match() && offset + char_len == words.length()) {
      is_match = true;
      break;
    } else if (hit.is_unmatch()) {
      is_match = false;
      break;
    } else {
      // continue to match
    }
  }
  return ret;
}

int ObFTCacheDict::match_with_hit(const ObString &single_word,
                                  const ObDATrieHit &last_hit,
                                  ObDATrieHit &hit) const
{
  int ret = OB_SUCCESS;
  ObFTWordCode code = 0;
  bool found = false;
  if (OB_FAIL(resolve_char(single_word, code, found))) {
  } else {
    ret = reader_.match_with_code(single_word.length(), code, found, last_hit, hit);
  }
  return ret;
}

int ObFTCacheDict::make_and_fetch_cache_entry(const ObFTDictDesc &desc,
                                              ObFTDAT *dat_buff,
                                              const size_t buff_size,
                                              const int32_t range_id,
                                              const ObDictCacheValue *&value,
                                              ObKVCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  ObDictCache &cache = ObDictCache::get_instance();
  uint64_t name = static_cast<uint64_t>(desc.type_);
  const ObDictCacheKey put_key(name, desc.type_, range_id);
  const ObDictCacheValue put_value(dat_buff);
  if (OB_FAIL(cache.put_and_fetch_dict(put_key, put_value, value, handle))) {
  }
  return ret;
}

} //  namespace storage
} //  namespace oceanbase
