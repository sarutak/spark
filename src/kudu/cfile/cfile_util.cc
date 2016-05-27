// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/cfile/cfile_util.h"

#include <glog/logging.h>
#include <algorithm>
#include <string>

#include "kudu/cfile/cfile_reader.h"
#include "kudu/util/env.h"
#include "kudu/util/mem_tracker.h"

namespace kudu {
namespace cfile {

using std::string;

static const int kBufSize = 1024*1024;

Status DumpIterator(const CFileReader& reader,
                    CFileIterator* it,
                    std::ostream* out,
                    const DumpIteratorOptions& opts,
                    int indent) {

  Arena arena(8192, 8*1024*1024);
  uint8_t buf[kBufSize];
  const TypeInfo *type = reader.type_info();
  size_t max_rows = kBufSize/type->size();
  uint8_t nulls[BitmapSize(max_rows)];
  ColumnBlock cb(type, reader.is_nullable() ? nulls : nullptr, buf, max_rows, &arena);

  string strbuf;
  size_t count = 0;
  while (it->HasNext()) {
    size_t n = opts.nrows == 0 ? max_rows : std::min(max_rows, opts.nrows - count);
    if (n == 0) break;

    RETURN_NOT_OK(it->CopyNextValues(&n, &cb));

    if (opts.print_rows) {
      if (reader.is_nullable()) {
        for (size_t i = 0; i < n; i++) {
          strbuf.append(indent, ' ');
          const void *ptr = cb.nullable_cell_ptr(i);
          if (ptr != nullptr) {
            type->AppendDebugStringForValue(ptr, &strbuf);
          } else {
            strbuf.append("NULL");
          }
          strbuf.push_back('\n');
        }
      } else {
        for (size_t i = 0; i < n; i++) {
          strbuf.append(indent, ' ');
          type->AppendDebugStringForValue(cb.cell_ptr(i), &strbuf);
          strbuf.push_back('\n');
        }
      }
      *out << strbuf;
      strbuf.clear();
    }
    arena.Reset();
    count += n;
  }

  VLOG(1) << "Dumped " << count << " rows";

  return Status::OK();
}

ReaderOptions::ReaderOptions()
  : parent_mem_tracker(MemTracker::GetRootTracker()) {
}

size_t CommonPrefixLength(const Slice& slice_a, const Slice& slice_b) {
  // This implementation is modeled after strings::fastmemcmp_inlined().
  int len = std::min(slice_a.size(), slice_b.size());
  const uint8_t* a = slice_a.data();
  const uint8_t* b = slice_b.data();
  const uint8_t* a_limit = a + len;

  const size_t sizeof_uint64 = sizeof(uint64_t);
  // Move forward 8 bytes at a time until finding an unequal portion.
  while (a + sizeof_uint64 <= a_limit &&
         UNALIGNED_LOAD64(a) == UNALIGNED_LOAD64(b)) {
    a += sizeof_uint64;
    b += sizeof_uint64;
  }

  // Same, 4 bytes at a time.
  const size_t sizeof_uint32 = sizeof(uint32_t);
  while (a + sizeof_uint32 <= a_limit &&
         UNALIGNED_LOAD32(a) == UNALIGNED_LOAD32(b)) {
    a += sizeof_uint32;
    b += sizeof_uint32;
  }

  // Now one byte at a time. We could do a 2-bytes-at-a-time loop,
  // but we're following the example of fastmemcmp_inlined(). The benefit of
  // 2-at-a-time likely doesn't outweigh the cost of added code size.
  while (a < a_limit &&
         *a == *b) {
    a++;
    b++;
  }

  return a - slice_a.data();
}

void GetSeparatingKey(const Slice& left, Slice* right) {
  DCHECK_LE(left.compare(*right), 0);
  size_t cpl = CommonPrefixLength(left, *right);
  right->truncate(cpl == right->size() ? cpl : cpl + 1);
}

} // namespace cfile
} // namespace kudu
