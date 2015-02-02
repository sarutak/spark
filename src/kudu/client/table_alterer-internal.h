// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_CLIENT_TABLE_ALTERER_INTERNAL_H
#define KUDU_CLIENT_TABLE_ALTERER_INTERNAL_H

#include <string>

#include "kudu/client/client.h"
#include "kudu/master/master.pb.h"

namespace kudu {

namespace client {

class KuduTableAlterer::Data {
 public:
  explicit Data(KuduClient* client);
  ~Data();

  KuduClient* client_;

  Status status_;

  master::AlterTableRequestPB alter_steps_;

  MonoDelta timeout_;

  bool wait_;

  DISALLOW_COPY_AND_ASSIGN(Data);
};

} // namespace client
} // namespace kudu

#endif
