// Copyright (c) 2025 The LevelDB Authors. All rights reserved.
//
// Tests for the Transaction class using the LevelDB C++ API.

#include "gtest/gtest.h"
#include "db/db_impl.h"
#include "db/filename.h"
#include "db/transaction.h"
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/write_batch.h"
#include "util/testutil.h"

namespace leveldb {

class TransactionTest : public testing::Test {
 public:
  TransactionTest() : env_(Env::Default()), db_(nullptr) {
    dbname_ = testing::TempDir() + "transaction_test";
    DestroyDB(dbname_, Options());
    Open();
  }

  ~TransactionTest() {
    Close();
    DestroyDB(dbname_, Options());
  }

  DBImpl* dbfull() const { return reinterpret_cast<DBImpl*>(db_); }
  Env* env() const { return env_; }
  DB* db() const { return db_; }

  void Close() {
    delete db_;
    db_ = nullptr;
  }

  Status OpenWithStatus(Options* options = nullptr) {
    Close();
    Options opts;
    if (options != nullptr) {
      opts = *options;
    } else {
      opts.reuse_logs = true;  // TODO(sanjay): test both ways
      opts.create_if_missing = true;
    }
    if (opts.env == nullptr) {
      opts.env = env_;
    }
    return DB::Open(opts, dbname_, &db_);
  }

  void Open(Options* options = nullptr) {
    ASSERT_LEVELDB_OK(OpenWithStatus(options));
  }

  Status Put(const std::string& k, const std::string& v) {
    return db_->Put(WriteOptions(), k, v);
  }

  std::string Get(const std::string& k, const Snapshot* snapshot = nullptr) {
    std::string result;
    Status s = db_->Get(ReadOptions(), k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

 private:
  std::string dbname_;
  Env* env_;
  DB* db_;
};

TEST_F(TransactionTest, SimpleCommit) {
  Transaction txn(db());
  txn.Put("key1", "value1");
  ASSERT_TRUE(txn.Commit().ok());
  ASSERT_EQ(Get("key1"), "value1");
}

TEST_F(TransactionTest, ReadYourOwnWrites) {
  Transaction txn(db());
  txn.Put("mykey", "inprogress");
  std::string value;
  ASSERT_TRUE(txn.Get(ReadOptions(), "mykey", &value).ok());
  ASSERT_EQ(value, "inprogress");
  ASSERT_TRUE(txn.Commit().ok());
}

TEST_F(TransactionTest, SnapshotIsolationConflict) {
  // Txn1 starts, then Txn2 starts and commits
  Transaction txn1(db());
  Transaction txn2(db());
  txn2.Put("k1", "txn2val");
  ASSERT_TRUE(txn2.Commit().ok());
  // Now txn1 tries to write k1 and commit - should be a conflict
  txn1.Put("k1", "txn1val");
  Status s = txn1.Commit();
  ASSERT_FALSE(s.ok());

  // The final value for k1 should be txn2val
  ASSERT_EQ(Get("k1"), "txn2val");
}

TEST_F(TransactionTest, AbortRollback) {
  Transaction txn(db());
  txn.Put("k", "v");
  txn.Abort();
  Status s = txn.Commit();
  ASSERT_FALSE(s.ok());
  ASSERT_EQ(Get("k"), "NOT_FOUND");
}

}  // namespace leveldb
