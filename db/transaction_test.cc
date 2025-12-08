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
#include <thread>
#include <vector>
#include <atomic>
#include <random>
#include <chrono>
#include <sstream>

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

TEST_F(TransactionTest, ConcurrentTransactionsSingleKey) {
  const int kThreads = 8;
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([this, i, &success_count]() {
      Transaction txn(db());
      txn.Put("concurrent_key", std::to_string(i));
      if (txn.Commit().ok()) {
        success_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  for (auto& t : threads) t.join();

  // At least one transaction should have succeeded.
  ASSERT_GE(success_count.load(std::memory_order_relaxed), 1);

  // The final value should be one of the committed values (not NOT_FOUND).
  std::string val = Get("concurrent_key");
  ASSERT_NE(val, "NOT_FOUND");
}

TEST_F(TransactionTest, ConcurrentMultiKeyTransfers) {
  // Initialize multiple counters
  const int kKeys = 4;
  const int kInitial = 1000;
  const int kThreads = 8;
  const int kIterations = 200;

  std::vector<std::string> keys;
  int64_t expected_total = 0;
  for (int i = 0; i < kKeys; ++i) {
    keys.push_back("acct_" + std::to_string(i));
    Put(keys.back(), std::to_string(kInitial));
    expected_total += kInitial;
  }

  std::atomic<int> committed_count{0};

  auto worker = [&](int seed) {
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int> dist_key(0, kKeys - 1);

    for (int it = 0; it < kIterations; ++it) {
      int a = dist_key(rng);
      int b = dist_key(rng);
      if (a == b) continue;

      Transaction txn(db());
      std::string va, vb;
      Status sa = txn.Get(ReadOptions(), keys[a], &va);
      Status sb = txn.Get(ReadOptions(), keys[b], &vb);
      if (!sa.ok() || !sb.ok()) {
        txn.Abort();
        continue;
      }

      int ia = 0, ib = 0;
      try {
        ia = std::stoi(va);
        ib = std::stoi(vb);
      } catch (...) {
        txn.Abort();
        continue;
      }

      // Transfer 1 unit from a -> b if available
      if (ia >= 1) {
        txn.Put(keys[a], std::to_string(ia - 1));
        txn.Put(keys[b], std::to_string(ib + 1));
      } else {
        // nothing to do
        txn.Abort();
        continue;
      }

      if (txn.Commit().ok()) {
        committed_count.fetch_add(1, std::memory_order_relaxed);
      }
    }
  };

  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    // Seed with time + t to diversify
    int seed = static_cast<int>(std::chrono::high_resolution_clock::now().time_since_epoch().count() + t);
    threads.emplace_back(worker, seed);
  }

  for (auto& th : threads) th.join();

  // Verify total remains the same and no account is negative
  int64_t final_total = 0;
  for (const auto& k : keys) {
    std::string v = Get(k);
    ASSERT_NE(v, "NOT_FOUND");
    int64_t iv = 0;
    try {
      iv = std::stoll(v);
    } catch (...) {
      FAIL() << "Invalid integer value for key " << k << ": " << v;
    }
    ASSERT_GE(iv, 0);
    final_total += iv;
  }

  ASSERT_EQ(final_total, expected_total);
  // Ensure some commits happened
  ASSERT_GT(committed_count.load(std::memory_order_relaxed), 0);
}

}  // namespace leveldb
