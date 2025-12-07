#ifndef LEVELDB_TRANSACTION_H_
#define LEVELDB_TRANSACTION_H_

#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <string>

namespace leveldb {

// Global commit lock to prevent race conditions during validation
// In production, consider per-key locking or more sophisticated MVCC
class TransactionManager {
 public:
  static TransactionManager& Instance() {
    static TransactionManager instance;
    return instance;
  }
  
  std::mutex& GetCommitLock() { return commit_lock_; }
  
 private:
  std::mutex commit_lock_;
  TransactionManager() = default;
};

class Transaction {
 public:
  explicit Transaction(DB* db);
  ~Transaction();

  // Disable copy and assignment
  Transaction(const Transaction&) = delete;
  Transaction& operator=(const Transaction&) = delete;

  // Read operations
  Status Get(const ReadOptions& options, const Slice& key, std::string* value);

  // Write operations
  Status Put(const Slice& key, const Slice& value);
  Status Delete(const Slice& key);

  // Transaction control
  Status Commit();
  Status Abort();

  // Transaction state
  bool IsCommitted() const { return committed_; }
  bool IsAborted() const { return aborted_; }

 private:
  struct BufferEntry {
    std::string value;
    bool is_tombstone;
    
    BufferEntry() : is_tombstone(false) {}
    explicit BufferEntry(const std::string& v) : value(v), is_tombstone(false) {}
    static BufferEntry Tombstone() {
      BufferEntry entry;
      entry.is_tombstone = true;
      return entry;
    }
  };

  Status CheckActive() const;
  Status ValidateReadSet();
  Status ValidateWriteSet();

  DB* db_;
  const Snapshot* snapshot_;
  bool committed_;
  bool aborted_;

  std::unordered_set<std::string> read_set_; // Track reads for conflict detection
  std::unordered_map<std::string, BufferEntry> write_buffer_; // Track writes with proper tombstone semantics
  
  WriteBatch write_batch_;
};

}  // namespace leveldb

#endif  // LEVELDB_TRANSACTION_H_