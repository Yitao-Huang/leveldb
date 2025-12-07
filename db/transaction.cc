// Copyright (c) 2025 The LevelDB Authors. All rights reserved.
//
// Implementation for the C++ snapshot-isolated Transaction API for LevelDB.

#include "db/transaction.h"

namespace leveldb {

Transaction::Transaction(DB* db)
    : db_(db),
      snapshot_(nullptr),
      committed_(false),
      aborted_(false) {
  if (db_ == nullptr) {
    aborted_ = true;
    return;
  }
  
  // Acquire a snapshot at transaction begin for snapshot isolation
  snapshot_ = db_->GetSnapshot();
  if (snapshot_ == nullptr) {
    aborted_ = true;
  }
}

Transaction::~Transaction() {
  if (snapshot_ && db_) {
    db_->ReleaseSnapshot(snapshot_);
  }
}

Status Transaction::CheckActive() const {
  if (aborted_) {
    return Status::InvalidArgument("Transaction has been aborted");
  }
  if (committed_) {
    return Status::InvalidArgument("Transaction has been committed");
  }
  if (db_ == nullptr) {
    return Status::InvalidArgument("Database is null");
  }
  if (snapshot_ == nullptr) {
    return Status::InvalidArgument("Snapshot is null");
  }
  return Status::OK();
}

Status Transaction::Get(const ReadOptions& options, const Slice& key, 
                        std::string* value) {
  Status s = CheckActive();
  if (!s.ok()) {
    return s;
  }

  std::string key_str = key.ToString();
  
  // First check the write buffer for read-your-own-writes
  auto it = write_buffer_.find(key_str);
  if (it != write_buffer_.end()) {
    if (it->second.is_tombstone) {
      // Key was deleted in this transaction
      return Status::NotFound("Key deleted in transaction");
    }
    *value = it->second.value;
    return Status::OK();
  }

  // Track this read for conflict detection
  read_set_.insert(key_str);

  // Read from database at snapshot
  ReadOptions tx_options = options;
  tx_options.snapshot = snapshot_;
  return db_->Get(tx_options, key, value);
}

Status Transaction::Put(const Slice& key, const Slice& value) {
  Status s = CheckActive();
  if (!s.ok()) {
    return s;
  }

  std::string key_str = key.ToString();
  std::string value_str = value.ToString();

  // Update write buffer
  write_buffer_[key_str] = BufferEntry(value_str);
  
  // Add to write batch
  write_batch_.Put(key, value);
  
  return Status::OK();
}

Status Transaction::Delete(const Slice& key) {
  Status s = CheckActive();
  if (!s.ok()) {
    return s;
  }

  std::string key_str = key.ToString();

  // Mark as tombstone in write buffer
  write_buffer_[key_str] = BufferEntry::Tombstone();
  
  // Add to write batch
  write_batch_.Delete(key);
  
  return Status::OK();
}

Status Transaction::ValidateReadSet() {
  // Check that all keys we read haven't been modified
  for (const auto& key_str : read_set_) {
    // Skip keys we also wrote (they're allowed to change)
    if (write_buffer_.find(key_str) != write_buffer_.end()) {
      continue;
    }

    Slice key(key_str);
    std::string value_at_snapshot;
    std::string value_current;

    // Read value at our snapshot
    ReadOptions snap_opts;
    snap_opts.snapshot = snapshot_;
    Status snap_status = db_->Get(snap_opts, key, &value_at_snapshot);

    // Read current value (latest committed state)
    ReadOptions current_opts;
    Status current_status = db_->Get(current_opts, key, &value_current);

    // Check for conflicts
    bool existed_at_snapshot = snap_status.ok();
    bool exists_now = current_status.ok();

    if (existed_at_snapshot != exists_now) {
      // Key was added or deleted
      return Status::Corruption(
          "Read-write conflict: key modified by another transaction", key_str);
    }

    if (existed_at_snapshot && exists_now && value_at_snapshot != value_current) {
      // Key value changed
      return Status::Corruption(
          "Read-write conflict: key modified by another transaction", key_str);
    }
  }

  return Status::OK();
}

Status Transaction::ValidateWriteSet() {
  // Check that all keys we're writing haven't been modified
  for (const auto& entry : write_buffer_) {
    const std::string& key_str = entry.first;
    Slice key(key_str);
    std::string value_at_snapshot;
    std::string value_current;

    // Read value at our snapshot
    ReadOptions snap_opts;
    snap_opts.snapshot = snapshot_;
    Status snap_status = db_->Get(snap_opts, key, &value_at_snapshot);

    // Read current value
    ReadOptions current_opts;
    Status current_status = db_->Get(current_opts, key, &value_current);

    // Check for conflicts
    bool existed_at_snapshot = snap_status.ok();
    bool exists_now = current_status.ok();

    if (existed_at_snapshot != exists_now) {
      // Key was added or deleted by another transaction
      return Status::Corruption(
          "Write-write conflict: key modified by another transaction", key_str);
    }

    if (existed_at_snapshot && exists_now && value_at_snapshot != value_current) {
      // Key value changed by another transaction
      return Status::Corruption(
          "Write-write conflict: key modified by another transaction", key_str);
    }
  }

  return Status::OK();
}

Status Transaction::Commit() {
  Status s = CheckActive();
  if (!s.ok()) {
    return s;
  }

  // If no writes, just mark as committed
  if (write_buffer_.empty()) {
    committed_ = true;
    return Status::OK();
  }

  // Acquire global commit lock to prevent race conditions during validation
  // This ensures atomicity of validation + commit
  std::lock_guard<std::mutex> lock(TransactionManager::Instance().GetCommitLock());

  // Validate read set (detect if any read values changed)
  s = ValidateReadSet();
  if (!s.ok()) {
    return s;
  }

  // Validate write set (detect write-write conflicts)
  s = ValidateWriteSet();
  if (!s.ok()) {
    return s;
  }

  // All validations passed, apply writes
  WriteOptions write_opts;
  s = db_->Write(write_opts, &write_batch_);
  
  if (s.ok()) {
    committed_ = true;
  }
  
  return s;
}

Status Transaction::Abort() {
  if (committed_) {
    return Status::InvalidArgument("Cannot rollback: transaction already committed");
  }
  
  if (aborted_) {
    return Status::OK();  // Already aborted
  }

  aborted_ = true;
  read_set_.clear();
  write_buffer_.clear();
  write_batch_.Clear();
  
  return Status::OK();
}

}  // namespace leveldb