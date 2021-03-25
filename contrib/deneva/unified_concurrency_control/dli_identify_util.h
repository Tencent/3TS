#pragma once
#include <vector>
#include <iostream>
#include <string>
#include "row_prece.h"

namespace ttts {

class Path {
 public:
    Path();
    Path(std::vector<PreceInfo>&& preces);
    Path(const PreceInfo& prece);
    Path(PreceInfo&& prece);
    Path(const Path&) = default;
    Path(Path&&) = default;
    Path& operator=(const Path&) = default;
    Path& operator=(Path&&) = default;

    bool operator<(const Path& p) const;

    Path& operator+=(const Path& p);

    Path operator+(const Path& p) const;

    friend std::ostream& operator<<(std::ostream& os, const Path& path);

    std::string ToString() const;

    bool Passable() const { return !preces_.empty(); }

    const std::vector<PreceInfo>& Preces() const { return preces_; }

    bool IsCycle() const {
        return preces_.size() >= 2 && preces_.front().from_txn_id() == preces_.back().to_txn_id();
    }

    std::vector<PreceInfo> preces_;
};

template <bool is_commit> Path DirtyCycle(TxnNode& txn_node);

}
