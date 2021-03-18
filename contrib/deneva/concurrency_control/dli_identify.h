#ifndef _DLI_IDENTIFY_H_
#define _DLI_IDENTIFY_H_

#include "config.h"
#include "../system/global.h"
#include "concurrency_control/row_prece.h"
#include <mutex>

#define CYCLE_ORDER 1

class PreceInfo;

class Path {
 public:
    Path() {}
    Path(std::vector<PreceInfo>&& preces) : preces_(
#if CYCLE_ORDER
        std::move(preces))
#else
        (sort(preces, std::greater<PreceInfo>()), std::move(preces)))
#endif
    {}
    Path(const PreceInfo& prece) : preces_{prece} {}
    Path(PreceInfo&& prece) : preces_{std::move(prece)} {}
    Path(const Path&) = default;
    Path(Path&&) = default;
    Path& operator=(const Path&) = default;
    Path& operator=(Path&&) = default;

    bool operator<(const Path& p) const {
        // impassable has the greatest weight
        if (!passable()) {
            return false;
        }
        if (!p.passable()) {
            return true;
        }
#if CYCLE_ORDER
        return preces_.size() < p.preces_.size();
#else
        return std::lexicographical_compare(preces_.begin(), preces_.end(), p.preces_.begin(), p.preces_.end());
#endif
    }

    Path operator+(const Path& p) const {
        if (!passable() || !p.passable()) {
            return {};
        }
#if CYCLE_ORDER
        assert(preces_.back().to_txn_id() == p.preces_.front().from_txn_id());
        std::vector<PreceInfo> preces = preces_;
        for (const auto& prece : p.preces_) {
            preces.emplace_back(prece);
        }
#else
        std::vector<PreceInfo> preces;
        std::merge(preces_.begin(), preces_.end(), p.preces_.begin(), p.preces_.end(), std::back_inserter(preces), std::greater<PreceInfo>());
#endif
        return preces;
    }

    friend std::ostream& operator<<(std::ostream& os, const Path& path) {
        if (path.preces_.empty()) {
            os << "Empty path";
        } else {
            std::copy(path.preces_.begin(), path.preces_.end(), std::ostream_iterator<PreceInfo>(os, ", "));
        }
        return os;
    }

    bool passable() const { return !preces_.empty(); }

    const std::vector<PreceInfo>& preces() const { return preces_; }

    bool is_cycle() const {
        return preces_.size() >= 2 && preces_.front().from_txn_id() == preces_.back().to_txn_id();
    }

 private:
    template <typename Container, typename Compare>
    static void sort(Container&& container, Compare&& comp) {
        if (!std::is_sorted(container.begin(), container.end(), comp)) {
            std::sort(container.begin(), container.end(), comp);
        }
    }
    std::vector<PreceInfo> preces_;
};

template <>
class AlgManager<DLI_IDENTIFY>
{
  public:
    void init();
    RC validate(TxnManager* txn);
    void finish(RC rc, TxnManager* txn);

  private:
    std::vector<std::shared_ptr<TxnNode>> refresh_and_lock_txns_();
    std::vector<std::vector<Path>> init_path_matrix_(
        const std::vector<std::shared_ptr<TxnNode>>& concurrent_txns) const;
    void update_path_matrix_(std::vector<std::vector<Path>>& matrix) const;
    Path min_cycle_(std::vector<std::vector<Path>>& matrix, const uint64_t this_idx) const;
    template <bool is_commit> Path dirty_cycle_(TxnNode& txn_node) const;

    std::mutex m_;
    std::list<std::weak_ptr<TxnNode>> txns_;
};

#endif
