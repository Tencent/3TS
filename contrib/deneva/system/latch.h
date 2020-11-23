
#ifndef _LATCH_H_
#define _LATCH_H_

#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <random>
#include <chrono>

// static std::default_random_engine e(time(0));
// static std::uniform_int_distribution<unsigned> u(0, 9);

class Latch {
public:
    Latch(int limit) {
        this->limit_ = limit;
        this->lt_ = limit;
    }

    virtual void await() = 0;
    virtual void count_down() = 0;
    virtual int get_unarrived() = 0;
    virtual void reset() = 0;
protected:
    int limit_;
    int lt_;
};

class CountDownLatch : public Latch {
public:
    using Latch::Latch;

void await() override {
    std::unique_lock<std::mutex> lk(mtx_);
    cv_.wait(lk, [&]{
        //std::cout << "limit_: " << limit_ << std::endl;
        return (limit_ == 0);
        });
}

void count_down() override {
    std::unique_lock<std::mutex> lk(mtx_);
    limit_--;
    cv_.notify_all();
}

int get_unarrived() override {
    std::unique_lock<std::mutex> lk(mtx_);
    return limit_;
}

void reset() override {
    std::unique_lock<std::mutex> lk(mtx_);
    limit_ = lt_;
}
private:
    std::mutex mtx_;
    std::condition_variable cv_;
};

#endif
