#pragma once

#include "global.h"


class DABlockQueue
{
    private:
        //std::queue<BaseQuery*> q;
        std::list<BaseQuery*> q;
        size_t cap;
        pthread_mutex_t mutex;
        pthread_cond_t full;
        pthread_cond_t empty;
        void LockQueue();
        void UnlockQueue();
        void ProductWait();
        void ConsumeWait();
        void NotifyProduct();
        void NotifyConsume();
        bool IsEmpty();
        bool IsFull();
    public:
        DABlockQueue(size_t _cnt);
        ~DABlockQueue();
        void push_data(BaseQuery* data);
        BaseQuery* pop_data();
};
