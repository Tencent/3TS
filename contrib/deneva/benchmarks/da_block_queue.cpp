
#include "da_block_queue.h"


void DABlockQueue::LockQueue()  //queue lock
{
    pthread_mutex_lock(&mutex);
}
void DABlockQueue::UnlockQueue()
{
    pthread_mutex_unlock(&mutex);
}
void DABlockQueue::ProductWait()
{
    pthread_cond_wait(&full,&mutex);
}
void DABlockQueue::ConsumeWait()
{
    pthread_cond_wait(&empty,&mutex);
}
void DABlockQueue::NotifyProduct()
{
    pthread_cond_signal(&full);
}
void DABlockQueue::NotifyConsume()
{
    pthread_cond_signal(&empty);
}
bool DABlockQueue::IsEmpty()
{
    return (q.size() == 0 ? true : false);
}
bool DABlockQueue::IsFull()
{
    return (q.size() == cap ? true : false);
}

DABlockQueue::DABlockQueue(size_t _cap = 50):cap(_cap)
{
    pthread_mutex_init(&mutex,NULL);
    pthread_cond_init(&full,NULL);
    pthread_cond_init(&empty,NULL);
}
DABlockQueue::~DABlockQueue()
{
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&full);
    pthread_cond_destroy(&empty);
}
void DABlockQueue::push_data(BaseQuery* data)
{
    LockQueue();
    while(IsFull())
    {
        NotifyConsume();
        std::cout<<"queue full,notify consume data,product stop!!"<<std::endl;
        ProductWait();
    }

    q.push_back(data);
    NotifyConsume();
    UnlockQueue();
}
BaseQuery* DABlockQueue::pop_data()
{
    BaseQuery* data;
    LockQueue();
    while(IsEmpty())
    {
        NotifyProduct();
        std::cout<<"queue empty,notify product data,consume stop!!"<<std::endl;
        list<BaseQuery*>().swap(q);
        ConsumeWait();
    }

    data = q.front();
    q.pop_front();
    NotifyProduct();
    UnlockQueue();
    return data;
}

