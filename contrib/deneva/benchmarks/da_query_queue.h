/*#include <boost/lockfree/queue.hpp>


class DAQueryQueue
{
    public:
        bool push();
        bool pop();
        bool is_full();
        bool is_empty();
    private:
        boost::lockfree::queue<int, boost::lockfree::fixed_sized<true>> q{100};
        uint64_t size;
};*/
