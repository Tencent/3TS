#ifndef LOGGER_H
#define LOGGER_H

#include "global.h"
#include "helper.h"
#include "concurrentqueue.h"
#include <set>
#include <queue>
#include <fstream>

enum LogRecType {
    LRT_INVALID = 0,
    LRT_INSERT,
    LRT_UPDATE,
    LRT_DELETE,
    LRT_TRUNCATE
};
enum LogIUD {
    L_INSERT = 0,
    L_UPDATE,
    L_DELETE,
    L_NOTIFY
};

// Command log record (logical logging)
struct CmdLogRecord {
    uint32_t checksum;
    uint64_t lsn;
    LogRecType type;
    uint64_t txn_id; // transaction id
#if WORKLOAD==TPCC
    TPCCTxnType txntype;
#elif WORKLOAD==YCSB
    //YCSBTxnType txntype;
#endif
    uint32_t params_size;
    char * params; // input parameters for this transaction type
};

// ARIES-style log record (physiological logging)
struct AriesLogRecord {
    void init() {
        checksum = 0;
        lsn = UINT64_MAX;
        type = LRT_UPDATE;
        iud = L_UPDATE;
        txn_id = UINT64_MAX;
        table_id = 0;
        key = UINT64_MAX;
    }

    uint32_t checksum;
    uint64_t lsn;
    LogRecType type;
    LogIUD iud;
    uint64_t txn_id; // transaction id
    //uint32_t partid; // partition id
    uint32_t table_id; // table being updated
    uint64_t key; // primary key (determines the partition ID)
};

class LogRecord {
public:
    //LogRecord();
    LogRecType getType() { return rcd.type; }
    void copyRecord( LogRecord * record);
    // TODO: compute a reasonable checksum
    uint64_t computeChecksum() {
        return (uint64_t)rcd.txn_id;
    };
#if LOG_COMMAND
    CmdLogRecord rcd;
#else
    AriesLogRecord rcd;
#endif
private:
    bool isValid;

};

class Logger {
public:
    void init(const char * log_file);
    void release();
    void flushBufferCheck(uint64_t thd_id);
    LogRecord * createRecord(LogRecord* record);

    LogRecord * createRecord(
        //LogRecType type,
        uint64_t txn_id, LogIUD iud,
        //uint64_t partid,
        uint64_t table_id, uint64_t key);
    void enqueueRecord(LogRecord* record);
    void processRecord(uint64_t thd_id);
    void writeToBuffer(uint64_t thd_id,char * data, uint64_t size);
    void writeToBuffer(uint64_t thd_id,LogRecord* record);
    uint64_t reserveBuffer(uint64_t size);
    void notify_on_sync(uint64_t txn_id);
private:
    pthread_mutex_t mtx;
    uint64_t lsn;

    void flushBuffer(uint64_t thd_id);
    std::queue<LogRecord*> log_queue;
    const char * log_file_name;
    std::ofstream log_file;
    uint64_t aries_write_offset;
    std::set<uint64_t> txns_to_notify;
    uint64_t last_flush;
    uint64_t log_buf_cnt;
};


#endif
