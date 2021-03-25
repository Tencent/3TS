#include "alg_dli_identify_cycle.h"
#include "alg_dli_identify_merge.h"
#include "txn_dli_identify.h"

int main() {

    ttts::AlgManager<ttts::UniAlgs::UNI_DLI_IDENTIFY_MERGE, int> alg;
    ttts::RowManager<ttts::UniAlgs::UNI_DLI_IDENTIFY_MERGE, int> row(1, 1);
    ttts::TxnManager<ttts::UniAlgs::UNI_DLI_IDENTIFY_MERGE, int> txn(1);

    row.Read(txn);
    row.Prewrite(1, txn);
    row.Write(1, txn);
    row.Revoke(1, txn);


    alg.Validate(txn);
    alg.Commit(txn);
    alg.Abort(txn);
    alg.CheckConcurrencyTxnEmpty();

}
