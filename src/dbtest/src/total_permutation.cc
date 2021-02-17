#include <iostream>
#include <algorithm>
#include <vector>
#include <fstream>
std::string file_name = "./long-fork-anomaly.log";

int main() {
    std::vector<std::string> txn_sequence;
    txn_sequence.push_back("1;BEGIN;select * from t1 where k=0;select * from t1 where k=1;commit;");
    txn_sequence.push_back("2;BEGIN;select * from t1 where k=1;select * from t1 where k=0;commit;");
    txn_sequence.push_back("3;BEGIN;update t1 set v=1 where k=0;commit;");
    txn_sequence.push_back("4;BEGIN;update t1 set v=1 where k=1;commit;");
    while (next_permutation(txn_sequence.begin(), txn_sequence.end())) {
        std::ofstream out(file_name, std::ios::app);
        out << txn_sequence[0] << std::endl;
        out << txn_sequence[1] << std::endl;
        out << txn_sequence[2] << std::endl;
        out << txn_sequence[3] << std::endl;
        out << "" << std::endl;
    }
    return 0;
}
