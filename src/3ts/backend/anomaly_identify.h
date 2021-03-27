/*
 * Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: farrisli@tencent.com
 *
 */
#include <iostream>
#include <map>
#include <unordered_map>
#include "util/generic.h"
#include "cca/conflict_serializable_algorithm.h"

enum class AlgType {
  DLI,
  DLI2,
  ALL
};

class Printer {
public:
  Printer() : anomaly_map_{
    {"DirtyWrite",         "WAT   SDA     'R0a W0a W1a R1a W1a R1a C0 C1'      Wi[xm]...Wj[xm+1]"},
    {"LostUpdate",         "WAT   SDA     'R0a R1a W0a R0a W0a W1a A1 C0'      Ri[xm]...Wj[xm+1]...Wi[xm+2]"},
    {"LostSelfUpdate",     "WAT   SDA     'R0a W0a R0a W1a R0a W1a C0 C1'      Wi[xm]...Wj[xm+1]...Ri[xm+1]"},
    {"Full-Write",         "WAT   SDA     'R0a W0a R1a W1a W0a C0 W1a C1'      Wi[xm]...Wj[xm+1]...Wi[xm+2]"},
    {"Read-WriteSkew1",    "WAT   DDA     'R0a W0a R0a R1b W0b W1a A0 C1'      Ri[xm]...Wj[xm+1]...Wj[yn]...Wi[yn+1]"},
    {"Read-WriteSkew2",    "WAT   DDA     'R0a W0a W0b W1a R1b W0b C0 C1'      Wi[xm]...Wj[xm+1]...Wj[yn]...Ri[yn]"},
    {"Double-WriteSkew1",  "WAT   DDA     'R0a W0a R1a W1a W1b W0b A1 C0'      Wi[xm]...Rj[xm]...Wj[yn]...Wi[yn+1]"},
    {"Double-WriteSkew2",  "WAT   DDA     'R0a W0a R0a W1a W1b R0b C1 C0'      Wi[xm]...Wj[xm+1]...Rj[yn]...Wi[yn+1]"},
    {"Full-WriteSkew",     "WAT   DDA     'R0a W0a W1a W1b W0b R1a A0 C1'      Wi[xm]...Wj[xm+1]...Wj[yn]...Wi[yn+1]"},
    {"StepWAT",            "WAT   MDA     'R0a R0b W1b W2c W0c A0 C1 W2b C2'   ...Wi[xm]...Wj[xm+1]..."},
    {"DirtyRead",          "RAT   SDA     'R0a W0a R1a R0a R1a R0a C1 A0'      Wi[xm]...Rj[xm+1]"},
    {"UnrepeatableRead",   "RAT   SDA     'R0a R1a R0a W1a R0a R1a C1 C0'      Ri[xm]...Wj[xm+1]...Ri[xm+1]"},
    {"IntermediateRead",   "RAT   SDA     'R0a W0a R1a W0a R1a W0a C1 C0'      Wi[xm]...Rj[xm+1]...Wi[xm+2]"},
    {"ReadSkew",           "RAT   DDA     'R0a W0a R1b R0b W0b R1a C0 A1'      Ri[xm]...Wj[xm+1]...Wj[yn]...Ri[yn]"},
    {"ReadSkew2",          "RAT   DDA     'R0a W0a W0b R1b R1a C1 W0a C0'      Wi[xm]...Rj[xm]...Rj[yn]...Wi[yn+1]"},
    {"Write-ReadSkew",     "RAT   DDA     'R0a W0a R1a W1b R0b R1a A0 C1'      Wi[xm]...Rj[xm]...Wj[yn]...Ri[yn]"},
    {"StepRAT",            "RAT   MDA     'R0a R0b W1a R2a R2c W0c C0 C1 C2'   ...Wi[xm]...Rj[xm]..., and not include (...Wii[xm]...Wjj[xm+1]...)"},
    {"WriteSkew",          "IAT   DDA     'R0a R0b R1a W0a R1b W1b C1 C0'      Ri[xm]...Wj[xm+1]...Rj[yn]...Wi[yn+1]"},
    {"StepIAT",            "IAT   MDA     'R0a R0b R1c W1a W2c A1 C2 W0c C0'   ...Ri[xm]...Wj[xm+1]..., and not include (...Wii[xm]...Rjj[xm]...and ...Wiii[xm]...Wjjj[xm+1]...)"}
  }, info_map_{
    {"History",   "The sequence of operations that produces the data anomaly, one history contains several operations."},
    {"Operation", "One operation contains 3 character, such as R0a, first character is operation type, second character is transaction id, third character is data item.\n    Operation Type -> R: Read, W: Write, C: Commit, A: Aort\n    Transaction ID -> such as 0 1 2 ...\n    Data Item -> such as a b c ..."},
    {"WAT",       "There is a WW partial order in the ring."},
    {"RAT",       "There is a WR or WCR partial order in the ring."},
    {"IAT",       "Anomalies other than WAT and RAT."},
    {"SDA",       "Two transactions occur in a single variable."},
    {"DDA",       "Two transactions occur in a double variable."},
    {"MDA",       "Two transactions occur in a multi variable."}
  } {};

  static std::vector<std::string> InitAnomalyList() {
    std::vector<std::string> anomaly_list;
    anomaly_list.emplace_back("Dirty Write          WAT   SDA      'R0a W0a W1a R1a W1a R1a C0 C1'      Wi[xm]...Wj[xm+1]");
    anomaly_list.emplace_back("Lost Update          WAT   SDA      'R0a R1a W0a R0a W0a W1a A1 C0'      Ri[xm]...Wj[xm+1]...Wi[xm+2]");
    anomaly_list.emplace_back("Lost Self Update     WAT   SDA      'R0a W0a R0a W1a R0a W1a C0 C1'      Wi[xm]...Wj[xm+1]...Ri[xm+1]");
    anomaly_list.emplace_back("Full-Write           WAT   SDA      'R0a W0a R1a W1a W0a C0 W1a C1'      Wi[xm]...Wj[xm+1]...Wi[xm+2]");
    anomaly_list.emplace_back("Read-Write Skew 1    WAT   DDA      'R0a W0a R0a R1b W0b W1a A0 C1'      Ri[xm]...Wj[xm+1]...Wj[yn]...Wi[yn+1]");
    anomaly_list.emplace_back("Read-Write Skew 2    WAT   DDA      'R0a W0a W0b W1a R1b W0b C0 C1'      Wi[xm]...Wj[xm+1]...Wj[yn]...Ri[yn]");
    anomaly_list.emplace_back("Double-Write Skew 1  WAT   DDA      'R0a W0a R1a W1a W1b W0b A1 C0'      Wi[xm]...Rj[xm]...Wj[yn]...Wi[yn+1]");
    anomaly_list.emplace_back("Double-Write Skew 2  WAT   DDA      'R0a W0a R0a W1a W1b R0b C1 C0'      Wi[xm]...Wj[xm+1]...Rj[yn]...Wi[yn+1]");
    anomaly_list.emplace_back("Full-Write Skew      WAT   DDA      'R0a W0a W1a W1b W0b R1a A0 C1'      Wi[xm]...Wj[xm+1]...Wj[yn]...Wi[yn+1]");
    anomaly_list.emplace_back("Step WAT             WAT   MDA      'R0a R0b W1b W2c W0c A0 C1 W2b C2'   ...Wi[xm]...Wj[xm+1]...");
    anomaly_list.emplace_back("");
    anomaly_list.emplace_back("Dirty Read           RAT   SDA      'R0a W0a R1a R0a R1a R0a C1 A0'      Wi[xm]...Rj[xm+1]");
    anomaly_list.emplace_back("Unrepeatable Read    RAT   SDA      'R0a R1a R0a W1a R0a R1a C1 C0'      Ri[xm]...Wj[xm+1]...Ri[xm+1]");
    anomaly_list.emplace_back("Intermediate Read    RAT   SDA      'R0a W0a R1a W0a R1a W0a C1 C0'      Wi[xm]...Rj[xm+1]...Wi[xm+2]");
    anomaly_list.emplace_back("Read Skew            RAT   DDA      'R0a W0a R1b R0b W0b R1a C0 A1'      Ri[xm]...Wj[xm+1]...Wj[yn]...Ri[yn]");
    anomaly_list.emplace_back("Read Skew 2          RAT   DDA      'R0a W0a W0b R1b R1a C1 W0a C0'      Wi[xm]...Rj[xm]...Rj[yn]...Wi[yn+1]");
    anomaly_list.emplace_back("Write-Read Skew      RAT   DDA      'R0a W0a R1a W1b R0b R1a A0 C1'      Wi[xm]...Rj[xm]...Wj[yn]...Ri[yn]");
    anomaly_list.emplace_back("Step RAT             RAT   MDA      'R0a R0b W1a R2a R2c W0c C0 C1 C2'   ...Wi[xm]...Rj[xm]..., and not include (...Wii[xm]...Wjj[xm+1]...)");
    anomaly_list.emplace_back("");
    anomaly_list.emplace_back("Write Skew           IAT   DDA      'R0a R0b R1a W0a R1b W1b C1 C0'      Ri[xm]...Wj[xm+1]...Rj[yn]...Wi[yn+1]");
    anomaly_list.emplace_back("Step IAT             IAT   MDA      'R0a R0b R1c W1a W2c A1 C2 W0c C0'   ...Ri[xm]...Wj[xm+1]..., and not include (...Wii[xm]...Rjj[xm]...and ...Wiii[xm]...Wjjj[xm+1]...)");
    return anomaly_list;

  }

  static void Print(const std::string& info) {
      std::cout << info << std::endl;
      std::cout << std::endl;
  }

  static void PrintAnomalyTableInfo(std::vector<std::string>& anomaly_list) {
    std::cout << "DA Name               Type  SubType   History Example                             Definition" << std::endl;
    std::cout << "-------------------------------------------------------------------------------------------------------------" << std::endl;
    for (auto info : anomaly_list) {
        std::cout << info << std::endl;
    }
    std::cout << "-------------------------------------------------------------------------------------------------------------\n" << std::endl;
  }

  static void PrintStartInfo() {
    std::cout << "Welcome to the 3TS-DAI." << std::endl;
    std::cout << "version: 1.0.0" << std::endl;
    std::cout << "Tencent is pleased to support the open source community by making 3TS available.\n" << std::endl;
    std::cout << "For information about 3TS-DAI(Tencent Transaction Processing Tested System-Data Anomaly Identify) products and services, visit:\n    https://github.com/Tencent/3TS\n" << std::endl;
    std::cout << "Type 'help' or 'h' for help.\n" << std::endl;
  }

  static void PrintAuthorInfo() {
    std::cout << "Authors:" << std::endl;
    std::cout << "Chang Liu" << std::endl;
    std::cout << "Yu Li" << std::endl;
    std::cout << "HaiXiang Li" << std::endl;
    std::cout << std::endl;
  }

  static void PrintHelpInfo() {
    std::cout << "List of all 3TS-DAI commands:" << std::endl;
    std::cout << "definition   (\\d) Output precise definitions of History and Anomaly, including History Operation WAT RAT IAT SDA DDA MDA, such as '\\d WAT'" << std::endl;
    std::cout << "algorithm    (\\g) Select the algorithm that identifies the exception, including DLI DLI2 ALL, such as '\\g DLI'" << std::endl;
    std::cout << "anomaly      (\\a) Output history sequence of anomaly, including " << std::endl;
    std::cout << "                  WAT: Dirty Write, Lost Update, Lost Self Update, Full-Write, Read-Write Skew 1, Read-Write Skew 2, Double-Write Skew 1, Double-Write Skew 2, Full-Write Skew, Step WAT" << std::endl;
    std::cout << "                  RAT: Dirty Read, Unrepeatable Read, Intermediate Read, Read Skew, Read Skew 2, Write-Read Skew, Step RAT" << std::endl;
    std::cout << "                  IAT: Write Skew, Step IAT" << std::endl;
    std::cout << "                  such as '\\a Dirty Write'" << std::endl;
    std::cout << "table        (\\t) Output table information, including anomaly, such as '\\t Anomalies'" << std::endl;
    std::cout << "authors      (\\A) Output author information, such as '\\A'" << std::endl;
    std::cout << "quit         (\\q) quit 3TS-DAI, such as '\\q'" << std::endl;
    std::cout << std::endl;
  }

  static void TrimSpace(std::string& str) {
      auto itor = remove_if(str.begin(), str.end(), ::isspace);
      str.erase(itor, str.end());
  }

  AlgType Alg() const { return alg_; };
  void SetAlg(AlgType alg) { alg_ = alg; };

  std::unordered_map<std::string, std::string> InfoMap() const { return info_map_; };
  std::unordered_map<std::string, std::string> AnomalyMap() const { return anomaly_map_; };
private:
  AlgType alg_ = AlgType::DLI;
  std::unordered_map<std::string, std::string> info_map_;
  std::unordered_map<std::string, std::string> anomaly_map_;
};

class Checker {
public:
  void ExecDLI(const std::string& text) {
    ttts::History history;
    std::istringstream is(text);
    if ((is >> history)) {
      ttts::ConflictSerializableAlgorithm<true> alg;
      const std::optional<ttts::AnomalyType> anomaly = alg.GetAnomaly(history, nullptr);
      if (!anomaly.has_value()) {
        std::cout << "No Data Anomaly\n" << std::endl;
      } else {
        const std::vector<std::string> anomaly_info = AnomalyInfo(ttts::ToString(anomaly.value()));
        std::cout << "DLI's Identification Result:" << std::endl;
        std::cout << "Anomaly Type: " << anomaly_info[0] << "\nAnomaly SubType: " << anomaly_info[1] << "\nAnomaly Name: " << anomaly_info[2] << "\nAnomaly Format: " << anomaly_info[3] << "\n" <<   std::endl;
      }
    }
  }

    std::vector<std::string> AnomalyInfo(const std::string& anomaly) {
      auto index = anomaly.find_first_of("_");
      std::vector<std::string> anomaly_info;
      if (index != anomaly.npos) {
        // get anomaly_type
        anomaly_info.emplace_back(anomaly.substr(0, index));
        // get anomaly_subtype
        std::string anomaly_subtype = "";
        std::string m = anomaly.substr(index + 1, 1);
        if ("1" == m) {
          anomaly_subtype = "SDA";
        } else if ("2" == m) {
          anomaly_subtype = "DDA";
        } else {
          anomaly_subtype = "MDA";
        }
        anomaly_info.emplace_back(anomaly_subtype);
        // get anomaly_name
        std::string name = anomaly.substr(index + 3);
        bool is_head = false;
        for (size_t i = 0;i < name.size();i++) {
          if (i == 0) {
            continue;
          } else if (name[i] == '_') {
            name[i] = 32; // '_' to ' '
            is_head = true;
          } else if (is_head == true) {
            is_head = false;
          } else if (name[i] >= 'A' && name[i] <= 'Z') {
              name[i] += 32; // Convert to lowercase
          }
        }
        anomaly_info.emplace_back(name);
        // get anomaly_format
        std::string format = "";
        if ("Dirty Write" == name) {
          format = "Wi[xm]...Wj[xm+1]";
        } else if ("Full Write" == name) {
          format = "Wi[xm]...Wj[xm+1]...Wi[xm+2]";
        } else if ("Lost Self Update" == name) {
          format = "Wi[xm]...Wj[xm+1]...Ri[xm+1]";
        } else if ("Lost Update" == name) {
          format = "Ri[xm]...Wj[xm+1]...Wi[xm+2]";
        } else if ("Double Write Skew 1" == name) {
          format = "Wi[xm]...Rj[xm]...Wj[yn]...Wi[yn+1]";
        } else if ("Double Write Skew 2" == name) {
          format = "Wi[xm]...Wj[xm+1]...Rj[yn]...Wi[yn+1]";
        } else if ("Read Write Skew 1" == name) {
          format = "Ri[xm]...Wj[xm+1]...Wj[yn]...Wi[yn+1]";
        } else if ("Read Write Skew 2" == name) {
          format = "Wi[xm]...Wj[xm+1]...Wj[yn]...Ri[yn]";
        } else if ("Full Write Skew" == name) {
          format = "Wi[xm]...Wj[xm+1]...Wj[yn]...Wi[yn+1]";
        } else if ("WAT_STEP" == anomaly) {
          format = "...Wi[xm]...Wj[xm+1]...";
        } else if ("Dirty Read" == name) {
          format = "Wi[xm]...Rj[xm+1]";
        } else if ("Non-Repeatable Read" == name) {
          format = "Ri[xm]...Wj[xm+1]...Ri[xm+1]";
        } else if ("Intermediate Reads" == name) {
          format = "Wi[xm]...Rj[xm+1]...Wi[xm+2]";
        } else if ("Write Read Skew" == name) {
          format = "Wi[xm]...Rj[xm]...Wj[yn]...Ri[yn]";
        } else if ("DOUBLE_WRITE_SKEW_COMMITTED" == name) {
          format = "Wi[xm]...Rj[xm+1]...Wj[yn]...Wi[yn+1]";
        } else if ("Read Skew" == name) {
          format = "Ri[xm]...Wj[xm+1]...Wj[yn]...Ri[yn]";
        } else if ("Read Skew 2" == name) {
          format = "Wi[xm]...Rj[xm]...Rj[yn]...Wi[yn+1]";
        } else if ("RAT_STEP" == anomaly) {
          format = "...Wi[xm]...Rj[xm]..., and not include (...Wii[xm]...Wjj[xm+1]...)";
        } else if ("LOST_UPDATE_COMMITTED" == name) {
          format = "Ri[xm]...Wj[xm]...Wj[yn]...Wi[yn+1]";
        } else if ("READ_WRITE_SKEW_COMMITTED" == name) {
          format = "Ri[xm]...Wj[xm]...Wj[yn]...Wi[yn+1]";
        } else if ("Write Skew" == name) {
          format = "Ri[xm]...Wj[xm+1]...Rj[yn]...Wi[yn+1]";
        } else if ("IAT_STEP" == anomaly) {
          format = "...Ri[xm]...Wj[xm+1]..., and not include (...Wii[xm]...Rjj[xm]...and ...Wiii[xm]...Wjjj[xm+1]...)";
        }
        anomaly_info.emplace_back(format);
      } else {
        std::cerr << "get AnomalyType failed" << std::endl;
      }
      return anomaly_info;
    }
};

