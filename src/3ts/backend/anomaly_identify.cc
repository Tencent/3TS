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
#include "anomaly_identify.h"

int main() {
  Printer printer;
  Checker checker;
  printer.PrintStartInfo();
  std::unordered_map<std::string, std::string> info_map = printer.InitInfoMap();
  std::vector<std::string> anomaly_list = printer.InitAnomalyList();
  std::unordered_map<std::string, std::string> anomaly_map = printer.InitAnomalyMap();
  while (true) {
    std::cout << "Please type a history for anomaly identification." << std::endl;
    std::cout << "3ts> ";
    std::string text = "";
    std::getline(std::cin, text);
    auto index_d1 = text.find("\\d");
    auto index_d2 = text.find("definition");
    auto index_a1 = text.find("\\a");
    auto index_a2 = text.find("anomaly");
    auto index_g1 = text.find("\\g");
    auto index_g2 = text.find("algorithm");
    auto index_t1 = text.find("\\t");
    auto index_t2 = text.find("table");
    auto index_A1 = text.find("authors");
    auto index_A2 = text.find("A");
    auto index_space = text.find_first_of(" ");
    if ("help" == text || "h" == text) {
        printer.PrintHelpInfo();
    } else if ("\\q" == text || "quit" == text) {
      break;
    } else if (index_d1 != text.npos || index_d2 != text.npos) {
      if (index_space != text.npos) {
        std::string input = text.substr(index_space);
        printer.TrimSpace(input);
        if (info_map.count(input) == 0) {
          printer.Print("Unknonw Definition");
        } else {
          printer.Print(info_map[input]);
        }
      } else {
        printer.Print("Please check input format, such as \\d History");
      }
    } else if (index_a1 != text.npos || index_a2 != text.npos) {
      if (index_space != text.npos) {
        std::string input = text.substr(index_space);
        printer.TrimSpace(input);
        if (anomaly_map.count(input) == 0) {
          printer.Print("Unknown Anomaly");
        } else {
          printer.Print(anomaly_map[input]);
        }
      } else {
        printer.Print("Please check input format, such as \\a Dirty Write");
      }
    } else if (index_g1 != text.npos || index_g2 != text.npos) {
      if (index_space != text.npos) {
        std::string input = text.substr(index_space);
        printer.TrimSpace(input);
        if ("DLI" == input) {
          printer.SetAlg(0);
        } else if ("CCA" == input) {
          printer.SetAlg(1);
        } else if ("All" == input) {
          printer.SetAlg(2);
        } else {
          printer.Print("Unknonw Algorithm");
        }
      } else {
        printer.Print("Please check input format, such as \\g DLI");
      }
    } else if (index_t1 != text.npos || index_t2 != text.npos) {
      if (index_space != text.npos) {
        std::string input = text.substr(index_space);
        printer.TrimSpace(input);
        if ("Anomalies" == input) {
          printer.PrintAnomalyTableInfo(anomaly_list);
        } else {
          printer.Print("Unknonw Table");
        }
      }
    } else if (index_A1 != text.npos || index_A2 != text.npos) {
      printer.PrintAuthorInfo();
    } else {
        if (printer.Alg() == 0) {
          checker.ExecDLI(text);
        } else if (printer.Alg() == 1) {
          // to do
        } else if (printer.Alg() == 2) {
         // to do
        } else {
          printer.Print("alg has Unknonw value");
        }
    }
  }
  return 0;
}
