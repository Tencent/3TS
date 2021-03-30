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

  while (true) {
    std::cout << "Please type a history for anomaly identification." << std::endl;
    std::cout << "3ts> ";
    std::string text = "";
    std::getline(std::cin, text);
    if ("help" == text || "h" == text) {
      Printer::PrintHelpInfo();
    } else if ("q" == text || "quit" == text) {
      break;
    } else if (text.find("\\d") != text.npos || text.find("definition") != text.npos) {
      const auto index_space = text.find_first_of(" ");
      if (index_space != text.npos) {
        std::string input = text.substr(index_space);
        Printer::TrimSpace(input);
        if (printer.InfoMap().count(input) == 0) {
          Printer::Print("Unknown Definition");
        } else {
          Printer::Print(printer.InfoMap()[input]);
        }
      } else {
        Printer::Print("Please check input format, such as \\d History");
      }
    } else if (text.find("\\a") != text.npos || text.find("anomaly") != text.npos) {
      const auto index_space = text.find_first_of(" ");
      if (index_space != text.npos) {
        std::string input = text.substr(index_space);
        Printer::TrimSpace(input);
        if (printer.AnomalyMap().count(input) == 0) {
          Printer::Print("Unknown Anomaly");
        } else {
          std::cout << "Type  SubType  History Example                     Definition" << std::endl;
          std::cout << "-------------------------------------------------------------" << std::endl;
          Printer::Print(printer.AnomalyMap()[input]);
        }
      } else {
        Printer::Print("Please check input format, such as \\a Dirty Write");
      }
    } else if (text.find("\\g") != text.npos || text.find("algorithm") != text.npos) {
      const auto index_space = text.find_first_of(" ");
      if (index_space != text.npos) {
        std::string input = text.substr(index_space);
        Printer::TrimSpace(input);
        if ("DLI_IDENTIFY_CYCLE" == input) {
          printer.SetAlg(AlgType::DLI_IDENTIFY_CYCLE);
        } else if ("DLI_IDENTIFY_CHAIN" == input) {
          printer.SetAlg(AlgType::DLI_IDENTIFY_CHAIN);
        } else if ("All" == input) {
          printer.SetAlg(AlgType::ALL);
        } else {
          Printer::Print("Unknown Algorithm");
        }
      } else {
        Printer::Print("Please check input format, such as \\g DLI_IDENTIFY_CYCLE");
      }
    } else if (text.find("\\t") != text.npos || text.find("table") != text.npos) {
      std::vector<std::string> anomaly_list = Printer::InitAnomalyList();
      draw();
      std::cout << "Please tell me how you feel now:" << std::endl;
      std::string feel;
      std::cin >> feel;
      std::cout << "I knonw you are so " << feel << ", but i want to ask you a question:" << std::endl;
      std::cout << "Do you want to join the TDSQL team? yes/no" << std::endl;
      std::string y = "no";
      while ("yes" != y) {
        std::cin >> y;
        if ("yes" == y) {
          break;
        } else {
          std::cout << "Please think it over before you answer!" << std::endl;
        }
      }
      std::cout << "Welcome to our team! You will receive a book of martial arts secrets!" << std::endl;
      std::cout << "Whether to get it now? yes/no" << std::endl;
      std::string ret;
      std::cin >> ret;
      if ("yes" == ret) {
        Printer::PrintAnomalyTableInfo(anomaly_list);
      } else {
        std::cout << "You've lost a chance to get stronger." << std::endl;
      }
    } else if (text.find("\\A") != text.npos || text.find("authors") != text.npos) {
      Printer::PrintAuthorInfo();
    } else if (text.find("R") != text.npos || text.find("W") != text.npos) {
        if (printer.Alg() == AlgType::DLI_IDENTIFY_CYCLE) {
          checker.ExecDLI(text);
        } else if (printer.Alg() == AlgType::DLI_IDENTIFY_CHAIN) {
          // to do
        } else if (printer.Alg() == AlgType::ALL) {
          // to do
        } else {
          Printer::Print("alg has Unknown value");
        }
    }
  }
  return 0;
}
