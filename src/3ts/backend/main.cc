/*
 * Tencent is pleased to support the open source community by making 3TS available.
 *
 * Copyright (C) 2020 THL A29 Limited, a Tencent company.  All rights reserved. The below software
 * in this distribution may have been modified by THL A29 Limited ("Tencent Modifications"). All
 * Tencent Modifications are Copyright (C) THL A29 Limited.
 *
 * Author: williamcliu@tencent.com
 *
 */
#include <gflags/gflags.h>

#include <iostream>

#include "history/parse_config.h"

DEFINE_string(conf_path, "config/config.cfg", "program configure file.");

using namespace ttts;

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::cout << "FLAGS_conf_path:" << FLAGS_conf_path << std::endl;

  try {
    ReadAndRun(FLAGS_conf_path);
  } catch (const libconfig::SettingNotFoundException &nfex) {
    std::cerr << "setting no found" << std::endl;
  } catch (char const *str) {
    std::cerr << str << std::endl;
  } catch (std::string str) {
    std::cerr << str << std::endl;
  } catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
  }

  return 0;
}
