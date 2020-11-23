/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef _IOTHREAD_H_
#define _IOTHREAD_H_

#include "global.h"
#include "message.h"
class Workload;
class MessageThread;

class InputThread : public Thread {
public:
    RC             run();
    RC  client_recv_loop();
    RC  server_recv_loop();
    void  check_for_init_done();
    void setup();

    bool fakeprocess(Message * msg);
    TxnManager * txn_man;
};

class OutputThread : public Thread {
public:
    RC             run();
    void setup();
    MessageThread * messager;
};

#endif
