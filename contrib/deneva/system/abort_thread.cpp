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

#include "global.h"
#include "helper.h"
#include "thread.h"
#include "abort_thread.h"
#include "abort_queue.h"

void AbortThread::setup() {}

RC AbortThread::run() {
    tsetup();
    printf("Running AbortThread %ld\n",_thd_id);
        while (!simulation->is_done()) {
        heartbeat();
        abort_queue.process(get_thd_id());
    }
    return FINISH;

}


