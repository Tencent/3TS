# 3TS setup

This document is the instruction for setting up 3TS test.


## Strat
System version: CentOS Linux release 7.2

## Setting environment
Before everything

```
yum -y update
```

CentOS 8 already comes with GCC 8. On CentOS 7, you can install GCC 8 from Developer Toolset. First you need to enable the Software Collections repository:
```
yum install centos-release-scl
```

Then you can install GCC 8 and its C++ compiler:
```
yum install devtoolset-8-gcc devtoolset-8-gcc-c++
```

To switch to this GCC version, use:
```
scl enable devtoolset-8 -- bash
```
add this to /etc/profile make it permanently

To install gflags, gtest, curl, nanomsg:(execute one by one)
```
yum install gflags-devel
yum install gtest-devel
yum install curl-devel
yum install nanomsg-devel
```

Recommend manully configure this version protobuf  3.9.1 (+ version not working). Firstly, find package in github. To build and install the C++ Protocol Buffer runtime and the Protocol Buffer compiler (protoc) execute the following:
```
./configure
make
make check
sudo make install
sudo ldconfig # refresh shared library cache.
```

 

Find libconfig 1.7.2 package in github. 

To create configure for libconfig 1.7.2:
```
aclocal || die "aclocal failed"
automake --add-missing --force-missing --copy --foreign || die "automake failed"
autoreconf || die "autoreconf failed"
```

Easy install by:
```
./configure
make
make check
make install
```

Find Boost 1.7  (1.5 not working) package from https://www.boost.org/users/download/. 

Easy install by:
```
./bootstrap.sh
./b2 install
```

To get protobuf and nanomsg for env:
```
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
```

Some errors:
```
/opt/rh/devtoolset-8/root/usr/libexec/gcc/x86_64-redhat-linux/8/ld: cannot find libasan_preinit.o: No such file or directory
/opt/rh/devtoolset-8/root/usr/libexec/gcc/x86_64-redhat-linux/8/ld: cannot find -lasan
```
To solve:
```
yum install devtoolset-8-libasan-devel
```
To permenantly add: vim ~/.bashrc

## To start the server and debug

To compile the code:
```
make clean
make -j 10
make deps
```

To start server and client:
```
./rundb -nid0
./runcl -nid1
```

For GDB debug:
```
gdb --args ./rundb -nid0
```

Add breakpoints: (format:   b codefile:line_number)
```
b concurrency_control/row_ssi.cpp:331 
```

Extra function：
To install perf
```
yum install perf gawk
```

To install jemalloc:

```
cd jemalloc-5.2.1
./autogen.sh
make
make install
```
To enable, just add "-ljemalloc" to complier Makefile.

To install tbb (install the newest development version):
```
yum install tbb-devel
```