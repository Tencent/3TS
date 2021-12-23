# 3TS 环境搭建

本文档是安装和配置3TS的说明文档

## 准备
系统版本以CentOS Linux release 7.2为例进行安装

## 配置需要环境
在安装之前yum更新：

```
yum -y update
```

在Centos 7下，可以在Developer Toolset上安装GCC 8。首先需要开启Software Collections repository（CentOS 8 是自带GCC 8可省去这一步）：

```
yum install centos-release-scl
```

然后安装GCC 8和C++编译器：
```
yum install devtoolset-8-gcc devtoolset-8-gcc-c++
```

手动切换到GCC 8:
```
scl enable devtoolset-8 -- bash
```

把切换的代码添加到 /etc/profile 以后每次登录自动生效

安装gflags, gtest, curl, nanomsg：(建议一个个安装，确保都安装成功)
```
yum install gflags-devel
yum install gtest-devel
yum install curl-devel
yum install nanomsg-devel
```

建议手动安装protoobuf 3.9.1（更新版本不兼容），可以从github里找到此版本，下载解压之后，在protobof目录使用以下命令安装：
```
./configure
make
make check
sudo make install
sudo ldconfig # refresh shared library cache.
```

建议手动安装libconfig 1.7.2，从github里找到此版本，下载解压后，首先产生configure文件：
```
aclocal || die "aclocal failed"
automake --add-missing --force-missing --copy --foreign || die "automake failed"
autoreconf || die "autoreconf failed"
```

产生configure文件之后，可以使用以下命令安装：
```
./configure
make
make check
make install
```

下载Boost 1.7 版本 （1.5不兼容），下载链接https://www.boost.org/users/download/。

按照如下命令默认安装：
```
./bootstrap.sh
./b2 install
```

把安装的protobuf和naomsg添加到环境变量里：
```
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
```
永久添加可以放到最后一行: vim ~/.bashrc

一些可能遇到的error：
```
/opt/rh/devtoolset-8/root/usr/libexec/gcc/x86_64-redhat-linux/8/ld: cannot find libasan_preinit.o: No such file or directory
/opt/rh/devtoolset-8/root/usr/libexec/gcc/x86_64-redhat-linux/8/ld: cannot find -lasan
```
缺少libasan，需要安装：
```
yum install devtoolset-8-libasan-devel
```

## 运行和debug

编译代码：
```
make clean
make deps
make -j 10
```

启动一个server端和一个client端：
```
./rundb -nid0
./runcl -nid1
```

用GDB去debug：
```
gdb --args ./rundb -nid0
```

添加断点：（格式 b codefile:line_number），比如
```
b concurrency_control/row_ssi.cpp:331 
```

其他一些功能：
安装perf：
```
yum install perf gawk
```

安装jemalloc：
```
cd jemalloc-5.2.1
./autogen.sh
make
make install
```
为了使之有效，需要在编译时添加"-ljemalloc"。


安装tbb（安装最新版本）：
```
yum install tbb-devel
```