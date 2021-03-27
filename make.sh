g++ -gdwarf-2 -std=c++17 -static-libstdc++ -o 3TS $(cd "$(dirname "$0")";pwd)/src/3ts/backend/main.cc -lgflags -lpthread -lconfig++
g++ -std=c++17 -g -o 3TS-DAI $(cd "$(dirname "$0")";pwd)/src/3ts/backend/anomaly_identify.cc
