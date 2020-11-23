#!/bin/bash

set -x

#scripits to run
for concurrency_control in MVCC MAAT
do
    #update CC_ALG
    sed -i -e "/CC_ALG/d" -e "100a  \#define CC_ALG  ${concurrency_control}"  config.h
    #update compiling
    make clean
    make -j10
    echo "update of db success"
#    for theta_value in 0.0 0.25 0.5 0.55 0.6 0.65 0.7 0.75 0.8 0.9
    for theta_value in 0.0
    do
        ./rundb -nid0 -zipf${theta_value} &
        #connect 146
        echo "trying to connect 147"
ssh -t -p22 root@10.77.110.147  << remotessh
            cd /root/zzhdeneva
            sed -i -e "/CC_ALG/d"  -e "100a  \#define CC_ALG  ${concurrency_control}" config.h
            make clean
            make -j10
            ./runcl -nid1 -zipf${theta_value}  > /root/zzhdeneva/test/output_${concurrency_control}_${theta_value}.txt
            echo "test of ${concurrency_control} and ${theta_value} success"
            exit
remotessh
        # TODO: lsof -i 18000; kill this pid
        sleep 10
    done
    sleep 5
done

ssh -t -p22 root@10.77.110.147  << remotessh
            cd /root/zzhdeneva/test
            python output-gather.py > output.txt
            exit
remotessh

