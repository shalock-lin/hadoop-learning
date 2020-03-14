#!/bin/bash

params=$@
i=201
for (( i=1 ; i <= 3 ; i = $i + 1 )) ; do
    echo ============= node0$i $params =============
    ssh node0$i "source /etc/profile;$params"
done

