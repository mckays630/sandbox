#!/bin/bash

for num in 43 44 45 46 47 48 49 50 51 52
do
    echo $num
    mysqldump -p1fish2stink test_slice_$num | gzip -c > db.sql.gz
    zcat db.sql.gz | mysql -p1fish2stink test_slice_${num}_repaired
    mysqldump -p1fish2stink test_reactome_$num | gzip -c > db.sql.gz
    zcat db.sql.gz | mysql -p1fish2stink test_reactome_${num}_repaired
    rm -f db.sql.gz
done
