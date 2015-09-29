mysqldump -usmckay -p1fish2stink -hreactomecurator gk_central |gzip -c>gk_central_backup.sql.gz
mysqldump -usmckay -p1fish2stink test_reactome_53 | gzip -c > test_reactome_53_backup.sql.gz
mysqldump -usmckay -p1fish2stink test_slice_53 | gzip -c > test_slice_53_backup.sql.gz
