zcat stable_identifiers.sql.gz |mysql -p1fish2stink stable_identifiers_sheldon
zcat test_reactome_54.sql.gz |mysql -p1fish2stink test_reactome_999
./add_ortho_stable_ids.pl -user smckay -pass 1fish2stink -db test_reactome_999 -sdb test_slice_999 -release_num 999 -pdb test_reactome_53
