mysql -p1fish2stink test_slice_53 -e 'select count(*) as slice_instance from DatabaseObject where StableIdentifier IS NOT NULL'
mysql -p1fish2stink -hreactomecurator gk_central -e 'select count(*) as central_instance from DatabaseObject where StableIdentifier IS NOT NULL'
mysql -p1fish2stink test_reactome_53 -e 'select count(*) as release_instance from DatabaseObject where StableIdentifier IS NOT NULL'
mysql -p1fish2stink test_slice_53 -e 'select count(*) as slice from StableIdentifier'
mysql -p1fish2stink -hreactomecurator gk_central -e 'select count(*) as central from StableIdentifier'
mysql -p1fish2stink test_reactome_53 -e 'select count(*) as release_53 from StableIdentifier'
