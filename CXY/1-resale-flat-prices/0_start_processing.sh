# 1. Clear GCS folder before running 1_lat_long_generator.py
echo "#####################################################################"
echo "#####################################################################"
echo "#### 1. Clear GCS folder before running 1_lat_long_generator.py #####"
echo "#####################################################################"
echo "#####################################################################"

gsutil rm -r gs://ebd-group-project-data-bucket/1-resale-flat-prices/1-wip-data/*
touch ./1-wip-data/placeholder.txt
gsutil cp -r 1-wip-data gs://ebd-group-project-data-bucket/1-resale-flat-prices

gsutil rm -r gs://ebd-group-project-data-bucket/1-resale-flat-prices/2-cleaned-data/*
touch ./2-cleaned-data/placeholder.txt
gsutil cp -r 2-cleaned-data gs://ebd-group-project-data-bucket/1-resale-flat-prices

# 1a. Clear GCS folder before running 1_lat_long_generator.py
# echo "#####################################################################"
# echo "#####################################################################"
# echo "#### 1a. copy school_lat_long.csv from 0-school\2-cleaned-data #####"
# echo "#####################################################################"
# echo "#####################################################################"
# gsutil cp gs://ebd-group-project-data-bucket/0-school/2-cleaned-data/school_lat_long.csv gs://ebd-group-project-data-bucket/1-resale-flat-prices/0-external-data

# 2. Execute: spark-submit 1_lat_long_generator.py
echo "#####################################################################"
echo "#####################################################################"
echo "########## 2. Execute: spark-submit 1_lat_long_generator.py #########"
echo "#####################################################################"
echo "#####################################################################"

spark-submit 1_lat_long_generator.py

# 2a. Execute: Python 2_merge_and_cleaned.py
echo "#####################################################################"
echo "#####################################################################"
echo "############ 2a. Execute: Python 2_merge_and_cleaned.py #############"
echo "#####################################################################"
echo "#####################################################################"

pythonfile=$(readlink -f 2_merge_and_clean.py)
python $pythonfile "address_lat_long_ref_table" "resale_lat_long"
gsutil cp -r 1-wip-data/resale_lat_long.csv gs://ebd-group-project-data-bucket/1-resale-flat-prices/1-wip-data/

# 3. Execute: spark-submit 1_lat_long_generator.py
echo "#####################################################################"
echo "#####################################################################"
echo "####### 3. Execute: spark-submit 3_join_resale_addresses.py ########"
echo "#####################################################################"
echo "#####################################################################"

spark-submit 3_join_resale_addresses.py

# 3a. Execute: Python 2_merge_and_cleaned.py
echo "#####################################################################"
echo "#####################################################################"
echo "############ 3a. Execute: Python 2_merge_and_cleaned.py #############"
echo "#####################################################################"
echo "#####################################################################"

pythonfile=$(readlink -f 2_merge_and_clean.py)
python $pythonfile "resales_join_address" "resales_join_address"

# 4. Copy output file to GCS
echo "#####################################################################"
echo "#####################################################################"
echo "#################### 4. Copy output file to GCS #####################"
echo "#####################################################################"
echo "#####################################################################"

gsutil cp -r 1-wip-data/* gs://ebd-group-project-data-bucket/1-resale-flat-prices/1-wip-data/
gsutil cp 1-wip-data/resale_lat_long.csv gs://ebd-group-project-data-bucket/1-resale-flat-prices/2-cleaned-data/
gsutil cp 1-wip-data/resales_join_address.csv gs://ebd-group-project-data-bucket/1-resale-flat-prices/2-cleaned-data/

# 5. Clear WIP folder
echo "#####################################################################"
echo "#####################################################################"
echo "####################### 5. Clear WIP folder #########################"
echo "#####################################################################"
echo "#####################################################################"

rm -r 1-wip-data/*
rm -r 2-cleaned-data/*