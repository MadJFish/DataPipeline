# 1. Clear GCS folder before running 1_lat_long_generator.py
echo "#####################################################################"
echo "#####################################################################"
echo "#### 1. Clear GCS folder before running 1_lat_long_generator.py #####"
echo "#####################################################################"
echo "#####################################################################"

gsutil rm -r gs://ebd-group-project-data-bucket/2-nearby-resale/1-wip-data/*
touch ./1-wip-data/placeholder.txt
gsutil cp -r 1-wip-data gs://ebd-group-project-data-bucket/2-nearby-resale

gsutil rm -r gs://ebd-group-project-data-bucket/2-nearby-resale/2-cleaned-data/*
touch ./2-cleaned-data/placeholder.txt
gsutil cp -r 2-cleaned-data gs://ebd-group-project-data-bucket/2-nearby-resale

# 1a. Clear GCS folder before running 1_lat_long_generator.py
echo "#####################################################################"
echo "#####################################################################"
echo "#### 1a. copy school_lat_long.csv from 0-school\2-cleaned-data #####"
echo "#####################################################################"
echo "#####################################################################"

gsutil cp gs://ebd-group-project-data-bucket/0-school/2-cleaned-data/school_lat_long.csv gs://ebd-group-project-data-bucket/2-nearby-resale/0-external-data
gsutil cp gs://ebd-group-project-data-bucket/1-resale-flat-prices/2-cleaned-data/resale_lat_long.csv gs://ebd-group-project-data-bucket/2-nearby-resale/0-external-data
gsutil cp gs://ebd-group-project-data-bucket/1-resale-flat-prices/2-cleaned-data/resales_join_address.csv gs://ebd-group-project-data-bucket/2-nearby-resale/0-external-data

# 2. Execute: spark-submit 1_get_filtered_distance.py
echo "#####################################################################"
echo "#####################################################################"
echo "######## 2. Execute: spark-submit 1_get_filtered_distance.py ########"
echo "#####################################################################"
echo "#####################################################################"

spark-submit 1_get_filtered_distance.py

# 3. Execute: spark-submit 2_rank_distance.py
echo "#####################################################################"
echo "#####################################################################"
echo "########### 3. Execute: spark-submit 2_rank_distance.py #############"
echo "#####################################################################"
echo "#####################################################################"

spark-submit 2_rank_distance.py

# 4. Execute: spark-submit 2_rank_distance.py
echo "#####################################################################"
echo "#####################################################################"
echo "######## 4. Execute: spark-submit 3_distance_classifier.py ##########"
echo "#####################################################################"
echo "#####################################################################"

spark-submit 3_distance_classifier.py

4. Copy output file to GCS
echo "#####################################################################"
echo "#####################################################################"
echo "#################### 4. Copy output file to GCS #####################"
echo "#####################################################################"
echo "#####################################################################"

gsutil cp -r 1-wip-data/* gs://ebd-group-project-data-bucket/2-nearby-resale/1-wip-data/
gsutil cp -r 1-wip-data/* gs://ebd-group-project-data-bucket/2-nearby-resale/2-cleaned-data/

5. Clear WIP folder
echo "#####################################################################"
echo "#####################################################################"
echo "####################### 5. Clear WIP folder #########################"
echo "#####################################################################"
echo "#####################################################################"

rm -r 1-wip-data/*
rm -r 2-cleaned-data/*