# 1. Clear GCS folder before running 1_lat_long_generator.py
echo "#####################################################################"
echo "#####################################################################"
echo "#### 1. Clear GCS folder before running 1_lat_long_generator.py #####"
echo "#####################################################################"
echo "#####################################################################"

rm -r 1-wip-data/*
rm -r 2-cleaned-data/*

gsutil rm -r gs://ebd-group-project-data-bucket/0-school/1-wip-data/*
touch ./1-wip-data/placeholder.txt
gsutil cp -r 1-wip-data gs://ebd-group-project-data-bucket/0-school

gsutil rm -r gs://ebd-group-project-data-bucket/0-school/2-cleaned-data/*
touch ./2-cleaned-data/placeholder.txt
gsutil cp -r 2-cleaned-data gs://ebd-group-project-data-bucket/0-school

# 2. Execute: spark-submit 1_lat_long_generator.py
echo "#####################################################################"
echo "#####################################################################"
echo "########## 2. Execute: spark-submit 1_lat_long_generator.py #########"
echo "#####################################################################"
echo "#####################################################################"

spark-submit 1_lat_long_generator.py

# 3. Execute: Python 2_merge_and_cleaned.py
echo "#####################################################################"
echo "#####################################################################"
echo "############# 3. Execute: Python 2_merge_and_cleaned.py #############"
echo "#####################################################################"
echo "#####################################################################"

pythonfile=$(readlink -f 2_merge_and_clean.py)
python $pythonfile

# 4. Copy output file to GCS
echo "#####################################################################"
echo "#####################################################################"
echo "#################### 4. Copy output file to GCS #####################"
echo "#####################################################################"
echo "#####################################################################"

gsutil cp -r 1-wip-data/* gs://ebd-group-project-data-bucket/0-school/1-wip-data/
gsutil cp -r 1-wip-data/merged.csv gs://ebd-group-project-data-bucket/0-school/2-cleaned-data/school_lat_long.csv

# 5. Clear WIP folder
echo "#####################################################################"
echo "#####################################################################"
echo "####################### 5. Clear WIP folder #########################"
echo "#####################################################################"
echo "#####################################################################"

rm -r 1-wip-data/*
rm -r 2-cleaned-data/*