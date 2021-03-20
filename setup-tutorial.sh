# enable the Dataproc, Pub/Sub, Cloud Functions, and Datastore APIs
gcloud services enable \
    dataproc.googleapis.com \
    pubsub.googleapis.com \
    cloudfunctions.googleapis.com \
    datastore.googleapis.com
	
# Activate the Datastore database for your project
gcloud app create --region=us-central

# Clone the Git repository for this tutorial
git clone https://github.com/GoogleCloudPlatform/dataproc-pubsub-spark-streaming

# Change to the repository root and save the path into an environment variable to use later
cd dataproc-pubsub-spark-streaming
export REPO_ROOT=$PWD

# Create the tweets topic
gcloud pubsub topics create tweets

# Create the tweets-subscription subscription for the tweets topic
gcloud pubsub subscriptions create tweets-subscription --topic=tweets

# Create a service account
export SERVICE_ACCOUNT_NAME="dataproc-service-account"
gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME

# Add the Dataproc worker IAM role to allow the service account to create clusters and run jobs
export PROJECT=$(gcloud info --format='value(config.project)')
gcloud projects add-iam-policy-binding $PROJECT \
    --role roles/dataproc.worker \
    --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com"
	
# Add the Datastore user IAM role to allow the service account to read and write to the database
gcloud projects add-iam-policy-binding $PROJECT \
    --role roles/datastore.user \
    --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com"
	
# Add the Pub/Sub subscriber IAM role to allow the service account to subscribe to the tweets-subscription Pub/Sub subscription:
gcloud beta pubsub subscriptions add-iam-policy-binding \
    tweets-subscription \
    --role roles/pubsub.subscriber \
    --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com"
	
# Creating a Dataproc cluster
gcloud dataproc clusters create demo-cluster \
    --region=us-central1 \
    --zone=us-central1-a \
    --scopes=pubsub,datastore \
    --image-version=1.2 \
    --service-account="$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com"
	
# Change to the application directory
cd $REPO_ROOT/spark

# Change the Java Development Kit (JDK) version to 1.8
sudo update-java-alternatives -s java-1.8.0-openjdk-amd64 && export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre

# Build the application archive
mvn clean package

# Submit the Spark streaming app job
export PROJECT=$(gcloud info --format='value(config.project)')
export JAR="spark-streaming-pubsub-demo-1.0-SNAPSHOT.jar"
export SPARK_PROPERTIES="spark.dynamicAllocation.enabled=false,spark.streaming.receiver.writeAheadLog.enabled=true"
export ARGUMENTS="$PROJECT 60 20 60 hdfs:///user/spark/checkpoint"

gcloud dataproc jobs submit spark \
--cluster demo-cluster \
--region us-central1 \
--async \
--jar target/$JAR \
--max-failures-per-hour 10 \
--properties $SPARK_PROPERTIES \
-- $ARGUMENTS

# Deploy the HTTP function
cd $REPO_ROOT/http_function
gcloud functions deploy http_function \
    --trigger-http \
    --runtime nodejs10 \
    --allow-unauthenticated \
    --region=us-central1
