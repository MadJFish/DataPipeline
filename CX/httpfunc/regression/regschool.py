from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.utils import AnalysisException
INPUT_FILE = 'gs://ebd-group-project-data-bucket/2-nearby-resale/1-wip-data/merged_2_rank_distance.csv'
OUTPUT_DIR = 'gs://ebd-group-project-data-bucket/2-nearby-resale/1-wip-data/4_prediction2/'
df = spark.read.csv(INPUT_FILE, inferSchema=True, header=True)

# from pyspark.sql.functions import col
# df.select(col("distance"),col("sqm_resale_price"), col("school")).show(truncate=False)

school_list = [row.school for row in df.select('school').distinct().collect()]

assembler = VectorAssembler(
    inputCols=["distance"],
    outputCol="features")
df_features = assembler.transform(df)

# Create model for each school
model_list = []
for school in school_list[:5]:
    print(school)
    df_filtered = df_features.filter(df.school == school)
    glr = GeneralizedLinearRegression(family="gaussian", link="identity", linkPredictionCol="p",
                                    maxIter=10, regParam=0.2, labelCol='sqm_resale_price')
    model = glr.fit(df_filtered)
    pred = model.transform(df_filtered)
    model_list.append({'school': school, 'model': model, 'pred': pred, 'coef': float(model.coefficients.toArray()[0])})
    try:
        pred.drop('features').write.format("csv").option("header", "true").save(OUTPUT_DIR+school)
    except AnalysisException:
        # data exits
        continue

# sort by coefficients
coef_list = [(x['school'], x['coef']) for x in model_list]
coef_list = sorted(coef_list, key=lambda x:x[1])
cols = ["school","coef"]
data = spark.createDataFrame(data=coef_list, schema=cols)
data.write.format("csv").option("header", "true").save(OUTPUT_DIR+"0all_coef")
