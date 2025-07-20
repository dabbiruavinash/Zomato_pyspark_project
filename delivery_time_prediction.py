from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

class DeliveryTimePredictor:
    def __init__(self, spark):
        self.spark = spark
    
    def build_pipeline(self):
        # Feature preprocessing
        restaurant_indexer = StringIndexer(inputCol="restaurant_id", outputCol="restaurant_idx")
        location_indexer = StringIndexer(inputCol="delivery_location", outputCol="location_idx")
        encoder = OneHotEncoder(inputCols=["restaurant_idx", "location_idx"],
                               outputCols=["restaurant_vec", "location_vec"])
        
        # Feature assembly
        assembler = VectorAssembler(
            inputCols=["restaurant_vec", "location_vec", "order_time", "distance"],
            outputCol="features"
        )
        
        # Model
        rf = RandomForestRegressor(featuresCol="features", labelCol="delivery_time")
        
        # Pipeline
        pipeline = Pipeline(stages=[
            restaurant_indexer,
            location_indexer,
            encoder,
            assembler,
            rf
        ])
        
        return pipeline
    
    def train_model(self, train_df):
        pipeline = self.build_pipeline()
        model = pipeline.fit(train_df)
        return model
    
    def evaluate_model(self, model, test_df):
        predictions = model.transform(test_df)
        evaluator = RegressionEvaluator(
            labelCol="delivery_time",
            predictionCol="prediction",
            metricName="rmse"
        )
        rmse = evaluator.evaluate(predictions)
        return rmse