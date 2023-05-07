from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from keras import models, layers
import numpy as np
import pyspark.sql.functions as f
from sklearn.preprocessing import MinMaxScaler
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import sklearn.metrics as metrics
import tensorflow as tf

# Create a SparkSession
spark = SparkSession.builder.appName("my_app").getOrCreate()
sqlContext = SQLContext(spark)

# Load the CSV file into a DataFrame
df = sqlContext.read.format('com.databricks.spark.csv') \
    .options(header='true', inferschema='true') \
    .load('AAPL.csv')

# Convert 'Date' column to a date type
df = df.withColumn('date', f.to_date('Date'))

# Split the 'date' column into 'year', 'month', and 'day' columns
date_breakdown = ['year', 'month', 'day']
for i in enumerate(date_breakdown):
    index = i[0]
    name = i[1]
    df = df.withColumn(name, f.split('date', '-')[index])

# Split the data into training and testing sets based on the 'year' column
trainDF = df[df.year < 2018]
testDF = df[df.year > 2019]

trainDF = trainDF.drop('date')

# Select only the relevant columns for training and testing
trainDF = trainDF.select('Open','High','Low','Close','Volume','Adj Close')
testDF = testDF.select('Open','High','Low','Close','Volume','Adj Close')

trainArray = np.array(trainDF.collect())
testArray = np.array(testDF.collect())

minMaxScale = MinMaxScaler()
minMaxScale.fit(trainArray)
testingArray = minMaxScale.transform(testArray)
trainingArray = minMaxScale.transform(trainArray)

xtrain = trainingArray[:, :-1]
xtest = testingArray[:, :-1]
ytrain = trainingArray[:, -1:]
ytest = testingArray[:, -1:]

def train_partition(partition, n_timesteps=5):
    partition_data = [row for row in partition]

    if not partition_data:
        print("Empty partition")
        return None

    partition_data = np.array(partition_data, dtype=np.float32)

    x = []
    y = []
    for i in range(len(partition_data) - n_timesteps):
        x.append(partition_data[i:i + n_timesteps, :-1])
        y.append(partition_data[i + n_timesteps, -1])

    x = np.array(x)
    y = np.array(y)

    local_model = models.Sequential()
    local_model.add(layers.LSTM(1, input_shape=(n_timesteps, x.shape[2])))
    local_model.add(layers.Dense(1))
    local_model.compile(loss='mean_squared_error', optimizer='adam')
    loss=local_model.fit(x, y, epochs=300, batch_size=20, verbose=0)
    plt.plot(loss.history['loss'], label = 'loss')
    plt.title('mean squared error by epoch')
    plt.legend()
    plt.show()
    return [local_model]
#Training the model
models_rdd = trainDF.rdd.mapPartitions(train_partition)

n_timesteps=5
n_samples_train = xtrain.shape[0] // n_timesteps
n_samples_test = xtest.shape[0] // n_timesteps
xtrain = np.reshape(xtrain[:n_samples_train * n_timesteps], (n_samples_train, n_timesteps, xtrain.shape[1]))
xtest = np.reshape(xtest[:n_samples_test * n_timesteps], (n_samples_test, n_timesteps, xtest.shape[1]))
#predicting the values
predictions_rdd = models_rdd.flatMap(lambda model: [model.predict(xtest)])
predictions_list = predictions_rdd.collect()
combined_array = predictions_list
print(combined_array)


plt.figure(figsize=(16, 6))  # Set the figure size
for predictions in predictions_list:
    plt.plot(predictions, color='blue', label='predicted')
   
plt.legend(loc = 'lower right')
plt.title(' Predicted APPL Stock')
plt.xlabel('Days')

plt.ylabel(' Price Predicted')
plt.show()

