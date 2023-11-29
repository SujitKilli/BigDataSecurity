from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("MutualFriends").getOrCreate()

# Read the data from the text file
data_path = "./mutual.txt"  # Replace with the actual path to your file
friends_info = spark.read.text(data_path)

# Map function for parsing the file
def create_friend_list(line):
    user_friends = line.value.split('\t')
    user_id = int(user_friends[0])
    friends = []
    if len(user_friends) > 1:
        for fr in user_friends[1].split(','):
            try:
                friends.append(int(fr))
            except:
                print(fr)
    return (user_id, friends)

# Apply the map function to create a DataFrame of user and their friends
user_friends = friends_info.rdd.map(create_friend_list).toDF(["user", "friends"])

# Create a DataFrame of friend pairs
friend_pairs = user_friends.select("user", col("friends").alias("friend"))

# Self-join to get pairs of friends
friend_pairs = friend_pairs.join(friend_pairs, "user", "inner").toDF("user", "friend1", "friend2")

# Filter out rows where friend1 is not equal to friend2
mutual_friends = friend_pairs.filter(col("friend1") != col("friend2"))

# Display the result
mutual_friends.show(truncate=False)

# Stop Spark session
spark.stop()
