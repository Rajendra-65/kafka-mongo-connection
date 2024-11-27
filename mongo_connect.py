from pymongo import MongoClient

# MongoDB connection string

conn_string = "mongodb+srv://Rajendra:65_ODA4@cluster0.c4uzthx.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Connect to MongoDB

client = MongoClient(conn_string)

# Select the database

db = client['airbnb_mart']

# Select the collection

collection = db['airbnb_property_reviews']

# Insert a document

# insert_result = collection.insert_one({'name': 'John Doe', 'age': 30})
# print(f"Document inserted with id: {insert_result.inserted_id}")

# Query the collection

# find_query = {'property_type' : 'Apartment', 'room_type': 'Private room'} # comma treated as and

# find_query = { '$or' : [ {'property_type' : 'House'} , {'property_type' : 'Apartment'} ] }
# find_query = { 'room_type': 'Private room', 
#                 '$or' : [ {'property_type' : 'House'} , {'property_type' : 'Apartment'} ] 
#             }
find_query = { 'accommodates': { '$gt' : 2} }

count_results = collection.count_documents(find_query)
print("Total Documents Found : ",count_results)

# Print documents fetched from find query
# find_results = collection.find(find_query)
# for doc in find_results:
#     print(doc)


# grp1 = [
#     {
#         "$group": {
#             "_id": "$address.country",  # Field to group by
#             "avg_price": {"$avg": "$price"}  # Field to avg
#         }
#     },
#     {
#         "$project": {
#             "country": "$_id",
#             "avg_price": {"$toDouble": "$avg_price"},
#             "_id":0
#         }
#     }
# ]

grp2 = [
    {
        "$group": {
            "_id": {
                "country": "$address.country",
                "city": "$address.suburb"
            },
            "avg_price": {"$avg": "$price"}
        }
    },
    {
        "$project": {
            "country": "$_id.country",
            "city": "$_id.city",
            "avg_price": {"$toDouble": "$avg_price"},
            "_id": 0
        }
    }
]

results = collection.aggregate(grp2)
for result in results:
    print(result)


# # Close the connection
client.close()
