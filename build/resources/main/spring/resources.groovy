import com.mongodb.MongoClient

// Place your Spring DSL code here
beans = {
    mongo(MongoClient, "localhost")
}
