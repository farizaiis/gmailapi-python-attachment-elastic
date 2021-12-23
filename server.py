import connexion
from elasticsearch import Elasticsearch


options = {"swagger_ui": True}
app = connexion.App(__name__, specification_dir="./", options=options)

app.add_api("swagger.yml")

es = Elasticsearch(host="localhost", port=9200)
es = Elasticsearch()
index = "train_elastic_email"

def create_index(index): 
    if(es.indices.exists(index=index) == False):
        create = es.indices.create(index=index, ignore=400)
        return create
create_index(index)

@app.route('/')
def home():
    return "Server Running"

if __name__ == "__main__":
    app.run(threaded=True, debug=True)