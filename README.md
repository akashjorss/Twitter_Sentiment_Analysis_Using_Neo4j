## SDM/BDM Joint Project
Twitter sentiment analysis using Speed layer implementation of Lambda architecture. 

### To run this app
```
git clone https://github.com/akashjorss/SDM_BDM_Joint_Project.git 
pip install -r requirement.txt
```
setup environment variables as follows:
```
export CONSUMER_KEY="your twitter consumer key"
export CONSUMER_SECRET="you twitter consumer secret"
export ACCESS_TOKEN="your twitter access token"
export ACCESS_SECRET="your twitter access secret"
export ELASTIC_CLOUD_ID="your elastic cloud id" (Ask Akash if you don't have one)
export ELASTIC_USERNAME="your elastic username"
export ELASTIC_PASSWORD="your elastic password"
```
Start the mongodb server. 
Finally run the app:
```
python3 __init__.py
```
Make sure you are using java 8 (check by java -version command)
If not, on Mac, do the following in command line of environment:
```
export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
```
If you are not using OSX, change the functions start_mongodb and stop_mongodb in __init__.py 
### How to contribute to this project?
To view the project board: https://github.com/akashjorss/SDM_BDM_Joint_Project/projects/1<br>
Follow GitHub flow: https://guides.github.com/introduction/flow/
1. Clone this branch (git clone "repo_url")
2. Checkout a branch (git checkout -b "branch_name")
3. Make changes to the code. (Create a python file or add a method to the existing project)
4. Write a readme.
5. push the changes to the branch. 
6. Submit a pull request for the review. 

### Architecture
Twitter -> Kafka -> Spark Streaming -> (Neo4j, MongoDB)

### Arguments for using stream processing over batch processing
1. Data is time sensitive.
2. End user is interested in information that can be obtained by stream processing algorithms.
3. Process a unit of data, e.g., a tweet, at a time, as opposed to data as a whole. Batch processing is done in neo4j where algorithms run on the whole data. 
4. No need for scheduling tool, since data is continuously arriving and being processed. Neo4j can run background jobs periodically. https://neo4j.com/docs/labs/apoc/current/background-operations/background-jobs/

### Architecture should tackle volume, velocity and variety. 
MongoDB for Volume and Variety: <ul>
  <li>Schemaless DBMS, for Variety. 
  <li>Scalable, for volume. 
  <li>Fault tolerant. 
  <li>Good for OLAP due to aggregation pipeline.</ul>
Spark Streaming for velocity: <ul>
  <li>Process one tweet (or a document) at a time.</ul>

### Artifacts
These are the files that are generated during the development process but are not used for deployment. 
These can be helpful scripts for testing or POC. Put all such scripts in the artifacts directory. 

### Classes needed to be implement
<ul>
<li>Kafka
<li>Spark Streaming
<li>MongoDB
<li>Neo4j
<li>Machine learning
</ul>

### To load data to Neo4j:
<ol>
<li>Run neo4j community edition as a console application. 
<li>Configure Neo4j.py with uri, user, password and database.</li>
<li>Configure tweet_server with your twitter credentials and run it.</li>
<li>Run spark-streaming.py concurrently in command line. (Make sure you have openjdk 8)</li>
<li>Open neo4j browser and explore the data</li>



