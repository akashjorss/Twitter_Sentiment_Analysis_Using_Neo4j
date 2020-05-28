## SDM_BDM_Joint_Project
Twitter sentiment analysis using Speed layer implementation of Lambda architecture. 

### How to contribute to this project?
Follow GitHub flow: https://guides.github.com/introduction/flow/
1. Clone this branch (git clone "repo_url")
2. Checkout a branch (git checkout -b "branch_name")
3. Make changes to the code.
4. Write a readme.
5. push the changes to the branch. 
6. Submit a pull request for the review. 

Create your java project or add a class to the existing java
### Architecture
Twitter -> Kafka -> Spark Streaming -> (Neo4j, MongoDB)

### Arguments for using stream processing over batch processing
1. Data is time sensitive.
2. End user is interested in information that can be obtained by stream processing algorithms.
3. Process a unit of data, e.g., a tweet, at a time, as opposed to data as a whole. Batch processing is done in neo4j where algorithms run on the whole data. 
4. No need for scheduling tool, since data is continuously arriving and being processed. Neo4j can run background jobs periodically. https://neo4j.com/docs/labs/apoc/current/background-operations/background-jobs/

### Architecture should tackle volume, velocity and variety. 
MongoDB for Volume and Variety: <ol>
  <li>Schemaless DBMS, for Variety. 
  <li>Scalable, for volume. 
  <li>Fault tolerant. 
  <li>Good for OLAP due to aggregation pipeline.</ol>
Spark Streaming for velocity: <ol>
  <li>Process one tweet (or a document) at a time.</ol>

### Artifacts
These are the files that are generated during the development process but are not used for deployment. 
These can be helpful scripts for testing or POC. Put all such scripts in the artifacts directory. 

### Spark Streaming
This java project is already initialized in the repository. 
This is basically taken from BDM Spark Streaming lab and we can reuse code. 





