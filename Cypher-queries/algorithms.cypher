//Label Propagation
CALL algo.labelPropagation.stream("hashtag", "CONTAINS",
  {direction: "INCOMING", iterations: 10})
yield  nodeId,label
WITH algo.asNode(nodeId).name AS Hashtag,label as  community
WHERE NOT Hashtag = 'null'
WITH Hashtag, community
match (c:Company)-[:ABOUT]-(h:Hashtag)-[:CONTAINS]-(t:Tweet)
WHERE h.name=Hashtag
WITH community,h,t,t.sentiment As sentiment,c.name as Company
RETURN community, collect(distinct h.name) AS Hashtags,count(t) AS numberOfTweets, avg(sentiment) AS avgSentiment,Company ORDER BY numberOfTweets DESC

//Louvain algorithm
CALL algo.louvain.stream('hashtag', 'CONTAINS', { direction: 'BOTH'}) YIELD nodeId, community
WITH algo.asNode(nodeId).name AS Hashtag, community
WHERE NOT Hashtag = 'null'
WITH Hashtag, community
match (h:Hashtag)-[:CONTAINS]-(t:Tweet)
WHERE h.name=Hashtag
WITH community,h,t,t.sentiment As sentiment
RETURN community, collect(distinct h.name) AS Hashtags,count(t) AS numberOfTweets, sum(sentiment) AS OverallSentiment ORDER BY numberOfTweets DESC

//Pagerank for hashtags
CALL algo.pageRank.stream('', '', {iterations:20})
YIELD nodeId, score
with algo.getNodeById(nodeId).name AS Hashtag, score
WHERE NOT Hashtag = 'null' AND Hashtag =~'#.*'
WITH Hashtag, score
match (c:Company)-[:ABOUT]-(h:Hashtag)-[:CONTAINS]-(t:Tweet)
WHERE h.name=Hashtag
WITH score,h,t,t.sentiment As sentiment,c.name as Company
RETURN score, collect(distinct h.name) AS Popularhashtags,count(t) AS numberOfTweets, avg(sentiment) AS avgSentiment,Company ORDER BY score DESC

//Pagerank for companies
CALL algo.pageRank.stream('', '', {iterations:20})
YIELD nodeId, score
with algo.getNodeById(nodeId).name AS Company, score
WHERE NOT Company = 'null' AND NOT Company =~'#.*'
WITH Company, score
match (c:Company)-[:ABOUT]-(h:Hashtag)-[:CONTAINS]-(t:Tweet)
WHERE c.name=Company
WITH score,h,t,t.sentiment As sentiment,c.name as Company
RETURN score, collect(distinct Company) AS Popularcompany,count(t) AS numberOfTweets, avg(sentiment) AS avgSentiment ORDER BY score DESC