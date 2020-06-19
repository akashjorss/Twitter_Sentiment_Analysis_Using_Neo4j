// Tweet level sentiment normalization
MATCH(t:Tweet)
SET t.sentiment = (t.sentiment+1)/2
RETURN t


// Calculating the probability of Hashtag being 1 or 0 (prob_1, prob_0)
MATCH (h:Hashtag)-[]-(t:Tweet)
WITH h AS h, SUM(t.sentiment) AS spos, SUM(1-t.sentiment) AS sneg,
SUM(t.sentiment)+SUM(1-t.sentiment) AS denom
SET h.prob_1 = spos/denom, h.prob_0 = sneg/denom
RETURN h


// Giving a discrete sentiment label for the hashtag based on prob_1 & prob_0
MATCH (h:Hashtag)
SET h.sent_label = CASE WHEN h.prob_1 > h.prob_0 THEN 1 WHEN h.prob_1 < h.prob_0 THEN -1 ELSE 0 END
RETURN h 
 
 
// Projecting Hashtag-Tweet-Hashtag graph to weighted Hashtag-Hashtag graph 
CALL gds.graph.create.cypher(
    'g',
    'MATCH (h:Hashtag) RETURN id(h) AS id, h.sent_label AS sent_label',
    'MATCH (h1:Hashtag)-[:CONTAINS]-(t)-[:CONTAINS]-(h2:Hashtag) RETURN id(h1) AS source, id(h2) AS target, COUNT(t) AS weight'
)

