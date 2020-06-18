//display all hashtags and tweets
match(H:Hashtag)<--(T:Tweet)
return H,T;

//Average sentiment of a hashtag
match(H:Hashtag)<--(T:Tweet)
return H.name,avg(T.sentiment) as Average_Sentiment

//Get the positive hashtags
match(H:Hashtag)<--(T:Tweet)
with avg(T.sentiment) AS Positive,H
where Positive>0
return H.name,Positive
order by Positive DESC

//Top 3 hashtag by company
MATCH(c:Company)<-[a:ABOUT]-(h:Hashtag)-[contain:CONTAINS]-(t:Tweet)
WITH c,h,count(t) AS tweetNum ORDER BY tweetNum DESC
RETURN c,collect(h)[. .3 ]

//Return Trending company rank by number of tweets related to company
MATCH(c:Company)<-[a:ABOUT]-(h:Hashtag)-[contain:CONTAINS]-(t:Tweet)
WITH c,h,count(t) AS tweetNum ORDER BY tweetNum DESC
WITH distinct c AS company ,sum(tweetNum) AS tweetsCount
WITH company, COLLECT(tweetsCount) AS weights
WITH collect(company) AS cs,collect(weights) AS ws
UNWIND [r IN RANGE(1, SIZE(ws)) | {rank:r, weight: ws[r-1], company:cs[r-1]}] AS res
RETURN res

