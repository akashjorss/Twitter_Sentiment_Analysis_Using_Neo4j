from textblob import TextBlob

def prune_tweet(tweet, company):
    """
    :param tweet: a json tweet object
    :param company: company name
    :return: tweet, a modified tweet object with only relevant keys
    """
    #calculate polarity
    polarity = TextBlob(tweet["text"]).sentiment.polarity
    hashtags = []
    for h in tweet["entities"]["hashtags"]:
        hashtags.append('#'+h["text"])
    date = tweet["created_at"].split(" ")[2]+"_"+tweet["created_at"].split(" ")[1]+"_"+tweet["created_at"].split(" ")[5]
    time = tweet["created_at"].split(" ")[3]
    modified_tweet = {
        "id": tweet["id_str"],
        "company": company,
        "sentiment": polarity,
        "retweet_count": tweet["retweet_count"],
        "date": date,
        "time": time,
        "hashtags": hashtags,
    }
    return modified_tweet


def identify_company(tweet, companies):
    """
    :param tweet: tweet object
    :param companies: list of companies
    :return: list of (company, tweet) pairs
    """
    flatmap = []
    for company in companies:
        if company in tweet['text']:
            flatmap.append((company, tweet))

    return flatmap