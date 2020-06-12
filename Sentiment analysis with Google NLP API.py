# -*- coding: utf-8 -*-
# DO NOT EDIT! This is a generated sample ("Request",  "language_sentiment_gcs")

# To install the latest published package dependency, execute the following:
#   pip install google-cloud-language

# sample-metadata
#   title: Analyzing Sentiment (GCS)
#   description: Analyzing Sentiment in text file stored in Cloud Storage
#   usage: python3 samples/v1/language_sentiment_gcs.py [--gcs_content_uri "gs://cloud-samples-data/language/sentiment-positive.txt"]

# Configure the environment setting in permenant windows environment 
# The name is GOOGLE_APPLICATION_CREDENTIALS; 
# The path is C:\Users\休息的风\Desktop\NLP.json
# ! Four times configuration setting need to be changed in advanced configuration setting for windows system

#
# [START language_sentiment_gcs]
from google.cloud import language_v1
from google.cloud.language_v1 import enums


def sample_analyze_sentiment(gcs_content_uri):
    """
    Analyzing Sentiment in text file stored in Cloud Storage

    Args:
      gcs_content_uri Google Cloud Storage URI where the file content is located.
      e.g. gs://[Your Bucket]/[Path to File]
    """

    client = language_v1.LanguageServiceClient()

    # gcs_content_uri = 'gs://cloud-samples-data/language/sentiment-positive.txt'

    # Available types: PLAIN_TEXT, HTML
    type_ = enums.Document.Type.PLAIN_TEXT

    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
    language = "en"
    document = {"gcs_content_uri": gcs_content_uri, "type": type_, "language": language}

    # Available values: NONE, UTF8, UTF16, UTF32
    encoding_type = enums.EncodingType.UTF8

    response = client.analyze_sentiment(document, encoding_type=encoding_type)
    # Get overall sentiment of the input document
    print(u"Document sentiment score: {}".format(response.document_sentiment.score))
    print(
        u"Document sentiment magnitude: {}".format(
            response.document_sentiment.magnitude
        )
    )
    # Get sentiment for all sentences in the document
    for sentence in response.sentences:
        print(u"Sentence text: {}".format(sentence.text.content))
        print(u"Sentence sentiment score: {}".format(sentence.sentiment.score))
        print(u"Sentence sentiment magnitude: {}".format(sentence.sentiment.magnitude))

    # Get the language of the text, which will be the same as
    # the language specified in the request or, if not specified,
    # the automatically-detected language.
    print(u"Language of the text: {}".format(response.language))


# [END language_sentiment_gcs]


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--gcs_content_uri",
        type=str,
        default="gs://cloud-samples-data/language/sentiment-positive.txt",
    )
    args = parser.parse_args()

    sample_analyze_sentiment(args.gcs_content_uri)


if __name__ == "__main__":
    main()
