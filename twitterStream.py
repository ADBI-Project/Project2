from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
   
    counts = stream(ssc, pwords, nwords, 50)
    print counts
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE



def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    with open(filename, 'r') as f:
        words = f.read().split('\n')
    return set(words)
    # YOUR CODE HERE

def count_words(wordlist, tweet):
    tweet_words = set([x.lower() for x in tweet.split(' ')])
    return len(wordlist & tweet_words)

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))
    # tweets.pprint()
    pairs = tweets.flatMap(lambda x: [("positive", count_words(pwords, x)), ("negative", count_words(nwords, x))])
    pairs = pairs.reduceByKey(lambda x, y: x + y)
    pairs.pprint()

    # neg = tweets.map(lambda x: ("negative", count_words(nwords, x)))
    # neg = neg.reduceByKey(lambda x, y: x + y)
    # neg.pprint()
    # print tweets.collect()

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    pairs.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()