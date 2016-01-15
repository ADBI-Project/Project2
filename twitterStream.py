from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.lines as mlines


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    # Create a streaming context with batch interval of 10 sec
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")

    counts = stream(ssc, pwords, nwords, 100)
    # print counts
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    positive = np.array([x[0][1] for x in counts if len(x) > 0])
    negative = np.array([x[1][1] for x in counts if len(x) > 0])
    x_axis = np.array(range(len(positive)))
    plt.plot(x_axis, positive, '.b-', x_axis, negative, '.g-')
    plt.axis([-1, x_axis[-1] + 1, min(min(negative), min(positive)) -
              20, max(max(negative), max(positive)) + 20])
    plt.ylabel('Word count')
    plt.xlabel("Time step")
    blue_line = mlines.Line2D([], [], color='blue',
                              marker='.', markersize=10, label='positive')
    green_line = mlines.Line2D(
        [], [], color='green', marker='.', markersize=10, label='negative')
    plt.legend(handles=[blue_line, green_line])
    plt.show()
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
        ssc, topics=['twitterstream'], kafkaParams={"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii", "ignore"))
    # tweets.pprint()
    pairs = tweets.flatMap(lambda x: [("positive", count_words(
        pwords, x)), ("negative", count_words(nwords, x))])
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


if __name__ == "__main__":
    main()
