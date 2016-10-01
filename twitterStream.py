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
   
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    new_counts = []
    for x in counts:
      if x:
        new_counts.append(x)

    time_step = range(len(new_counts))

    
    pos = []
    neg = []
    for x in new_counts:
        pos.append(x[0][-1])
        neg.append(x[1][-1])

    plt.xlabel("Time step")
    plt.ylabel("Word count")
    plt.axis([-1,12,0,200])
    pos_lines, = plt.plot(time_step,pos,label='positive',linewidth=2)
    neg_lines, = plt.plot(time_step,neg,label='negative',linewidth=2)
    plt.setp(pos_lines,color='b',marker='o')
    plt.setp(neg_lines,color='g',marker='o')
    plt.legend(bbox_to_anchor=(1.05,1),loc=1,borderaxespad=0.)
    plt.show()

    return



def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    mylist = []
    f = open(filename,'r')
    for line in f:
      word = line.strip()
      mylist.append(word)

    f.close()
    return mylist



def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE

    counts = []
    def countFunc(x):
      if x in pwords:
        return ("positive",1)
      elif x in nwords:
        return ("negative",1)
      else:
        return ("positive",0)
    tweets = tweets.flatMap(lambda x: x.split(" ")).map(countFunc).reduceByKey(lambda x,y:x+y)
    tweets.pprint()
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    tweets.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    

    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
