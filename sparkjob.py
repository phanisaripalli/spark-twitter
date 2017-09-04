from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import matplotlib.pyplot as plt;
plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt

def process_word(word):
    """
    processes a word. Check if begins with desired chars
    """
    if (word.startswith('@') or word.startswith('#') or word.startswith('www.') or word.startswith('http')):
        return (word.lower().encode('utf-8'), 1)
    else:
        return (word, 0)

def process_categories(word):
    """
    Processes categories - user-mentions, hahstags, links and rest are others
    """
    if (has_non_asci(word)):
        if (word.startswith('@')):
            return ('@user-mentions', 1)
        elif (word.startswith('#')):
            return ('#hashtags', 1)
        elif (word.startswith('www.') or word.startswith('http')):
            return ('links', 1)
        else:
            return ('other', 0)
    else:
        return ('other', 0)

def process_occurences(line_b):
    """
    Processes occurrences of two words (words based on criteria) in a tweet
    """

    line =  line_b['text'].encode('utf-8')
    words = line.split(" ")
    result = []
    for word in words:
        for word2 in words:
            if (is_word_for_analysis(word) and (is_word_for_analysis(word2))):
                if ( (word != word2) and (((word2, word), 1) not in result) ):
                    result.append(((word, word2), 1))
    return result

def process_occurences_special(line_b):

    line =  line_b['text'].encode('utf-8')
    line = line.replace("\n", " ")
    words = line.split(" ")
    words1 = []

    for word in words:
        if word.endswith(':'):
            s = word[:-1]
        else:
            s = word
        words1.append(s.lower())

    words = words1
    result = []
    for word in words:
        for word2 in words:
            if (is_word_for_analysis(word) and (is_word_for_analysis(word2))):
                if ( (word != word2) and (((word2, word), 1) not in result) ):
                    result.append(((word, word2), 1))
    return result

def process_occurences_hash(line_b):
    """
        Processes occurrences of two hahstags (words based on criteria) in a tweet
    """

    line = line_b['text']
    line = line.replace("\n", " ")
    line = line.replace("\u00A0", " ")
    line = line.replace("\u00a0", " ")
    line = line.replace(",", " ")
    line = line.replace(".", " ")
    line = line.encode('utf-8')
    line = line.replace("\xc2\xa0", " ")

    words = line.split(" ")
    words1 = []

    for word in words:
        if word.endswith(':'):
            s = word[:-1]
        else:
            s = word
        words1.append(s.lower())

    words = words1
    result = []
    for word in words:
        for word2 in words:
            if (is_hashtag(word) and (is_hashtag(word2))):
                if ( (word != word2) and (((word2, word), 1) not in result) ):
                    sets = [word, word2]
                    sets = sorted(sets)
                    result.append(((sets[0].decode('utf-8'), sets[1].decode('utf-8')), 1))
    return result

# found on stackoverflow
def remove_emojis(word):
    import re
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               "]+", flags=re.UNICODE)
    return  emoji_pattern.sub(r'__', word)

def is_word_for_analysis(word):
    """
    Only hashtags and user-mentions. Links can be activated
    """
    # or word.startswith('http') OR  word.startswith('www')
    if (word.startswith('@') or word.startswith('#')):
        return True
    else:
        return False

def has_non_asci(word):
    """
    Checks if a word has non-asci character.
    """
    return all(ord(c) < 128 for c in word)

def is_hashtag(word):
    """
    Check if a word begind with a # and has non asci. For e.g. #foo... is mostly end of the tweet and not counted
    """
    if (word.startswith('#') and has_non_asci(word)):
        return True
    else:
        return False

def plot_bar(df, title, x_axis, y_axis):
    """
    Plots a bar chart
    :param df: dataframe
    :param title:  title
    :param x_axis: x axis - ttile
    :param y_axis: y -axis - title
    :return:
    """
    x = []
    y = []
    for category in df.take(50):
        x.append(category[0])
        y.append(category[1])

    objects = tuple(x)
    y_pos = np.arange(len(objects))

    plt.bar(y_pos, y, align='center', alpha=0.5)
    plt.xticks(y_pos, objects)
    plt.ylabel(y_axis)
    plt.xlabel(x_axis)
    plt.title(title)

    for i, v in enumerate(y):
        plt.subplot().text(i + .25, v + 3, str(v), color='blue', fontweight='bold')

    plt.show()

def plot_network(df):
    """
    Plots network graph
    :param df:
    :return:
    """
    import networkx as nx
    import matplotlib.pyplot as plt
    import pylab

    G = nx.DiGraph()
    i = 0
    weights = {}
    for network in df.collect():
        if i <= 10:
            weights[network[0][0] + '_' + network[0][1]] = network[1]
            #print(network[0])
            G.add_edges_from([network[0]])

            i = i + 1

    val_map = {'A': 100.0,
               'D': 200.5714285714285714,
               'H': 0.0}

    values = [val_map.get(node, 0.45) for node in G.nodes()]
    edge_labels = dict([((u, v,), weights[u+'_'+v])
                        for u, v, d in G.edges(data=True)])
    pos = nx.spring_layout(G)
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)
    nx.draw(G, pos, node_size=1500, edge_color='black', edge_cmap=plt.cm.Reds, with_labels=True)
    pylab.show()

def process(spark):
    """
    Basically process the spark app
    :param spark: spark session
    """
    sc = spark.sparkContext
    sc.addPyFile("dependencies.zip")

    path = "tweets.json"
    # Read
    tweetsDF = spark.read.json(path)

    # Total tweets collected
    print(tweetsDF.count())

    tweetsDF.createOrReplaceTempView("tweets")

    hashtagsDF = tweetsDF.select(explode("hashtags").alias("hashtag")).select("hashtag.text")
    hashtagsDF.printSchema()

    hashtagsDF.createOrReplaceTempView("hashtags")

    hashtags_tweets_df = spark.sql("SELECT nr_hashtags, count(id) AS nr_tweets FROM "
                                "(SELECT id, size(hashtags) AS nr_hashtags FROM tweets "
                                ") sq "
                                "GROUP BY nr_hashtags ORDER BY nr_hashtags ")

    plot_bar(hashtags_tweets_df.rdd, 'Nr. Hashtags and total tweets distribution', 'Nr. Hashtags', 'Total tweets')


    hashtags_agg = spark.sql("SELECT text, COUNT(*) AS total "
              "FROM hashtags "
              "GROUP BY text "
              "ORDER BY count(*) DESC "
              "LIMIT 10 ")
    #hashtags_agg.show()
    plot_bar(hashtags_agg.rdd, 'Hashtags distribution', 'Hashtags', 'Count')


    occurencesDf = tweetsDF.rdd.flatMap(process_occurences_special).map(lambda ls: ls).reduceByKey(lambda a, b: a + b).sortBy(
        lambda x: x[1], False)
    plot_network(occurencesDf)

    occurences_hashtags_df = tweetsDF.rdd.flatMap(process_occurences_hash).map(lambda ls: ls).reduceByKey(
        lambda a, b: a + b).sortBy(
        lambda x: x[1], False)
    plot_network(occurences_hashtags_df)


    # categories
    categories = tweetsDF.rdd.flatMap(lambda line: line['text'].split(" ")) \
        .map(process_categories).reduceByKey(lambda a, b: a + b).filter(lambda x: x[1] >= 1).sortBy(lambda x: x[1],
                                                                                                    False)
    #print(categories.collect())
    plot_bar(categories, 'Categories distribution', 'Categories', 'Tweets')


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL on tweets") \
        .getOrCreate()

    process(spark)
    spark.stop()

