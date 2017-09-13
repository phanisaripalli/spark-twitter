<h1>Spark app for twitter analytics</h1>

- Use Kafka to save tweets in JSON (basically producer puts in queue and consumer writes to JSON)
- Use spark to read in JSON
- Do some analysis and create charts
  1. Categorise tweets (user-mentions, links, hastags etc)
  2. Hashtags found
  3. Nr. of hashtags and total tweets relation
  4. Occurrences of hashtags, links in tweets
  5. Occurences of two hashtags in tweets - a network graph that shows which two hashtags found in the same tweet
 
__________________________________________________________________________________________________________________

#To run
$SPARK_HOME/bin/spark-submit sparkjob.py

- Some charts based on data collected on irma (not for a long time though)

![Hashtags distribution](https://github.com/phanisaripalli/spark-twitter/blob/master/img/irma/hashtags_distribution.png)

![Hashtags relation](https://github.com/phanisaripalli/spark-twitter/blob/master/img/irma/relation_hashtags.png)

![Overall relation](https://github.com/phanisaripalli/spark-twitter/blob/master/img/irma/relation_overall.png)

__________________________________________________________________________________________________________________

- Some charts based on data collected on usopen (did not collect for a lot of time) in the repo

![Nr. hashtags and total tweets](https://raw.githubusercontent.com/phanisaripalli/spark-twitter/master/charts/nr_hashtags_and_total_tweets_distribution.png)


![Hashtags occurrences - network](https://raw.githubusercontent.com/phanisaripalli/spark-twitter/master/charts/occurences_hashtags_relationship.png)
