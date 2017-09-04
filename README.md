<h1>Spark app fo twitter analytics</h1>

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

#Some charts based on data colelcted on usopen (did not collect for a lot of time) in the repo
