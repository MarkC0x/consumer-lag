# Calculate Consumer Lag from Java Application

Simple example of how to programatically calcuate Consumer Group Lag by partition
from within a Java application:

```
Topic=demo-topic :: Partition=0 End Offset=13 Current Offset=6 Lag=7
Topic=demo-topic :: Partition=1 End Offset=9 Current Offset=9 Lag=0
Topic=demo-topic :: Partition=2 End Offset=9 Current Offset=2 Lag=7
Topic=demo-topic :: Partition=3 End Offset=20 Current Offset=14 Lag=6
Topic=demo-topic :: Partition=4 End Offset=23 Current Offset=20 Lag=3
Topic=demo-topic :: Partition=5 End Offset=10 Current Offset=7 Lag=3
```

To use, add in your own information in the following places in the file:

```
"pkc-xxxxx.us-west-1.aws.confluent.cloud:9092"
"<Topic Name>"
"<Consumer Group Name>"
"<API-Key>"
"<API-Secret>"
```
