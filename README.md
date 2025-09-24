## Lab 1-2

### Build

```bash
docker build -t pyspark-lab .
```

### Run

```bash
docker run -d -it -v "$(pwd):/app" --name pyspark-dev pyspark-lab
```

### Stop and delete

```bash
docker rm -f pyspark-dev
```

### Connect

```bash
docker exec -it pyspark-dev bash
```

### Launch scripts

```bash
spark-submit lab1/inverted_index.py
```

```bash
spark-submit lab1/word_count.py
```

```bash
spark-submit lab2/pagerank.py lab2/graph.txt 10 0.85
```

## Lab4

### Run Kafka Environment

```bash
docker-compose up -d
```

### Launch Scripts

**Synchronous version:**

```bash
python lab4/consumer.py
```

```bash
python lab4/producer.py
```

**Asynchronous version:**

```bash
python lab4/async-producer-consumer.py
```

### Stop and delete environment

```bash
docker-compose down
```

# Мешканці мого потоку!

Мій код – #####

Воно вам не треба...

![Смішна картинка](https://i.imgflip.com/64slsz.png?a475992)