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

### Inverted index

```bash
spark-submit inverted_index.py
```

```bash
spark-submit word_count.py
```

