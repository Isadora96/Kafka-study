# Kafka-study

## Resources 

- [Confluent Kafka Github](https://github.com/confluentinc/confluent-kafka-python)
- [Confluent Kafka python](https://developer.confluent.io/get-started/python#introduction)
- [Confluent Kafka python example](https://pandeyshikha075.medium.com/getting-started-with-confluent-kafka-in-python-579b708801e7)
- [Upstash free Kafka](https://console.upstash.com)

## Getting Started

### Prerequisites

`Python 3.x`

### On root folder

- Copy file `.env-example` into a `.env` file
- Fill the env variables in `.env`
- Run `export $(cat .env | xargs)` in order to export the environment variables
- Create Virtual Environment (VENV)
```sh
    python3.x -m venv venv
    source venv/bin/activate
```
- Run `pip install confluent-kafka` to install dependencie
- Run `python -m kafka.main` to start the project.


