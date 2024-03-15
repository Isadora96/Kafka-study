# Kafka-study

## Resources 

- [Apache Kafka Series - Learn Apache Kafka for Beginners v3](https://www.udemy.com/course/apache-kafka/?couponCode=2021PM25)
- [Confluent Kafka Github](https://github.com/confluentinc/confluent-kafka-python)
- [Confluent Kafka python](https://developer.confluent.io/get-started/python#introduction)
- [Confluent Kafka python example](https://pandeyshikha075.medium.com/getting-started-with-confluent-kafka-in-python-579b708801e7)
- [Upstash free Kafka](https://console.upstash.com)
- [Bonsai for elasticsearch/opensearch](https://bonsai.io/pricing)
- [Opensearch python](https://opensearch.org/docs/latest/clients/python-low-level/)

## Getting Started

### Prerequisites

`Python 3.x`

### On root folder

- Copy file `.env-example` into a `.env` file
- Fill the env variables in `.env`
- Create Pipfile if not exist
```
    pipenv install 
    pipenv shell
    pipenv install `package_name`
```
- Create Virtual Environment (VENV)
```sh
    pip install --upgrade pipenv
    pipenv shell
    PIPENV_VENV_IN_PROJECT=1 pipenv install
```
- Run `export $(cat .env | xargs)` in order to export the environment variables
- Run `python -m kafka.main` to start the project.


