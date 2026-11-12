from psycopg2 import connect


def create_conn(config):
    return connect(**config)