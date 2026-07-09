import os
import dlt
import requests
from itertools import islice

BASE_URL = "https://jaffle-shop.scalevector.ai/api/v1"


def fetch_all(endpoint: str):
    url = f"{BASE_URL}/{endpoint}"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()


def chunk(data, size):
    it = iter(data)
    while True:
        batch = list(islice(it, size))
        if not batch:
            break
        yield batch


@dlt.resource(name="customers", parallelized=True, write_disposition="merge")
def customers(chunk_size=5000):
    data = fetch_all("customers")
    for c in chunk(data, chunk_size):
        yield c


@dlt.resource(name="orders", parallelized=True, write_disposition="merge")
def orders(chunk_size=5000):
    data = fetch_all("orders")
    for c in chunk(data, chunk_size):
        yield c


@dlt.source
def jaffle_source():
    return [customers(), orders()]


def configure_perf():
    os.environ["EXTRACT__WORKERS"] = "4"
    os.environ["NORMALIZE__WORKERS"] = "4"
    os.environ["LOAD__WORKERS"] = "8"

    os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "20000"
    os.environ["NORMALIZE__DATA_WRITER__BUFFER_MAX_ITEMS"] = "20000"

    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "5000"
    os.environ["NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS"] = "5000"

    os.environ["DATA_WRITER__FILE_MAX_BYTES"] = "20000000"
    os.environ["NORMALIZE__DATA_WRITER__FILE_MAX_BYTES"] = "20000000"

    os.environ["EXTRACT__NEXT_ITEM_MODE"] = "round_robin"


def run():
    configure_perf()

    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop_fastest",
        destination="duckdb",
        dataset_name="jaffle_fastest",
    )

    info = pipeline.run(jaffle_source())
    print(info)


if __name__ == "__main__":
    run()
