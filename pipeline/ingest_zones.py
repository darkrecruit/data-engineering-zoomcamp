#!/usr/bin/env python
import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string",
}


@click.command()
@click.option("--pg-user", default="root", envvar="PG_USER", help="PostgreSQL user")
@click.option("--pg-pass", default="root", envvar="PG_PASS", help="PostgreSQL password")
@click.option(
    "--pg-host", default="localhost", envvar="PG_HOST", help="PostgreSQL host"
)
@click.option(
    "--pg-port", default=5432, type=int, envvar="PG_PORT", help="PostgreSQL port"
)
@click.option(
    "--pg-db", default="ny_taxi", envvar="PG_DB", help="PostgreSQL database name"
)
@click.option(
    "--target-table",
    default="zones",
    envvar="TARGET_TABLE",
    help="Target table name",
)
@click.option(
    "--chunksize",
    default=100000,
    type=int,
    envvar="CHUNKSIZE",
    help="Chunk size for reading CSV",
)
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, chunksize):
    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        iterator=True,
        chunksize=chunksize,
    )

    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace")
            first = False

        df_chunk.to_sql(name=target_table, con=engine, if_exists="append")


if __name__ == "__main__":
    run()
