"""Airflow DAG wiring for crypto data collection and staging loads."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import pipeline_tasks as tasks


with DAG(
    dag_id="crypto_data_pipeline",
    description="Fetch crypto data, enrich it, load staging tables, and populate DW dims/facts.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["crypto", "data", "staging"],
) as dag:
  with TaskGroup("fetch") as fetch_group:
    fetch_fear_and_greed = PythonOperator(
        task_id="fetch_fear_and_greed",
        python_callable=tasks.fetch_fear_and_greed_history,
        do_xcom_push=False,
    )
    fetch_reddit = PythonOperator(
        task_id="fetch_reddit_bitcoin",
        python_callable=tasks.fetch_reddit_bitcoin,
        op_kwargs={"post_limit": 200, "comment_limit": 200},
        do_xcom_push=False,
    )
    fetch_klines = PythonOperator(
        task_id="fetch_btcusdt_klines",
        python_callable=tasks.fetch_btcusdt_5m_history,
        op_kwargs={"total": 25000},
        do_xcom_push=False,
    )
    fetch_eth_klines = PythonOperator(
        task_id="fetch_ethusdt_klines",
        python_callable=tasks.fetch_symbol_5m_history,
        op_kwargs={"symbol": "ETHUSDT", "total": 25000},
        do_xcom_push=False,
    )
    fetch_bnb_klines = PythonOperator(
        task_id="fetch_bnbusdt_klines",
        python_callable=tasks.fetch_symbol_5m_history,
        op_kwargs={"symbol": "BNBUSDT", "total": 25000},
        do_xcom_push=False,
    )

  with TaskGroup("transform") as transform_group:
    score_reddit = PythonOperator(
        task_id="score_reddit_sentiment",
        python_callable=tasks.score_reddit_sentiment,
        do_xcom_push=False,
    )
    compute_klines_ind = PythonOperator(
        task_id="compute_btcusdt_indicators",
        python_callable=tasks.compute_kline_indicators,
        do_xcom_push=False,
    )
    compute_eth_ind = PythonOperator(
        task_id="compute_ethusdt_indicators",
        python_callable=tasks.compute_kline_indicators,
        op_kwargs={"symbol": "ETHUSDT"},
        do_xcom_push=False,
    )
    compute_bnb_ind = PythonOperator(
        task_id="compute_bnbusdt_indicators",
        python_callable=tasks.compute_kline_indicators,
        op_kwargs={"symbol": "BNBUSDT"},
        do_xcom_push=False,
    )

  with TaskGroup("load_staging") as load_staging_group:
    load_fear_and_greed_mysql = PythonOperator(
        task_id="load_fear_and_greed_mysql",
        python_callable=tasks.load_fear_and_greed_into_mysql,
        do_xcom_push=False,
    )
    load_reddit_posts = PythonOperator(
        task_id="load_reddit_posts_mysql",
        python_callable=tasks.load_reddit_posts,
        do_xcom_push=False,
    )
    load_reddit_comments = PythonOperator(
        task_id="load_reddit_comments_mysql",
        python_callable=tasks.load_reddit_comments,
        do_xcom_push=False,
    )
    load_reddit_posts_scored = PythonOperator(
        task_id="load_reddit_posts_scored_mysql",
        python_callable=tasks.load_reddit_posts_scored,
        do_xcom_push=False,
    )
    load_reddit_comments_scored = PythonOperator(
        task_id="load_reddit_comments_scored_mysql",
        python_callable=tasks.load_reddit_comments_scored,
        do_xcom_push=False,
    )
    load_klines = PythonOperator(
        task_id="load_btcusdt_klines_mysql",
        python_callable=tasks.load_klines,
        do_xcom_push=False,
    )
    load_klines_ind = PythonOperator(
        task_id="load_btcusdt_indicators_mysql",
        python_callable=tasks.load_kline_indicators,
        do_xcom_push=False,
    )
    load_eth_klines = PythonOperator(
        task_id="load_ethusdt_klines_mysql",
        python_callable=tasks.load_klines_for_symbol,
        op_kwargs={"symbol": "ETHUSDT"},
        do_xcom_push=False,
    )
    load_eth_ind = PythonOperator(
        task_id="load_ethusdt_indicators_mysql",
        python_callable=tasks.load_kline_indicators_for_symbol,
        op_kwargs={"symbol": "ETHUSDT"},
        do_xcom_push=False,
    )
    load_bnb_klines = PythonOperator(
        task_id="load_bnbusdt_klines_mysql",
        python_callable=tasks.load_klines_for_symbol,
        op_kwargs={"symbol": "BNBUSDT"},
        do_xcom_push=False,
    )
    load_bnb_ind = PythonOperator(
        task_id="load_bnbusdt_indicators_mysql",
        python_callable=tasks.load_kline_indicators_for_symbol,
        op_kwargs={"symbol": "BNBUSDT"},
        do_xcom_push=False,
    )

  with TaskGroup("load_dim") as load_dim_group:
    load_dim_symbol = PythonOperator(
        task_id="load_dim_symbol_mysql",
        python_callable=tasks.etl_dim_symbol,
        do_xcom_push=False,
    )
    load_dim_interval = PythonOperator(
        task_id="load_dim_interval_mysql",
        python_callable=tasks.etl_dim_interval,
        do_xcom_push=False,
    )
    load_dim_times = PythonOperator(
        task_id="load_dim_times_mysql",
        python_callable=tasks.etl_dim_time_from_klines,
        do_xcom_push=False,
    )
    load_dim_symbol_eth = PythonOperator(
        task_id="load_dim_symbol_ethusdt",
        python_callable=tasks.etl_dim_symbol,
        op_kwargs={"symbol_code": "ETHUSDT", "base_asset": "ETH", "quote_asset": "USDT"},
        do_xcom_push=False,
    )
    load_dim_times_eth = PythonOperator(
        task_id="load_dim_times_ethusdt",
        python_callable=tasks.etl_dim_time_from_klines,
        op_kwargs={"symbol": "ETHUSDT"},
        do_xcom_push=False,
    )
    load_dim_symbol_bnb = PythonOperator(
        task_id="load_dim_symbol_bnbusdt",
        python_callable=tasks.etl_dim_symbol,
        op_kwargs={"symbol_code": "BNBUSDT", "base_asset": "BNB", "quote_asset": "USDT"},
        do_xcom_push=False,
    )
    load_dim_times_bnb = PythonOperator(
        task_id="load_dim_times_bnbusdt",
        python_callable=tasks.etl_dim_time_from_klines,
        op_kwargs={"symbol": "BNBUSDT"},
        do_xcom_push=False,
    )

  with TaskGroup("load_fact") as load_fact_group:
    fact_ohlv = PythonOperator(
        task_id="load_fact_ohlv_mysql",
        python_callable=tasks.etl_fact_ohlv,
        do_xcom_push=False,
    )
    fact_indicator = PythonOperator(
        task_id="load_fact_indicator_mysql",
        python_callable=tasks.etl_fact_indicator,
        do_xcom_push=False,
    )
    fact_ohlv_eth = PythonOperator(
        task_id="load_fact_ohlv_ethusdt",
        python_callable=tasks.etl_fact_ohlv,
        op_kwargs={"symbol_code": "ETHUSDT", "base_asset": "ETH", "quote_asset": "USDT"},
        do_xcom_push=False,
    )
    fact_indicator_eth = PythonOperator(
        task_id="load_fact_indicator_ethusdt",
        python_callable=tasks.etl_fact_indicator_for_symbol,
        op_kwargs={"symbol_code": "ETHUSDT", "base_asset": "ETH", "quote_asset": "USDT"},
        do_xcom_push=False,
    )
    fact_ohlv_bnb = PythonOperator(
        task_id="load_fact_ohlv_bnbusdt",
        python_callable=tasks.etl_fact_ohlv,
        op_kwargs={"symbol_code": "BNBUSDT", "base_asset": "BNB", "quote_asset": "USDT"},
        do_xcom_push=False,
    )
    fact_indicator_bnb = PythonOperator(
        task_id="load_fact_indicator_bnbusdt",
        python_callable=tasks.etl_fact_indicator_for_symbol,
        op_kwargs={"symbol_code": "BNBUSDT", "base_asset": "BNB", "quote_asset": "USDT"},
        do_xcom_push=False,
    )
    dim_news = PythonOperator(
        task_id="etl_dim_news",
        python_callable=tasks.etl_dim_news,
        do_xcom_push=False,
    )
    dim_comments = PythonOperator(
        task_id="etl_dim_comments",
        python_callable=tasks.etl_dim_comments,
        do_xcom_push=False,
    )
    fact_news_sent = PythonOperator(
        task_id="etl_fact_news_sentiment",
        python_callable=tasks.etl_fact_news_sentiment,
        do_xcom_push=False,
    )
    fact_comment_sent = PythonOperator(
        task_id="etl_fact_comment_sentiment",
        python_callable=tasks.etl_fact_comment_sentiment,
        do_xcom_push=False,
    )

  # DAG FLOW (task_ids unchanged)
  fetch_fear_and_greed >> load_fear_and_greed_mysql

  # Reddit branch: fetch -> score -> load dims/facts (after ETL)
  fetch_reddit >> score_reddit
  score_reddit >> [load_reddit_posts_scored, load_reddit_comments_scored]
  score_reddit >> [load_reddit_posts, load_reddit_comments]
  [load_reddit_posts, load_reddit_posts_scored] >> dim_news >> fact_news_sent
  [load_reddit_comments, load_reddit_comments_scored] >> dim_comments >> fact_comment_sent

  # Klines branch: fetch -> indicators -> load staging -> load dims/facts
  fetch_klines >> compute_klines_ind
  compute_klines_ind >> load_klines_ind
  fetch_klines >> load_klines

  load_klines >> [load_dim_times, load_dim_symbol, load_dim_interval]
  [load_dim_times, load_dim_symbol, load_dim_interval, load_klines] >> fact_ohlv
  [load_dim_times, load_dim_symbol, load_dim_interval, load_klines_ind] >> fact_indicator

  # ETH branch
  fetch_eth_klines >> [compute_eth_ind, load_eth_klines]
  compute_eth_ind >> load_eth_ind
  load_eth_klines >> [load_dim_times_eth, load_dim_symbol_eth, load_dim_interval]
  [load_dim_times_eth, load_dim_symbol_eth, load_dim_interval, load_eth_klines] >> fact_ohlv_eth
  [load_dim_times_eth, load_dim_symbol_eth, load_dim_interval, load_eth_ind] >> fact_indicator_eth

  # BNB branch
  fetch_bnb_klines >> [compute_bnb_ind, load_bnb_klines]
  compute_bnb_ind >> load_bnb_ind
  load_bnb_klines >> [load_dim_times_bnb, load_dim_symbol_bnb, load_dim_interval]
  [load_dim_times_bnb, load_dim_symbol_bnb, load_dim_interval, load_bnb_klines] >> fact_ohlv_bnb
  [load_dim_times_bnb, load_dim_symbol_bnb, load_dim_interval, load_bnb_ind] >> fact_indicator_bnb
