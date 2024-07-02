from pandas import read_sql

from dagster import Definitions, SourceAsset, asset, define_asset_job

from .mylib import pickle_to_s3, create_db_connection, train_recommender_model

raw_users = SourceAsset(key="raw_users")


@asset(deps=[raw_users])
def users() -> None:
    raw_users_df = read_sql("select * from raw_users", con=create_db_connection())
    users_df = raw_users_df.dropna()
    users_df.to_sql(name="users", con=create_db_connection())


@asset(deps=[users])
def user_recommender_model() -> None:
    users_df = read_sql("select * from users", con=create_db_connection())
    users_recommender_model = train_recommender_model(users_df)
    pickle_to_s3(users_recommender_model, key="users_recommender_model")


users_recommender_job = define_asset_job("users_recommenders_job", selection="*")


defs = Definitions(
    assets=[users, user_recommender_model],
    jobs=[users_recommender_job],
)
