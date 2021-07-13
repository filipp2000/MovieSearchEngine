import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from elasticsearch import helpers
import upload_toElastic

columns = ['Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary',
           'Drama',
           'Fantasy', 'Film-Noir', 'Horror', 'IMAX', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War',
           'Western']

df_rat = pd.read_csv("ratings.csv")
df_mov = pd.read_csv("movies.csv")

titles_list = df_mov['title'].to_list()


def get_genre_ratings(ratings, movies, genres, column_names):
    genre_ratings = pd.DataFrame()
    for genre in genres:
        genre_movies = movies[movies['genres'].str.contains(genre)]
        # print(genre_movies)
        avg_genre_votes_per_user = \
            ratings[ratings['movieId'].isin(genre_movies['movieId'])].loc[:, ['userId', 'rating']].groupby(['userId'])[
                'rating'].mean().round(2)

        genre_ratings = pd.concat([genre_ratings, avg_genre_votes_per_user], axis=1)

    genre_ratings.columns = column_names

    return genre_ratings


genre_ratings = get_genre_ratings(df_rat, df_mov, columns, columns)
# print(genre_ratings)

genre_ratings = genre_ratings.fillna(0)
X = genre_ratings[columns].values

k_means = KMeans(n_clusters=8)
clusters = k_means.fit_predict(X)
# print(predicts)
centers = k_means.cluster_centers_


# print clusters-genres-avg_ratings
def pd_centers(cols_of_interest, centers):
    colNames = list(cols_of_interest)
    colNames.append('cluster')
    # Zip with a column called 'prediction' (index)
    Z = [np.append(A, index) for index, A in enumerate(centers)]
    # Convert to pandas data frame
    df = pd.DataFrame(Z, columns=colNames)
    df['cluster'] = df['cluster'].astype(int)

    return df


df1 = pd.DataFrame(clusters, columns=['cluster'])  # df userId-cluster

df1.index.name = 'userId'
df1.index += 1  # set index to start from 1

grouped = df1.groupby('cluster')

merged_df = pd.merge(df_rat, df_mov, on="movieId", how="left")
user_titles_ratings = pd.pivot_table(merged_df, index='userId', columns='movieId',
                                     values='rating')  # df with userId-movieId-ratings


def user_titles_ratings_func():
    for cluster_num, group in grouped:
        group = group.reset_index()
        users_in_cluster = group.get('userId')
        # print(users_in_cluster)
        users_titles_clusters = user_titles_ratings.loc[users_in_cluster, :]  # users in cluster - titles

        for user_id in users_in_cluster:
            titles_ratings = user_titles_ratings.loc[user_id, :]  # Get the movies this user rates
            # print(titles_ratings)
            user_unrated_movies = titles_ratings[titles_ratings.isnull()]  # Get the unrated movies

            # What are the ratings of these movies the user did not rate?
            avg_ratings = pd.concat([user_unrated_movies, users_titles_clusters.mean()], axis=1, join='inner').loc[:, 0]
            # print(avg_ratings)

            avg_ratings.fillna(0, inplace=True)
            # print(avg_ratings)
            user_titles_ratings.loc[user_id, :].fillna(avg_ratings, inplace=True)

    df = user_titles_ratings.reset_index()
    df = df.replace('', np.nan).set_index('userId').stack().reset_index(name='rating')

    return df


final_df = user_titles_ratings_func()


print(final_df.head(100))


def upload_func(dataframe):
    es = upload_toElastic.connect_elasticsearch()

    es.indices.create(index="clustered_ratings", ignore=400)
    dataframe.insert(0, "ID", range(0, len(dataframe)))
    df_generator = upload_toElastic.df_generator(dataframe, 'clustered_ratings')
    helpers.bulk(es, df_generator)

# Run to upload final_df to elasticsearch
# upload_func(final_df)
