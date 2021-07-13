import pandas as pd
import tensorflow as tf
from prediction_stats import round_predictions
import upload_toElastic
from elasticsearch import helpers


def rating_predictions(userId):
    df_rat = pd.read_csv("ratings.csv")
    df_rat.drop('timestamp', axis='columns', inplace=True)

    final_df_mov = pd.read_csv('new_movies.csv')

    # Gathering all the movies in the dataset:
    not_watched = list(final_df_mov.movieId)

    # Selecting all movies that have been seen by the user:
    df_rat = df_rat[df_rat.userId == userId]
    # print(df_rat)

    df_rat = df_rat.merge(final_df_mov, how='left', on='movieId').dropna()

    watched = list(df_rat.movieId)
    # print(watched)

    for movie in watched:
        if movie in not_watched:
            not_watched.remove(movie)

    total_user_like_df = pd.read_csv('total_user_like_df.csv')
    total_user_dislike_df = pd.read_csv('total_user_dislike_df.csv')

    # Selecting from total_user_like_df and total_user_dislike_df to isolate only the userId input:
    total_user_like_df = total_user_like_df[total_user_like_df.userId == userId]
    total_user_dislike_df = total_user_dislike_df[total_user_dislike_df.userId == userId]

    # Changing the columns names to differentiate between the columns of total_user_like_df and total_user_dislike_df:

    like_columns = list(total_user_like_df.columns)
    like_columns_modified = []

    for column in like_columns:
        if column == 'userId':
            like_columns_modified.append('userId')
        else:
            modify_column = 'user_like_' + column
            like_columns_modified.append(modify_column)

    total_user_like_df.columns = like_columns_modified

    dislike_columns = list(total_user_dislike_df.columns)
    dislike_columns_modified = []

    for column in dislike_columns:
        if column == 'userId':
            dislike_columns_modified.append('userId')
        else:
            modify_column = 'user_dislike_' + column
            dislike_columns_modified.append(modify_column)

    total_user_dislike_df.columns = dislike_columns_modified

    not_watched_df = pd.DataFrame({'movieId': not_watched}, index=list(range(len(not_watched))))

    not_watched_df = not_watched_df.merge(final_df_mov, how='left', on='movieId').dropna()

    # Adding a userId column to the template DF so that merging is possible with total_user_like_df,
    # total_user_dislike_df, and like_dislike_tags
    not_watched_df['userId'] = userId

    not_watched_df = not_watched_df.merge(total_user_like_df, how='left', on='userId').dropna()
    not_watched_df = not_watched_df.merge(total_user_dislike_df, how='left', on='userId').dropna()

    like_columns_modified.remove('userId')

    dislike_columns_modified.remove('userId')
    like_columns.remove('userId')

    # print(template_df)

    genres_like_input = not_watched_df.loc[:, like_columns_modified]
    # print(genres_like_input.head(100).to_string())
    genres_dislike_input = not_watched_df.loc[:, dislike_columns_modified]
    genres_movie_input = not_watched_df.loc[:, like_columns]

    # Saving the movieId list:
    movieId_list = list(not_watched_df.movieId)

    # Load model

    genres_model = tf.keras.models.load_model('trained_model.h5', compile=True)  # load model
    X = [genres_like_input, genres_dislike_input, genres_movie_input]
    genres_model_predictions = (genres_model.predict(x=X)) * 5
    genres_model_predictions_list = []

    for prediction in genres_model_predictions:
        genres_model_predictions_list.append(prediction[0])

    genres_model_predictions_list = round_predictions(genres_model_predictions_list)
    # print(genres_model_predictions_list)
    # print(len(genres_model_predictions_list))

    # predictions_df['userId'] = userId
    predictions_df = pd.DataFrame({'userId': userId, 'movieId': movieId_list,
                                   'genres_predictions': genres_model_predictions_list})

    return predictions_df


df_rat = pd.read_csv("ratings.csv")
userId_list = list(set(df_rat.userId))

# Loop through all users and save the predictions
appended_data = []
for user in userId_list:
    predict_df = rating_predictions(user)
    appended_data.append(predict_df)

full_predict_df = pd.concat(appended_data)

full_predict_df.to_csv('unrated_movies_predictions.csv', index=False)


def upload_final_df():
    df_rat.drop('timestamp', axis='columns', inplace=True)
    unrated_movies_predictions = pd.read_csv('unrated_movies_predictions.csv')

    final_df = pd.concat([unrated_movies_predictions, df_rat], ignore_index=True)
    final_df.to_csv('part4_final_df', index=False)

    es = upload_toElastic.connect_elasticsearch()
    es.indices.create(index="predicted_ratings", ignore=400)
    final_df.insert(0, "ID", range(0, len(final_df)))
    df_generator = upload_toElastic.df_generator(final_df, 'predicted_ratings')
    helpers.bulk(es, df_generator)

# Run to upload final_df to elasticsearch
# upload_func(final_df)
# upload_final_df()
