import pandas as pd
from sklearn.utils import shuffle


def merge_shuffle():
    final_df_mov = pd.read_csv('new_movies.csv')
    total_user_like_df = pd.read_csv('total_user_like_df.csv')
    total_user_dislike_df = pd.read_csv('total_user_dislike_df.csv')

    df_rat = pd.read_csv("ratings.csv")
    df_rat.drop('timestamp', axis='columns', inplace=True)

    df_rat = shuffle(df_rat)  # shuffle ratings df
    # print(df_rat)

    df_rat = df_rat.merge(final_df_mov, how='left', on='movieId').dropna()

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

    df_rat = df_rat.merge(total_user_like_df, how='left', on='userId').dropna()

    dislike_columns = list(total_user_dislike_df.columns)
    dislike_columns_modified = []

    for column in dislike_columns:
        if column == 'userId':
            dislike_columns_modified.append('userId')
        else:
            modify_column = 'user_dislike_' + column
            dislike_columns_modified.append(modify_column)

    total_user_dislike_df.columns = dislike_columns_modified

    # Merging all the DFs to create one final DF:
    df_rat = df_rat.merge(total_user_dislike_df, how='left', on='userId').dropna()  # 10004 x 65 columns

    like_columns_modified.remove('userId')
    dislike_columns_modified.remove('userId')
    like_columns.remove('userId')

    genres_like = df_rat.loc[:, like_columns_modified]
    #print(genres_like)
    genres_dislike = df_rat.loc[:, dislike_columns_modified]
    genres_movie = df_rat.loc[:, like_columns]
    print(genres_movie.head().to_string())

    ratings = list(df_rat.rating)

    return genres_like, genres_dislike, genres_movie, ratings

