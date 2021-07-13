import pandas as pd
import numpy as np
from sklearn.preprocessing import MultiLabelBinarizer

df_rat = pd.read_csv("ratings.csv")
df_mov = pd.read_csv("movies.csv")

columns = ['(no genres listed)', 'Action', 'Adventure', 'Animation', 'Children', 'Comedy', 'Crime', 'Documentary',
           'Drama',
           'Fantasy', 'Film-Noir', 'Horror', 'IMAX', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War',
           'Western']


# ohe genres
def func(genres):
    return genres.split('|')


def ohe_genres():
    series = df_mov['genres'].apply(func)

    df_mov['new_genres'] = series.values
    # print(df_mov.head())

    multilabel_binarizer = MultiLabelBinarizer()
    multilabel_binarizer.fit(df_mov['new_genres'])

    # transform target variable
    y = multilabel_binarizer.transform(df_mov['new_genres'])

    # print(multilabel_binarizer.inverse_transform(y)[0])

    ohe_df = pd.DataFrame(data=y,
                          index=np.arange(0, 9125),
                          columns=columns)

    df_final = pd.concat([df_mov, ohe_df], axis=1)
    df_final.drop('genres', axis='columns', inplace=True)
    # print(df_final.head(10).to_string())
    return df_final


df_final = ohe_genres()
df_final.to_csv('new_movies.csv', index=False)
