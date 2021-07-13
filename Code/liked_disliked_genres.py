import pandas as pd

final_df_mov = pd.read_csv('new_movies.csv')
df_rat = pd.read_csv("ratings.csv")
df_rat.drop('timestamp', axis='columns', inplace=True)

ratings_movies_df = df_rat.merge(final_df_mov, how='left',
                                 on='movieId').dropna()

# Getting a count of all the liked and dislike genres and transforming it into a percentage (liked genre counts / all
# liked genres counts) # If the user rated the movie 3.5+, then they liked it. If lower than 3.5, then they disliked it.
users_list = list(set(ratings_movies_df.userId))

total_user_like_df = pd.DataFrame()
total_user_dislike_df = pd.DataFrame()

for user in users_list:
    temp_df = ratings_movies_df[ratings_movies_df.userId == user]
    like_df = temp_df[temp_df.rating > 3].iloc[:, 5:]  # Only selecting the genres
    dislike_df = temp_df[temp_df.rating <= 3].iloc[:, 5:]

    liked_total_counts = 0
    liked_dict = {'userId': user, 'Action': 0, 'Adventure': 0, 'Animation': 0,
                  'Children': 0,
                  'Comedy': 0, 'Crime': 0, 'Documentary': 0,
                  'Drama': 0,
                  'Fantasy': 0, 'Film-Noir': 0, 'Horror': 0, 'IMAX': 0, 'Musical': 0, 'Mystery': 0, 'Romance': 0,
                  'Sci-Fi': 0, 'Thriller': 0, 'War': 0,
                  'Western': 0, '(no genres listed)': 0}

    disliked_total_counts = 0
    disliked_dict = {'userId': user, 'Action': 0, 'Adventure': 0, 'Animation': 0,
                     'Children': 0,
                     'Comedy': 0, 'Crime': 0, 'Documentary': 0,
                     'Drama': 0,
                     'Fantasy': 0, 'Film-Noir': 0, 'Horror': 0, 'IMAX': 0, 'Musical': 0, 'Mystery': 0, 'Romance': 0,
                     'Sci-Fi': 0, 'Thriller': 0, 'War': 0,
                     'Western': 0, '(no genres listed)': 0}

    for genre in list(like_df.columns):  # Getting all the genre counts for liked and disliked, separately
        if len(like_df) == 0:  # If the user has not given a movie a rating of 3.5 or higher
            pass

        else:
            liked_total_counts += sum(like_df[genre])

        if len(dislike_df) == 0:  # If the user has not given a movie a rating of 3 or lower
            pass

        else:
            disliked_total_counts += sum(dislike_df[genre])

    for genre in list(like_df.columns):
        if liked_total_counts == 0:
            pass

        else:
            liked_genre_total_counts = sum(like_df[genre])
            liked_dict[genre] = liked_genre_total_counts / liked_total_counts

        if disliked_total_counts == 0:
            pass

        else:
            disliked_genre_total_counts = sum(dislike_df[genre])
            disliked_dict[genre] = disliked_genre_total_counts / disliked_total_counts

    user_like_df = pd.DataFrame(liked_dict, index=[0])

    # Even though some users have not rated a movie higher or lower than 3, the zero counts will still be added for
    # complete-ness
    user_dislike_df = pd.DataFrame(disliked_dict, index=[0])

    # Concatenating the user total counts
    if len(total_user_like_df) == 0:
        total_user_like_df = user_like_df

    else:
        total_user_like_df = pd.concat([total_user_like_df, user_like_df], ignore_index=True)

    if len(total_user_dislike_df) == 0:
        total_user_dislike_df = user_dislike_df

    else:
        total_user_dislike_df = pd.concat([total_user_dislike_df, user_dislike_df], ignore_index=True)

total_user_like_df.to_csv('total_user_like_df.csv', index=False)
total_user_dislike_df.to_csv('total_user_dislike_df.csv', index=False)

# The reason for scaling was mainly to minimize any bias created by the models for
# users that have rated many movies over users that have rated only a few movies.
