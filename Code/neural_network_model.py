import numpy as np
import keras
import prepare_dataset
import prediction_stats

user_liked_genres = keras.Input(shape=(20,))
user_disliked_genres = keras.Input(shape=(20,))
movie_genres = keras.Input(shape=(20,))

# Liked genres Input:
liked_input = keras.layers.Dense(20, activation='relu')(user_liked_genres)
liked_hidden_1 = keras.layers.Dense(40, activation='relu')(liked_input)
liked_hidden_2 = keras.layers.Dense(40, activation='relu')(liked_hidden_1)

# Disliked genres Input:
disliked_input = keras.layers.Dense(20, activation='relu')(user_disliked_genres)
disliked_hidden_1 = keras.layers.Dense(40, activation='relu')(disliked_input)
disliked_hidden_2 = keras.layers.Dense(40, activation='relu')(disliked_hidden_1)

# Movie genres Input:
movie_input = keras.layers.Dense(20, activation='relu')(movie_genres)
movie_hidden_1 = keras.layers.Dense(40, activation='relu')(movie_input)
movie_hidden_2 = keras.layers.Dense(40, activation='relu')(movie_hidden_1)

# Merging:
merged_model = keras.layers.concatenate([liked_hidden_2, disliked_hidden_2, movie_hidden_2])
merged_model_hidden_1 = keras.layers.Dense(120, activation='relu')(merged_model)
merged_model_hidden_2 = keras.layers.Dense(60, activation='relu')(merged_model_hidden_1)
merged_model_hidden_3 = keras.layers.Dense(40, activation='relu')(merged_model_hidden_2)

# Output Layer:
output_rating = keras.layers.Dense(1, activation='sigmoid')(merged_model_hidden_3)  # We use a sigmoid on the output
# layer to ensure our network output is between 0 and 1

# Molding the Model together:
genres_model = keras.Model(inputs=[user_liked_genres, user_disliked_genres, movie_genres], outputs=output_rating)

# Compiling the Model:
genres_model.compile(optimizer=keras.optimizers.Adam(learning_rate=0.001), loss='mean_squared_error')

# Generating the datasets:
genres_like, genres_dislike, genres_movie, ratings = prepare_dataset.merge_shuffle()


def train_split_test_model():
    train_split = 0.5
    split_index = int(len(ratings) * train_split)

    genres_like_train = genres_like.iloc[: split_index, :]
    genres_like_test = genres_like.iloc[split_index:, :]

    genres_dislike_train = genres_dislike.iloc[: split_index, :]
    genres_dislike_test = genres_dislike.iloc[split_index:, :]

    genres_movie_train = genres_movie.iloc[: split_index, :]
    genres_movie_test = genres_movie.iloc[split_index:, :]

    ratings_scaled = np.array(ratings) / 5  # ratings scaled(0-1)
    ratings_scaled_train = ratings_scaled[: split_index]
    ratings_scaled_test = ratings_scaled[split_index:]
    X_train = [genres_like_train, genres_dislike_train, genres_movie_train]
    y_train = ratings_scaled_train

    X_test = [genres_like_test, genres_dislike_test, genres_movie_test]
    y_test = ratings_scaled_test

    return X_train, X_test, y_train, y_test


'''
# Run for the train_split_test_model predictions(use it for larger size movie file)

X_train, X_test, y_train, y_test = train_split_test_model()
# Fit the keras model on the dataset
genres_model.fit(x=X_train, y=y_train, epochs=epochs, batch_size=batch_size)

# evaluate the keras model
accuracy = genres_model.evaluate(X_train, y_train)
print('Accuracy: %.2f' % (accuracy * 1000 * 2), '%')

genres_model_predictions = (genres_model.predict(x=X_test)) * 5
prediction_compare_df = prediction_stats.stats(genres_model_predictions, y_test * 5)
'''

# We train the final model on all available data.

ratings_scaled = np.array(ratings) / 5  # ratings scaled(0-1)

batch_size = 500
epochs = 20
# epochs = 1000

# Fit the keras model on the dataset
X = [genres_like, genres_dislike, genres_movie]
y = ratings_scaled
genres_model.fit(x=X, y=y, epochs=epochs, batch_size=batch_size)

genres_model.save('trained_model.h5', overwrite= True, include_optimizer= True) # save model
genres_model = tf.keras.models.load_model('trained_model.h5', compile=True) # load model

# Evaluate the keras model
accuracy = genres_model.evaluate(X, y)
print('Accuracy: %.2f' % (accuracy * 1000 * 2), '%')

genres_model_predictions = (genres_model.predict(x=X)) * 5

prediction_compare_df = prediction_stats.stats(genres_model_predictions, y * 5)
# print(prediction_compare_df)
