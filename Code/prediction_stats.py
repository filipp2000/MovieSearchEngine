import pandas as pd
import numpy as np


df_rat = pd.read_csv("ratings.csv")
df_mov = pd.read_csv("movies.csv")


def stats(predictions, true, _range=0.5):
    predictions_list = []
    round_list = np.arange(0.5, 5.5, 0.5)

    for value in predictions:
        value_ori = value
        compare_diff = 99999
        value_round = 0

        for rating in round_list:
            compare_value = abs(value_ori - rating)

            if compare_value < compare_diff:  # The absolute difference value that is closest to 0 is the rating 
                # the prediction will be rounded to
                compare_diff = compare_value
                value_round = rating

        predictions_list.append(value_round)

    prediction_dict = {'PREDICTION': predictions_list, 'TRUE': list(true)}
    prediction_compare_df = pd.DataFrame(prediction_dict)

    # prediction_compare_df.to_csv('test_predictions.csv', index=False)

    prediction_length = len(prediction_compare_df)

    rating_accuracy_ranged = 0  # If the prediction was within +/- 0.5 of the actual

    for index, row in prediction_compare_df.iterrows():

        if (row.TRUE - _range) <= row.PREDICTION <= (row.TRUE + _range):
            rating_accuracy_ranged += 1

    rating_accuracy_ranged = rating_accuracy_ranged / prediction_length

    print('Rating Accuracy: ', rating_accuracy_ranged * 100)

    return prediction_compare_df


def round_predictions(predictions):
    predictions_list = []
    round_list = np.arange(0.5, 5.5, 0.5)

    for value in predictions:

        value_ori = value
        compare_diff = 99999
        value_round = 0

        for rating in round_list:
            compare_value = abs(value_ori - rating)

            if compare_value < compare_diff:  # The absolute difference value that is closest to 0 is the rating
                # the prediction will be rounded to
                compare_diff = compare_value
                value_round = rating

        predictions_list.append(value_round)
    # print(predictions_list)
    return predictions_list
