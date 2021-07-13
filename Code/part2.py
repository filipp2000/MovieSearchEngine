import numpy
import pandas as pd
from elasticsearch import helpers
import upload_toElastic

df_rat = pd.read_csv("ratings.csv")
df_mov = pd.read_csv("movies.csv")

es = upload_toElastic.connect_elasticsearch()


def upload_func(dataframe):
    es.indices.create(index="ratings", ignore=400)
    dataframe.insert(0, "ID", range(0, len(dataframe)))
    df_generator = upload_toElastic.df_generator(dataframe, 'ratings')
    helpers.bulk(es, df_generator)


def user_request():
    user_input = input("Search Elasticsearch: ").strip()
    return user_input


def user_request2():
    while True:
        user_id = input("Give user id: ").strip()

        if user_id.isdigit() and 0 < int(user_id) < 672:
            return user_id

        else:
            print("UserID is an integer(1,671).Try again!")


def set_movies_query(user_req):
    query_body = {
        "query": {
            "multi_match": {
                "type": "best_fields",
                "query": user_req,
                "fields": []
            }
        }
    }
    return query_body


def set_ratings_query(user_req, moviesId):
    query_body = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"userId": user_req}},
                    {"terms": {"movieId": moviesId}}
                ]
            }
        }
    }
    return query_body


def get_movies_res():
    result = [movies_res.get("hits").get("hits")]

    score_list = []
    movieId_list = []
    search_result = []
    for index in result:
        for key in index:
            movieId = key.get("_source").get("movieId")
            res = key.get("_source")
            my_value = key.get("_score")

            score_list.append(my_value)
            movieId_list.append(movieId)
            search_result.append(res)

    dictionary = dict()
    dictionary['my_score'] = score_list
    dictionary['my_movieId'] = movieId_list
    dictionary['search_result'] = search_result

    return dictionary


def get_ratings_res(userID):
    result = [userID.get("hits").get("hits")]

    rating_list = []
    rat_movieId_list = []
    for index in result:
        for key in index:
            rating = key.get("_source").get("rating")
            rat_movieId = key.get("_source").get("movieId")

            rating_list.append(rating)
            rat_movieId_list.append(rat_movieId)

    dictionary = dict()
    dictionary['my_rating'] = rating_list
    dictionary['rat_movieId'] = rat_movieId_list

    return dictionary


new_df = pd.merge(df_mov, df_rat, on="movieId", how="outer")


def avg_rating(search_movieId):  # returns the average rating
    grouped_df = new_df[["userId", "rating", "genres", "movieId"]].groupby("movieId")
    avg_list = []

    for mov_id in search_movieId:
        group = grouped_df.get_group(mov_id)  # selects the group with the search movieId
        avg = group.get("rating").mean()
        avg_list.append(avg)

    return avg_list


def find_index_list(movieId, rat_movieId):  # returns the position of the movieId found in rat_movieId

    d = {k: v for v, k in enumerate(movieId)}  # dict with key = list_item and value = position
    indexes = [d[k] for k in rat_movieId]

    return indexes


def newscore_list(indexes, to_modify, replacements):
    avg_res = avg_rating(get_movies_res().get("my_movieId"))
    for i in range(len(to_modify)):
        to_modify[i] += avg_res[i]  # adds the average rating to bm25 score

    for (index, replacement) in zip(indexes, replacements):
        to_modify[index] = to_modify[index] + replacement

    return to_modify


def modify_result(search_res, new_similarity):  # sorts the search result according to the new similarity

    search_res = numpy.array(search_res)
    new_similarity = numpy.array(new_similarity)
    n = len(new_similarity)
    indices = new_similarity.argsort()[::-1][:n]  # Returns the indices that would sort this array in desc order

    result = search_res[indices]

    return result


def similarity_func(ratings):
    userID = search_func(ratings)

    bm25_score = get_movies_res().get("my_score")  # taking score key from dict
    movieId = get_movies_res().get("my_movieId")  # movieIds from the search result
    print("\nOld score(bm25): ", bm25_score)

    final_result = get_movies_res().get('search_result')

    user_rating = get_ratings_res(userID).get("my_rating")  # list with the user ratings of movies
    rat_movieId = get_ratings_res(userID).get(
        "rat_movieId")  # list with the movieIds of the rated movies from the user

    index_list = find_index_list(movieId, rat_movieId)
    new_score = newscore_list(index_list, bm25_score, user_rating)
    new_result = modify_result(final_result, new_score)

    new_score.sort(reverse=True)
    print("New score: ", new_score)

    return new_result


def get_results(result):
    for num, doc in enumerate(result):  # iterate the nested dictionaries inside the result list
        print("\nResult ID: ", num)
        for key, value in doc.items():
            print(key, ": ", value)
    print("\nTotal hits: ", len(result))


def search_func(ratings):
    userId = es.search(index=ratings, body=set_ratings_query(user_request2(), get_movies_res().get("my_movieId")))

    return userId


movies_res = es.search(index="movies", body=set_movies_query(user_request()), size=10000)  # movies result


def search_part2():
    res = similarity_func('ratings')
    get_results(res)


def search_part3():
    res = similarity_func('clustered_ratings')
    get_results(res)


def search_part4():
    res = similarity_func('predicted_ratings')
    get_results(res)


# Run to upload ratings df to elasticsearch
# upload_func(df_rat)

# Search for part2
search_part2()

# Search for part3
# search_part3()


# Search for part4
# search_part4()
