import random
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from flask import jsonify, request
import logging
from services.content_qa import fetch_movie_by_id, fetch_movie_by_title, fetch_avg_rating_by_movie_id, content_based_recommendation
import nltk
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize


nltk.download('punkt')


logging.basicConfig(level=logging.INFO)

ps = PorterStemmer()

def preprocess_text(text):

    words = word_tokenize(text)
    # stem extraction
    stemmed_words = [ps.stem(word) for word in words]
    return ' '.join(stemmed_words)

def get_system_initiative():
    genre = request.args.get('genre')
    user_id = request.args.get('user_id')

    logging.info(f"System Initiative - User ID: {user_id}, Genre: {genre}")

    if not genre or not user_id:
        return jsonify({"error": "Genre and user ID are required"}), 400

    try:

        movie = content_based_recommendation(genre)
    except ValueError as e:
        logging.error(e)
        return jsonify({"error": str(e)}), 400

    if not movie:
        return jsonify({"error": "No movie found for the given genre"}), 404

    movie_dict = {
        "movie_title": movie[1],
        "overview": movie[2],
        "director": movie[3],
        "cast": movie[4],
        "release_date": movie[5]
    }

    # Get the average rating information for the movie
    avg_rating = fetch_avg_rating_by_movie_id(movie[0])
    movie_dict["rating"] = round(avg_rating, 2) if avg_rating is not None else "N/A"

    response = {
        "recommendation": f"I recommend '{movie_dict['movie_title']}'",
        "details": [
            f"The genre of this movie is {genre}.",
            f"The plot is {movie_dict['overview']}.",
            f"The director is {movie_dict['director']}.",
            f"The cast includes {movie_dict['cast']}.",
            f"The release date is {movie_dict['release_date']}.",
            f"The average rating is {movie_dict['rating']}."
        ]
    }
    return jsonify(response)

def get_user_initiative():
    genre = request.args.get('genre')
    input_sentence = request.args.get('sentence')
    movie_title = request.args.get('movie_title')

    logging.info(f"User Initiative - Genre: {genre}, Sentence: {input_sentence}, Movie Title: {movie_title}")

    if not genre:
        return jsonify({"error": "Genre is required"}), 400

    if not input_sentence:
        # The user enters the title for the first time and returns to the recommended movie titles
        try:
            movie = content_based_recommendation(genre)
        except ValueError as e:
            logging.error(e)
            return jsonify({"error": str(e)}), 400

        if not movie:
            return jsonify({"error": "No movie found for the given genre"}), 404

        return jsonify({"response": f"I recommend '{movie[1]}'", "movie_title": movie[1]})

    if not movie_title:
        return jsonify({"error": "Movie title is required for further questions"}), 400

    # Follow-up dialogue
    movie = fetch_movie_by_title(movie_title)
    if not movie:
        return jsonify({"error": "Movie not found"}), 404

    movie_dict = {
        "movie_title": movie[1],
        "overview": movie[2],
        "director": movie[3],
        "cast": movie[4],
        "release_date": movie[5]
    }

    # Get the average rating information for the movie
    avg_rating = fetch_avg_rating_by_movie_id(movie[0])
    movie_dict["rating"] = round(avg_rating, 2) if avg_rating is not None else "N/A"
    movie_dict["genre"] = genre.capitalize()

    description_table = {
        "overview": {"Query Type": "What", "Response Template": "The plot is <overview>."},
        "director": {"Query Type": "Who", "Response Template": "The director is <director>."},
        "cast": {"Query Type": "Who", "Response Template": "The cast includes <cast>."},
        "release_date": {"Query Type": "What", "Response Template": "The release date is <release_date>."},
        "rating": {"Query Type": "How", "Response Template": "The average rating is <rating>."},
        "genre": {"Query Type": "What", "Response Template": "The genre is <genre>."}
    }

    keyword_to_attribute_map = {
        "plot": "overview",
        "story": "overview",
        "director": "director",
        "directors": "director",
        "cast": "cast",
        "actors": "cast",
        "actor": "cast",
        "date": "release_date",
        "release": "release_date",
        "rating": "rating",
        "genre": "genre",
        "category": "genre"
    }

    example_sentence_templates = {
        "What": ["What is the <Keyword>?", "What about the <Keyword>?", "Tell me about the <Keyword>", "What is the <Keyword> of the movie?", "Can you tell me about the <Keyword>?"],
        "Who": ["Who is the <Keyword>?", "What about the <Keyword>?", "Tell me about the <Keyword>", "Who is part of the <Keyword>?", "Can you tell me who is the <Keyword>?"],
        "How": ["How is the <Keyword>?", "Tell me about the <Keyword>", "What is the <Keyword> like?", "Can you describe the <Keyword>?"]
    }

    # Generate Q&A pairs
    qa_pairs = []
    for keyword, description in description_table.items():
        for template in example_sentence_templates[description["Query Type"]]:
            question = template.replace("<Keyword>", keyword)
            answer = description["Response Template"].replace(f"<{keyword}>", str(movie_dict[keyword]))
            qa_pairs.append((question, answer))

    questions, answers = zip(*qa_pairs)

    # Pre-process input sentences and predefined questions
    preprocessed_questions = [preprocess_text(question) for question in questions]
    preprocessed_input_sentence = preprocess_text(input_sentence)

    # Check specific keywords
    for keyword, attribute in keyword_to_attribute_map.items():
        if keyword in preprocessed_input_sentence:
            response = description_table[attribute]["Response Template"].replace(f"<{attribute}>", str(movie_dict[attribute]))
            return jsonify({"response": response})

    # Calculate the cosine similarity between the input sentence and the predefined questions.
    vectorizer = CountVectorizer().fit(preprocessed_questions)
    vectors = vectorizer.transform(preprocessed_questions).toarray()
    input_vector = vectorizer.transform([preprocessed_input_sentence]).toarray()

    cosine_similarities = cosine_similarity(input_vector, vectors).flatten()
    max_similarity_index = cosine_similarities.argmax()
    max_similarity = cosine_similarities[max_similarity_index]

    # Set similarity threshold
    threshold = 0.1
    if max_similarity >= threshold:
        response = answers[max_similarity_index]
    else:
        response = "I didn't understand your question. Can you please rephrase?"

    return jsonify({"response": response})
