import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from parser import Parser


class Crawler:
    def __init__(self, movies_to_parse, path_url):
        self.movies_to_parse = movies_to_parse
        self.path_url = path_url
        self.columns = [
            "title",
            "genre",
            "year",
            "certificate",
            "runtime",
            "user-votes",
            "imdb-scores",
            "metacritic-scores",
            "descriptions",
            "stars",
        ]

        self.all_titles = pd.Series()
        self.all_years = pd.Series()
        self.all_genres = pd.Series()
        self.all_certificates = pd.Series()
        self.all_runtimes = pd.Series()
        self.all_votes = pd.Series()
        self.all_imdb_scores = pd.Series()
        self.all_metacritic_scores = pd.Series()
        self.all_descriptions = pd.Series()
        self.all_stars = pd.Series()

    def get_dataset(self):
        self.iterate_over_pages()

        data_titles = pd.DataFrame(self.all_titles, columns=["title"])
        data_genres = pd.DataFrame(self.all_genres, columns=["genre"])
        data_years = pd.DataFrame(self.all_years, columns=["year"])
        data_certificates = pd.DataFrame(self.all_certificates, columns=["certificate"])
        data_runtimes = pd.DataFrame(self.all_runtimes, columns=["runtime"])
        data_votes = pd.DataFrame(self.all_votes, columns=["user-votes"])
        data_imdb_scores = pd.DataFrame(self.all_imdb_scores, columns=["imdb-scores"])
        data_metacritic_scores = pd.DataFrame(
            self.all_metacritic_scores, columns=["metacritic-scores"]
        )
        data_descriptions = pd.DataFrame(
            self.all_descriptions, columns=["descriptions"]
        )
        data_stars = pd.DataFrame(self.all_stars, columns=["stars"])

        data = pd.concat(
            [
                data_titles,
                data_genres,
                data_years,
                data_certificates,
                data_runtimes,
                data_votes,
                data_imdb_scores,
                data_metacritic_scores,
                data_descriptions,
                data_stars,
            ],
            axis=1,
            ignore_index=True,
        )
        data.columns = self.columns

        return data

    def iterate_over_pages(self):
        for i in tqdm(range(1, self.movies_to_parse, 50)):
            url = self.path_url + "&start=" + str(i) + "&ref_=adv_nxt"
            current_soup = self.get_bs(url)
            parser = Parser(current_soup)

            self.get_titles(parser)
            self.get_years(parser)
            self.get_genres(parser)
            self.get_age_rating(parser)
            self.get_runtimes(parser)
            self.get_imdb_scores(parser)
            self.get_metacritic_scores(parser)
            self.get_votes(parser)
            self.get_descriptions(parser)
            self.get_stars(parser)

    def get_bs(self, url):
        response = requests.get(url=url)
        soup = BeautifulSoup(response.text, "html.parser")
        return soup

    def get_titles(self, parser):
        page_titles = Parser.unique_movie_title(parser)
        self.all_titles = pd.concat([self.all_titles, page_titles], ignore_index=True)

    def get_genres(self, parser):
        page_genres = Parser.unique_movie_genre(parser)
        self.all_genres = pd.concat([self.all_genres, page_genres], ignore_index=True)

    def get_years(self, parser):
        page_years = Parser.unique_movie_year(parser)
        self.all_years = pd.concat([self.all_years, page_years], ignore_index=True)

    def get_age_rating(self, parser):
        page_certificate = Parser.unique_movie_age_rating(parser)
        self.all_certificates = pd.concat(
            [self.all_certificates, page_certificate], ignore_index=True
        )

    def get_runtimes(self, parser):
        page_runtimes = Parser.unique_movie_runtime(parser)
        self.all_runtimes = pd.concat(
            [self.all_runtimes, page_runtimes], ignore_index=True
        )

    def get_imdb_scores(self, parser):
        page_scores = Parser.unique_movie_imdb_score(parser)
        self.all_imdb_scores = pd.concat(
            [self.all_imdb_scores, page_scores], ignore_index=True
        )

    def get_votes(self, parser):
        page_votes = Parser.unique_movie_vote(parser)
        self.all_votes = pd.concat([self.all_votes, page_votes], ignore_index=True)

    def get_metacritic_scores(self, parser):
        page_metacritic_scores = Parser.unique_movie_metacritic_score(parser)
        self.all_metacritic_scores = pd.concat(
            [self.all_metacritic_scores, page_metacritic_scores], ignore_index=True
        )

    def get_descriptions(self, parser):
        page_descriptions = Parser.unique_movie_description(parser)
        self.all_descriptions = pd.concat(
            [self.all_descriptions, page_descriptions], ignore_index=True
        )

    def get_stars(self, parser):
        page_stars = Parser.unique_stars(parser)
        self.all_stars = pd.concat([self.all_stars, page_stars], ignore_index=True)
