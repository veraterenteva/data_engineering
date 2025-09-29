import re
import pandas as pd


class Parser:
    def __init__(self, soup):
        self.soup = soup

    # 1. Название
    def unique_movie_title(self):
        unique_strings_title = set()

        div_tags = self.soup.find_all("div", class_="lister-item-content")
        unique_strings_title = [
            div_tag.find("h3", class_="lister-item-header").find("a").text
            for div_tag in div_tags
        ]

        titles = pd.Series(unique_strings_title)
        return titles

    # 2. Год производства
    def unique_movie_year(self):
        unique_strings_year = set()

        div_tags = self.soup.find_all("div", class_="lister-item-content")
        unique_strings_year = [
            div_tag.find("h3", class_="lister-item-header")
            .find("span", class_="lister-item-year text-muted unbold")
            .text
            for div_tag in div_tags
        ]

        cleaned_data = []
        for year in unique_strings_year:
            cleaned_item = re.sub(r"[^0-9]+", "", year)
            if "-" in cleaned_item:
                cleaned_item = cleaned_item.split("-")[0]
            cleaned_data.append(cleaned_item)

        years = pd.Series(cleaned_data)
        return years

    # 3. Жанр
    def unique_movie_genre(self):
        unique_strings_movie_genre = set()

        div_tags = self.soup.find_all("div", class_="lister-item-content")
        unique_strings_movie_genre = [
            div_tag.find("p", class_="text-muted").find("span", class_="genre").text
            for div_tag in div_tags
        ]
        genres = [
            genre.replace(" ", "").strip() for genre in unique_strings_movie_genre
        ]

        genres = pd.Series(genres)
        return genres

    # 4. Возрастной ценз
    def unique_movie_age_rating(self):
        unique_strings_movie_age_rating = []

        div_tags = self.soup.find_all("div", class_="lister-item-content")

        for div_tag in div_tags:
            try:
                certificate = (
                    div_tag.find("p", class_="text-muted")
                    .find("span", class_="certificate")
                    .text
                )
            except:
                certificate = "No data"
            unique_strings_movie_age_rating.append(certificate)

        ratings = pd.Series(unique_strings_movie_age_rating)

        return ratings

    # 5. Продолжительность
    def unique_movie_runtime(self):
        unique_strings_movie_runtime = []

        div_tags = self.soup.find_all("div", class_="lister-item-content")

        for div_tag in div_tags:
            try:
                certificate = div_tag.find("p", class_="text-muted").find(
                    "span", class_="runtime"
                )
                certificate = certificate.text.removesuffix(" min")
            except:
                certificate = "No data"
            unique_strings_movie_runtime.append(certificate)

        runtime = pd.Series(unique_strings_movie_runtime)

        return runtime

    # 6. Рейтинг imdb
    def unique_movie_imdb_score(self):
        unique_movie_imdb_score = []

        div_tags = self.soup.find_all("div", class_="lister-item-content")

        for div_tag in div_tags:
            try:
                score = div_tag.find("div", class_="ratings-bar").find(
                    "div", class_="inline-block ratings-imdb-rating"
                )
                score = score.text
            except:
                score = "No data"
            unique_movie_imdb_score.append(score)

        scores = [score.strip() for score in unique_movie_imdb_score]
        scores = pd.Series(scores)

        return scores

    # 7. Количество голосов
    def unique_movie_vote(self):
        unique_movie_vote = []

        div_tags = self.soup.find_all("div", class_="lister-item-content")

        for div_tag in div_tags:
            try:
                vote = div_tag.find("p", class_="sort-num_votes-visible")
                vote = (re.findall(r"[0-9]+", str(vote)))[0]
            except:
                vote = "No data"
            unique_movie_vote.append(vote)

        votes = pd.Series(unique_movie_vote)
        return votes

    # 8. Рейтинг Metacritic
    def unique_movie_metacritic_score(self):
        unique_movie_metacritic_score = []

        div_tags = self.soup.find_all("div", class_="lister-item-content")

        for div_tag in div_tags:
            try:
                score = div_tag.find("div", class_="ratings-bar")
                score = score.find("div", class_="inline-block ratings-metascore").text
                unique_movie_metacritic_score.append(re.findall(r"\d+", score)[0])
            except:
                unique_movie_metacritic_score.append("No data")

        scores = pd.Series(unique_movie_metacritic_score)
        return scores

    # 9. Описания режиссёры, актёры
    def unique_movie_description(self):
        unique_strings_movie_description = []

        for div_tags in self.soup.select(
            "div.lister-item-content > p.text-muted:nth-of-type(2)"
        ):
            try:
                descriptions = div_tags.text.strip("\n")
            except:
                descriptions = None

            unique_strings_movie_description.append(descriptions)

        descriptions = pd.Series(unique_strings_movie_description)

        return descriptions

    # 10. Звёзды
    def unique_stars(self):
        unique_stars = []

        for div_tags in self.soup.select("div.lister-item-content > p:nth-of-type(3)"):
            try:
                unique_star = div_tags.text.strip("\n")
            except:
                unique_star = None
            unique_stars.append(unique_star)
        stars = pd.Series(unique_stars)

        return stars
