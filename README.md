# data_engineering

## Парсинг IMDB
В качестве данных были выбраны данные IMDB, полученные путём разбора 
html страниц


Ссылка на местонахождение данных:
https://drive.google.com/drive/folders/1BudgHC7Qf5wdLqyGeA76gvpOwXr7Ln8y

Пример первых 10 строк dataframe:

| # | Title                          | Genre                       | Year | Cert. | Runtime | Votes  | IMDb | Meta | Description                                                                                           | Stars                                  |
|---|--------------------------------|-----------------------------|------|-------|---------|--------|------|------|-------------------------------------------------------------------------------------------------------|----------------------------------------|
| 0 | The Fall of the House of Usher | Drama, Horror, Mystery      | 2023 | TV-MA | 493     | 67677  | 8.0  | –    | To secure their fortune two ruthless siblings build a dynasty that crumbles as heirs mysteriously die. | Carla Gugino, Bruce Greenwood, Mary McDonnell, Henry Thomas |
| 1 | Killers of the Flower Moon     | Crime, Drama, History       | 2023 | R     | 206     | 83339  | 8.0  | 89   | When oil is discovered in 1920s Oklahoma, Osage people are murdered until the FBI steps in.           | Leonardo DiCaprio, Robert De Niro, Lily Gladstone, Jesse Plemons |
| 2 | Bodies                         | Crime, Drama, History       | 2023 | TV-MA | 455     | 24785  | 7.4  | –    | Four detectives in four time periods of London investigate the same murder.                           | Amaka Okafor, Kyle Soller, Stephen Graham, Shira Haas |
| 3 | Pain Hustlers                  | Crime, Drama                | 2023 | R     | 122     | 21650  | 6.5  | 44   | Liza skyrockets in a pharma sales job but ends up in a federal conspiracy.                            | Emily Blunt, Chris Evans, Catherine O'Hara, Chloe Coleman |
| 4 | Gen V                          | Action, Adventure, Comedy   | 2023 | TV-MA | 50      | 47232  | 7.9  | –    | From the world of *The Boys*: young supes test their moral boundaries competing for top ranking.       | Jaz Sinclair, Chance Perdomo, Lizze Broadway, Maddie Phillips |
| 5 | A Haunting in Venice           | Crime, Drama, Horror        | 2023 | PG-13 | 103     | 55485  | 6.6  | 63   | Poirot attends a séance in Venice; when a guest is murdered, he must uncover the killer.              | Kenneth Branagh, Michelle Yeoh, Jamie Dornan, Tina Fey |
| 6 | All the Light We Cannot See    | Drama, History, War         | 2023 | TV-MA | 228     | 14208  | 7.7  | –    | Story of Marie-Laure, a blind French teen, and Werner, a German soldier, in occupied France.          | Aria Mia Loberti, Louis Hofmann, Lars Eidinger, Hugh Laurie |
| 7 | The Killer                     | Action, Adventure, Crime    | 2023 | R     | 118     | 47041  | 7.0  | 72   | An assassin battles his employers and himself on an international manhunt.                            | Michael Fassbender, Tilda Swinton, Charles Parnell, Arliss Howard |
| 8 | Invincible                     | Animation, Action, Adventure| 2021 | TV-MA | 50      | 172289 | 8.7  | –    | Animated series about a teen whose father is the most powerful superhero on Earth.                    | Steven Yeun, J.K. Simmons, Sandra Oh, Zazie Beetz |
| 9 | The Gilded Age                 | Drama                       | 2022 | TV-MA | 81      | 29509  | 8.0  | –    | A young woman infiltrates the wealthy Russell family in New York’s Gilded Age.                        | Ben Ahlers, Debra Monk, Kelli O'Hara, Taylor Richardson |

Типы данных
title              string[python]  
genre              string[python]  
year                        Int32  
certificate        string[python]  
runtime                     Int16  
user-votes                  Int32  
imdb-scores               float32  
metacritic-scores           Int16  
descriptions       string[python]  
stars              string[python]  
start_year                  Int32  
end_year                    Int32  

## Получение данных по API
Получение данных музея MET по API "https://collectionapi.metmuseum.org/public/collection/v1/objects"
Вывод данных первых 10 строк датафрейма с очищенными полями  

|   | objectID |                 title                  | objectName | objectDate | objectBeginDate | objectEndDate | accessionYear |    department     | medium | classification |   artistDisplayName   | artistNationality | artistBeginDate | artistEndDate | country | city |                repository                |                     objectURL                      |
|---|----------|----------------------------------------|------------|------------|-----------------|---------------|---------------|-------------------|--------|----------------|-----------------------|-------------------|-----------------|---------------|---------|------|------------------------------------------|----------------------------------------------------|
| 0 |    1     |      One-dollar Liberty Head Coin      |    Coin    |    1853    |      1853       |     1853      |     1979      | The American Wing |  Gold  |                | James Barton Longacre |     American      |      1794       |     1869      |         |      | Metropolitan Museum of Art, New York, NY | https://www.metmuseum.org/art/collection/search/1  |
| 1 |    2     |      Ten-dollar Liberty Head Coin      |    Coin    |    1901    |      1901       |     1901      |     1980      | The American Wing |  Gold  |                |  Christian Gobrecht   |     American      |      1785       |     1844      |         |      | Metropolitan Museum of Art, New York, NY | https://www.metmuseum.org/art/collection/search/2  |
| 2 |    3     |       Two-and-a-Half Dollar Coin       |    Coin    |  1909–27   |      1909       |     1927      |     1967      | The American Wing |  Gold  |                |                       |                   |                 |               |         |      | Metropolitan Museum of Art, New York, NY | https://www.metmuseum.org/art/collection/search/3  |
| 3 |    4     |       Two-and-a-Half Dollar Coin       |    Coin    |  1909–27   |      1909       |     1927      |     1967      | The American Wing |  Gold  |                |                       |                   |                 |               |         |      | Metropolitan Museum of Art, New York, NY | https://www.metmuseum.org/art/collection/search/4  |
| 4 |    5     |       Two-and-a-Half Dollar Coin       |    Coin    |  1909–27   |      1909       |     1927      |     1967      | The American Wing |  Gold  |                |                       |                   |                 |               |         |      | Metropolitan Museum of Art, New York, NY | https://www.metmuseum.org/art/collection/search/5  |
| 5 |    6     |       Two-and-a-Half Dollar Coin       |    Coin    |  1909–27   |      1909       |     1927      |     1967      | The American Wing |  Gold  |                |                       |                   |                 |               |         |      | Metropolitan Museum of Art, New York, NY | https://www.metmuseum.org/art/collection/search/6  |
| 6 |    7     |       Two-and-a-Half Dollar Coin       |    Coin    |  1909–27   |      1909       |     1927      |     1967      | The American Wing |  Gold  |                |                       |                   |                 |               |         |      | Metropolitan Museum of Art, New York, NY | https://www.metmuseum.org/art/collection/search/7  |
| 7 |    8     |       Two-and-a-Half Dollar Coin       |    Coin    |  1909–27   |      1909       |     1927      |     1967      | The American Wing |  Gold  |                |                       |                   |                 |               |         |      | Metropolitan Museum of Art, New York, NY | https://www.metmuseum.org/art/collection/search/8  |
| 8 |    9     |       Two-and-a-Half Dollar Coin       |    Coin    |  1909–27   |      1909       |     1927      |     1967      | The American Wing |  Gold  |                |                       |                   |                 |               |         |      | Metropolitan Museum of Art, New York, NY | https://www.metmuseum.org/art/collection/search/9  |
| 9 |    10    | Two-and-a-half-dollar Indian Head Coin |    Coin    |    1912    |      1912       |     1912      |     1979      | The American Wing |  Gold  |                |    Bela Lyon Pratt    |     American      |      1867       |     1917      |         |      | Metropolitan Museum of Art, New York, NY | https://www.metmuseum.org/art/collection/search/10 |
