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
