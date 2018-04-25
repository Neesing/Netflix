# Netflix
Reccomendation system

Netflix recommendation system using Spark and AWS

Netflix is the worlds largest streaming services, with 80 million members in over 250 countries. The aim of this project is to predict user ratings and recommend movies. 

Getting and processing the data
In order to build a movie recommendation system

DATASET DESCRIPTION
Training dataset:
It contains 17770 files, one file for each movie.
CustomerID, Rating, Date
•	MovieIDs range from 1 to 17770 sequentially.
•	CustomerIDs range from 1 to 2649429, with gaps. There are 480189 users.
•	Ratings are on a five star (integral) scale from 1 to 5.
•	Dates have the format YYYY-MM-DD.
Example :
1:
1488844,3,2005-09-06
822109,5,2005-05-13
885013,4,2005-10-19
30878,4,2005-12-26
823519,3,2004-05-03
893988,3,2005-11-17
124105,4,2004-08-05
1248029,3,2004-04-22


Movie file description
It contains the description of every movie.

MovieID, YearOfRelease, Title

•	MovieID do not correspond to actual Netflix movie ids or IMDB movie ids.
•	YearOfRelease can range from 1890 to 2005 and may correspond to the release of corresponding DVD, not necessarily its theaterical release.
•	Title is the Netflix movie title and are in English

Example :

1,2003,Dinosaur Planet
2,2004,Isle of Man TT 2004 Review
3,1997,Character
4,1994,Paula Abdul's Get Up & Dance
5,2004,The Rise and Fall of ECW
6,1997,Sick
7,1992,8 Man
8,2004,What the #$*! Do We Know!?




Prediction dataset
It consists of lines indicating a movie id, followed by a colon, and then customer ids and rating dates, one per line for that movie id.

MovieID1:
CustomerID11,Date11
CustomerID12,Date12
MovieID2:
CustomerID21,Date21
CustomerID22,Date22

Our aim is to predict the movie ratings the customers gave the movies in the qualifying dataset based on the information in the training dataset.

For example, if the qualifying dataset looked like:
111:
3245,2005-12-19
5666,2005-12-23
6789,2005-03-14
225:
1234,2005-05-26
3456,2005-11-07
then a prediction file should look something like:
111:
3.0
3.4
4.0
225:
1.0
2.0


To downlaod full dataset:
https://www.kaggle.com/netflix-inc/netflix-prize-data/data

