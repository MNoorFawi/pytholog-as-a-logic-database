import psycopg2
import pandas as pd

psql = psycopg2.connect(host = "localhost", database = "dvdrental",
                      user = "postgres", password = "password")
cursor = psql.cursor()
## fetch some data to confirm connection
print(pd.read_sql("SELECT * FROM language;", psql))

cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
print(cursor.fetchall())

def query_defn(table):
    return f"SELECT * FROM {table};"

actor = pd.read_sql(query_defn("actor"), psql)
actor["Actor"] = (actor["first_name"] + "_" + actor["last_name"]).str.lower()
print(actor.head())

language = pd.read_sql(query_defn("language"), psql)
film = pd.read_sql(query_defn("film"), psql)
category = pd.read_sql(query_defn("category"), psql)
#customer = pd.read_sql(query_defn("customer"), psql)
language["name"] = language["name"].str.lower()
film["title"] = film["title"].str.replace(" ", "_").str.lower()
category["name"] = category["name"].str.lower()
#customer["Customer"] = (customer["first_name"] + "_" + customer["last_name"]).str.lower()
film_category = pd.read_sql(query_defn("film_category"), psql)
#film[film.film_id.isin(film_category[film_category.category_id == 14].film_id)]

print(film.loc[film.film_id == 1, "title"])
print(actor.head())

import pytholog as pl
dvd = pl.KnowledgeBase("dvd_rental")

for i in range(film.shape[0]):
    dvd([f"film({film.film_id[i]}, {film.title[i]}, {film.language_id[i]})"])

for i in range(language.shape[0]):
    dvd([f"language({language.language_id[i]}, {language.name[i]})"])
    
dvd(["film_language(F, L) :- film(_, F, LID), language(LID, L)"])
print(dvd.query(pl.Expr("film_language(young_language, L)")))

for i in range(category.shape[0]):
    dvd([f"category({category.category_id[i]}, {category.name[i]})"])
    
for i in range(film_category.shape[0]):
    dvd([f"filmcategory({film_category.film_id[i]}, {film_category.category_id[i]})"])
    
dvd(["film_category(F, C) :- film(FID, F, _), filmcategory(FID, CID), category(CID, C)"])
print(dvd.query(pl.Expr("film_category(F, sci-fi)")))

for i in range(actor.shape[0]):
    dvd([f"actor({actor.actor_id[i]}, {actor.Actor[i]})"])
    
film_actor = pd.read_sql(query_defn("film_actor"), psql)
#print(film_actor[film_actor["actor_id"] == 3].shape)
print(film_actor.shape)
#(5462, 3)
for i in range(film_actor.shape[0]):
    dvd([f"filmactor({film_actor.film_id[i]}, {film_actor.actor_id[i]})"])

dvd(["film_actor(F, A) :- film(FID, F, _), filmactor(FID, AID), actor(AID, A)"])

print(dvd.query(pl.Expr("film_actor(annie_identity, Actor)")))
print(dvd.query(pl.Expr("film_actor(academy_dinosaur, Actor)")))
print(dvd.query(pl.Expr("film_actor(Film, penelope_guiness)")))
print(dvd.query(pl.Expr("film_actor(academy_dinosaur, lucille_tracy)")))

dvd(["actor_category(A, C) :- film_actor(F, A), film_category(F, C)"])
jd = dvd.query(pl.Expr("actor_category(jennifer_davis, Category)"))

from pprint import pprint
merged = {}
for d in jd:
    for k, v in d.items():
        if k not in merged: merged[k] = set()
        merged[k].add(v)

pprint(merged)

with open("dvd_rental.pl", "w") as f:
    for i in dvd.db.keys():
        for d in dvd.db[i]["facts"]:
            f.write(d.to_string() + "." + "\n")
    
f.close()
