from os import path, sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from mealcurator.url_digest.url import url_reader
from mealcurator.db_objs.db_conn import postgre
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_selection import VarianceThreshold
from sklearn.metrics.pairwise import cosine_similarity
import uuid
import psycopg2.extras
import pandas as pd


psycopg2.extras.register_uuid()
instance = postgre()
'''
sql = """
        select 
        cp.owner_id
        ,cpm.meal_id
        ,count(distinct cp.id) selected
        ,sum(cpm.review) ranking
        ,sum(cpm.review) / count(distinct cp.id) affinity
        from cooks_plan cp 
        join cooks_plan_meal cpm 
            on cp.id = cpm.plan_id
        group by 
        cp.owner_id , 
        cpm.meal_id ;
        """
cook_ratings = instance.select(sql)
'''
sql = """
        select
        mmr.meal_id 
        ,mmr.vegan 
        ,mmr.vegetarian 
        ,mmr.meal_time 
        ,mmr.dish_type 
        ,mmr.cooking_method 
        ,mmr.cooking_time
        ,mmr.protein_type
        ,mmr.times_selected 
        ,mmr.upvote 
        ,mmr.downvote
        ,least((mmr.upvote - mmr.downvote)/greatest (times_selected,0.1),1) mstr_affinity
        ,mmr.found_words
        from meals_mstr_recipe mmr
    """
mstr_recipes = instance.select(sql, 'meal_id')
mstr_recipes = pd.get_dummies(mstr_recipes, columns=['vegan', 'vegetarian', 'meal_time','dish_type','cooking_method','cooking_time', 'protein_type'])
# cook_ratings = cook_ratings.join(mstr_recipes, on='meal_id')

# known = cook_ratings[cook_ratings.owner_id ==1]
# unknown = mstr_recipes[~mstr_recipes.index.isin(known.meal_id)]

vectorizer = TfidfVectorizer(strip_accents='ascii',ngram_range=(1,3), max_df=0.90, min_df=0.05)
X = vectorizer.fit_transform(mstr_recipes.found_words.str.join(' '))

X_df = pd.DataFrame.sparse.from_spmatrix(X)
X_df.columns = vectorizer.get_feature_names_out()
X_df = X_df.set_index(mstr_recipes.index)
X_df = X_df.join(mstr_recipes, how='left')
X_df = X_df.drop(labels='found_words',axis=1)

selector = VarianceThreshold(0.001)
small_X = selector.fit_transform(X_df)

small_x_df = pd.DataFrame(small_X)
small_x_df.columns = selector.get_feature_names_out()
small_x_df = small_x_df.set_index(X_df.index)

corr = cosine_similarity(small_x_df)
corr_df = pd.DataFrame(corr)

corr_df.columns = small_x_df.index.values
corr_df = corr_df.set_index(small_x_df.index)
corr_df = corr_df.stack().reset_index()
corr_df['self'] = corr_df.meal_id == corr_df.level_1
corr_df['agree'] = 0
corr_df['disagree'] = 0
corr_df = corr_df[~corr_df.self]
corr_df.columns = ['sim_id', 'compare_id', 'sim_score', 'self', 'agree', 'disagree']
insert_df = corr_df[['sim_id', 'compare_id', 'sim_score', 'agree', 'disagree']]


instance.insert_df(insert_df, 'meals_recipe_sims', True)
