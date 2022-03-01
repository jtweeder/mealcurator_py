from os import path, sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from mealcurator.url_digest.url import url_reader
from mealcurator.db_objs.db_conn import postgre
import uuid
import psycopg2.extras

psycopg2.extras.register_uuid()
instance = postgre()
sql = "SELECT * FROM meals_raw_recipe WHERE mstr_flag=false and error_flag=false;"
to_process = instance.select(sql, 'id')
    
if len(to_process) > 0:
    for idx, data in to_process.iterrows():
        try:
            digest = url_reader(data.rec_url)
            title, learned_words = digest.raw_to_mstr()
            insert_sql = """INSERT INTO meals_mstr_recipe (meal_id, title, rec_url, vegan, vegetarian, meal_time, dish_type, cooking_method, cooking_time, protein_type, times_selected, upvote, downvote, found_words)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """
            vals = (uuid.uuid1(), title, data.rec_url, data.vegan,
                    data.vegetarian, data.meal_time, data.dish_type,
                    data.cooking_method, data.cooking_time, data.protein_type,
                    0, 0, 0, learned_words)
            instance.insert_update(insert_sql, vals)
            update_sql = """UPDATE meals_raw_recipe
                            SET mstr_flag=true WHERE id=%(idx)s;"""
            vals = {'idx': idx}
            instance.insert_update(update_sql, vals)
            instance.connection.commit()
        except ValueError:
            update_sql = """UPDATE meals_raw_recipe
                            SET error_flag=true WHERE id=%(idx)s;"""
            vals = {'idx': idx}
            instance.insert_update(update_sql, vals)
            instance.connection.commit()
            continue
            