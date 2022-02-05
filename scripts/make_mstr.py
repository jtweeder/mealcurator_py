from os import path, sys
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from mealcurator.url_digest.url import url_reader
from mealcurator.db_objs.db_conn import postgre

instance = postgre()
sql = "SELECT * FROM meals_raw_recipe WHERE mstr_flag=false and error_flag=false;"
to_process = instance.select(sql, 'id')
    
if len(to_process) > 0:
    for idx, data in to_process.iterrows():
        try:
            digest = url_reader(data.rec_url)
            title, learned_words = digest.raw_to_mstr()
            print(title)
        except ValueError:
            print(f'Failed : {title}')
            update_sql = (f'UPDATE meals_raw_recipe SET '
                          f'error_flag=true WHERE id={idx};')
            instance.update(update_sql)
            instance.connection.commit()
            continue
            