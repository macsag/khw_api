from typing import Callable, Optional
import sqlite3


class GenericClient(object):
    def __init__(self, path_to_db_file: str, sql_query: str,
                 dict_key_name: str, id_processing_method: Optional[Callable[[str], str]] = None):
        self.path_to_db_file = path_to_db_file
        self.sql_query = sql_query
        self.dict_key_name = dict_key_name
        self.id_processing_method = id_processing_method
        self.conn = self.open_connection()
        self.filtered_dict = {}

    def open_connection(self):
        conn = sqlite3.connect(self.path_to_db_file)
        return conn

    def close_connection(self):
        self.conn.close()

    def get_rows_from_db(self) -> tuple:
        c = self.conn.cursor()
        for row in c.execute(self.sql_query):
            yield row

    def filter_by_best_score(self, row: tuple):
        nlp_id, fetched_id, score, max_score = row
        fetched_from_f_dict = self.filtered_dict.get(nlp_id)

        if (fetched_from_f_dict and score >= fetched_from_f_dict['score']) or not fetched_from_f_dict:
            fetched_id = fetched_id if not self.id_processing_method else self.id_processing_method(fetched_id)
            self.filtered_dict[nlp_id] = {self.dict_key_name: fetched_id, 'score': score}

    def remove_scores(self):
        for external_ids_dict in self.filtered_dict.values():
            external_ids_dict.pop('score')

    def run_process(self):
        for r in self.get_rows_from_db():
            self.filter_by_best_score(r)
        self.remove_scores()

    def create_index(self) -> dict:
        self.run_process()
        self.close_connection()
        return self.filtered_dict
