import cx_Oracle
from datetime import datetime

class DatabaseConnector:
    def __init__(self, config):
        # todo: add implementation of logger obj
        # self._logger = None
        # self.driver = 'oracle.jdbc.OracleDriver'
        self._login = config['db']['login']
        self._password = config['db']['password']
        self._host = config['db']['host']
        self._port = config['db']['port']
        self._schema = config['db']['schema']
        self._table = config['db']['table']
        self._config_table = config['db']['configurationTable']
        self._application_name = config['db']['applicationName']

        self._con = cx_Oracle.connect(
            self._login,
            self._password,
            f'{self._host}:{self._port}/{self._schema}')
        self._cur = self._con.cursor()
        # logging "locally" -> "Successfully Connected to f'{self._host}:{self._port}/{self._schema}'"

    def log_to_db(self, log_level: str, message: str, exception: str) -> None:
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%m:%S')
        query = f'''
        INSERT INTO {self._table} (
            (CREATED, LOG_LEVEL, APPLICATION, MESSAGE, EXCEPTION_TEXT)
        VALUES 
            TO_({current_timestamp}, 'yyyy-mm-dd HH24:MI:SS'), 
            {log_level}, 
            {self._application_name}, 
            {message}, 
            {exception}
        '''
        try:
            # todo: add logging "locally" about beginning of query execution
            self._cur.execute(query)
            self._con.commit()

        except Exception as ex:
            # todo: add logging "locally" about failed transaction
            self._cur.rollback()
            raise ex

      # VA${configuration.applicationName}','$message','$exception')""".stripMargin
      # tmp = """INSERT INTO $table (CREATED,LOG_LEVEL,APPLICATION,MESSAGE,EXCEPTION_TEXT) VALUES(to_date('$currentTimestamp', 'yyyy-mm-dd HH24:MI:SS'),'$logLevel','${configuration.applicationName}','$message','$exception')""".stripMargin

    def get_config_from_db(self, message_type: str) -> str:
        airflow_dag = None
        try:
            query = f'''
            SELECT AIRFLOW_DAG FROM {self._config_table}
            WHERE message_type={message_type}
            '''
            # todo: add logging "locally" -> "Getting Configuration from database ..."
            self._cur.execute(query)
            # todo: "locally" logging


        except Exception as ex:
            raise ex



    def __del__(self):
        self._con.close()

