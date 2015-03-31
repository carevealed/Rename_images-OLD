import hashlib

__title__ = 'Renamer'
__author__ = 'California Audio Visual Preservation Project'
__version__ = '0.0.1'
__credits__= ['Henry Borchers']
import os
import shutil
import sqlite3

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class renamer(object):
    """
    This class is used for renaming file names to CAVPP naming standards.
    It uses a sqlite database to hold the data which can be used to
    generate reports.

    Since this uses a database file, this class is designed to be a
    singleton.
    """

    _instance = None
    _data = sqlite3.connect('data.db')
    @property
    def object_id(self):
        return self._object_id

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(renamer, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, object_id=None):
        if object_id:
            self._object_id = object_id
        else:
            self._object_id = None
        self.total_files = 1

        self._data.row_factory = dict_factory
        self.initize_database()


    def initize_database(self):
        self._data.execute('DROP TABLE IF EXISTS queue')
        self._data.execute('CREATE TABLE queue('
                           'q_id INTEGER PRIMARY KEY AUTOINCREMENT DEFAULT 1 NOT NULL ,'
                           'queue INTEGER DEFAULT 1,'
                           'source VARCHAR(255),'
                           'object_id VARCHAR(255),'
                           'destination VARCHAR(255),'
                           'status INTEGER DEFAULT 0,'
                           'date_converted DATE,'
                           'md5 VARCHAR(32),'
                           'project_id INT)')

        self._data.execute('DROP TABLE IF EXISTS status')
        self._data.execute('CREATE TABLE status('
                           'stat_id INTEGER PRIMARY KEY AUTOINCREMENT DEFAULT 1 NOT NULL,'
                           'statusName VARCHAR(10))')
        self._data.execute('INSERT INTO status (stat_id, statusName) VALUES (?,?)', (0, 'Queued'))
        self._data.execute('INSERT INTO status (stat_id, statusName) VALUES (?,?)', (1, 'Open'))
        self._data.execute('INSERT INTO status (stat_id, statusName) VALUES (?,?)', (2, 'Renamed'))
        self._data.commit()

    def add_file(self, source, destination=None):
        if not destination:
            if not self._object_id:
                raise Exception("Need to have set the object_id first.")
            else:
                new_destination = self.object_id + self.total_files
                # path, file = os.path.split(source)
                #
                # file, extension = os.path.splitext(file)
                # file = file + "_test"


            # new_destination = os.path.join(path, file + extension)
        else:
            new_destination = destination
        md5 = self._calculate_md5(source)
        self._data.execute('INSERT INTO queue (project_id, source, destination, md5) '
                           'VALUES (?, ?, ?, ?)', (self.object_id, source, new_destination, md5))
        self._data.commit()

        # print("self.destination: " + self.destination)

    def _calculate_md5(self, file):
        BUFFER = 8192
        md5 = hashlib.md5()
        with open(file, 'rb') as f:
            for chunk in iter(lambda: f.read(BUFFER), b''):
                md5.update(chunk)

        return md5.hexdigest()
    def remove_record(self, id):
        print("removing " + str(id))
        self._data.execute('DELETE FROM queue WHERE q_id = (?)', (id,))

    def rename_by_id(self, id):
        # TODO: build rename_by_id method
        print("renaming " + str(id))

    def close_database(self):
        self._data.close()



    def show_all_data(self):
        cursor = self._data.execute('SELECT queue.q_id, source, destination, status.statusName, md5 FROM queue JOIN status ON queue.status = status.stat_id')
        for row in cursor:
            print row

    def show_all_queues(self):
        cursor = self._data.execute('SELECT * '
                                    'FROM queue '
                                    'JOIN status ON queue.status = status.stat_id '
                                    'WHERE statusName = "Queued"')
        for row in cursor:
            print row['q_id'], row['source'], row['destination']


    def _renamer(self, record):

        source = ""
        destination = ""
        if os.path.isfile(source):
            shutil.copy(source, destination)


def main():
    tester = renamer(object_id="CASSC_23344")

    # source1 = '/Users/lpsdesk/PycharmProjects/rename_images/testImages/tif/ana102.tif'

    for root, dir, files in os.walk('/Users/lpsdesk/PycharmProjects/rename_images/testImages/tif'):
        for file in files:
            tester.add_file(os.path.join(root,file))
    # tester.add_file(source1)
    # tester.add_file(source1)
    # tester.add_file(source1)
    tester.show_all_queues()
    tester.close_database()




if __name__ == '__main__':
    main()