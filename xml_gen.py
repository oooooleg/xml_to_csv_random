import csv
import random
import os
import uuid
import xml.etree.ElementTree as ET
from argparse import ArgumentParser
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from typing import List, Tuple
from zipfile import ZipFile

DEFAULT_ARC_COUNT = 50
DEFAULT_ARC_FILES = 100
DEFAULT_PROCESSES_COUNT = cpu_count()


class XmlCsvProcessor:
    def __init__(self, zip_path: str, csv_path: str) -> None:
        self._zip_path = zip_path
        self._csv_path = csv_path

    def gen_xml_archives(self, arc_count: int = DEFAULT_ARC_COUNT,
                         files_in_arc: int = DEFAULT_ARC_FILES) -> None:
        '''
        Generates archives with XML files
        :param arc_count: number of result archives
        :param files_in_arc: number of files in one archive
        '''
        if not os.path.exists(self._zip_path):
            os.mkdir(self._zip_path)

        for _ in range(arc_count):
            self._gen_archive(files_in_arc)

    def _gen_archive(self, files_in_arc: int) -> None:
        rand_zip_path = self._rand_zip_path()

        with ZipFile(rand_zip_path, mode='w') as arc:
            for i in range(files_in_arc):
                root = XmlCsvProcessor._gen_single_xml()
                self._write_xml_archive(root, arc, i)

    def _rand_zip_path(self) -> str:
        while True:
            zip_name = XmlCsvProcessor._rand_str() + '.zip'
            full_zip_path = os.path.join(self._zip_path, zip_name)
            if not os.path.exists(full_zip_path):
                return full_zip_path

    @staticmethod
    def _gen_single_xml() -> ET.Element:
        root = ET.Element('root')

        var_id = ET.Element(
            'var', attrib={'name': 'id', 'value': str(uuid.uuid4())})
        root.append(var_id)
        var_level = ET.Element(
            'var',
            attrib={'name': 'level', 'value': str(random.randint(1, 100))}
        )
        root.append(var_level)

        objs = ET.Element('objects')
        for _ in range(random.randint(1, 10)):
            obj = ET.Element(
                'object', attrib={'name': XmlCsvProcessor._rand_str()})
            objs.append(obj)

        root.append(objs)
        return root

    @staticmethod
    def _rand_str() -> str:
        result = []

        for _ in range(random.randint(4, 10)):
            ch = chr(ord('a') + random.randint(0, 25))
            result.append(ch)

        return ''.join(result)

    def _write_xml_archive(self, root: ET.Element, arc: ZipFile, i: int) -> None:
        xml_str = ET.tostring(root)
        arc.writestr(f'{i}.xml', xml_str)

    def gen_csv_files(self,
                      processes_count: int = DEFAULT_PROCESSES_COUNT) -> None:
        '''
        Generates CSV files from prepared XMLs in archives
        :param processes_count: parallelism level
        '''
        if not os.path.exists(self._csv_path):
            os.mkdir(self._csv_path)

        with ThreadPool(processes=processes_count) as pool:
            results = pool.map(
                self._process_single_zip, os.listdir(self._zip_path))

        levels_path = os.path.join(self._csv_path, 'levels.csv')
        names_path = os.path.join(self._csv_path, 'names.csv')

        with open(levels_path, 'w') as lf, open(names_path, 'w') as nf:
            levels_writer = csv.DictWriter(lf, fieldnames=('id', 'level'))
            levels_writer.writeheader()
            names_writer = csv.DictWriter(nf, fieldnames=('id', 'object_name'))
            names_writer.writeheader()

            for res in results:
                for id_, level, object_ids_names in res:
                    levels_writer.writerow({'id': id_, 'level': level})
                    for name in object_ids_names:
                        names_writer.writerow({'id': id_, 'object_name': name})

    def _process_single_zip(self, zip_name: str) -> List[Tuple[str, int, List[str]]]:
        zip_path = os.path.join(self._zip_path, zip_name)
        result = []

        with ZipFile(zip_path) as zip_file:
            for name in zip_file.namelist():
                buf = zip_file.read(name)
                root = ET.fromstring(buf.decode())

                vars = root.findall('var')
                if vars[0].get('name') == 'id':
                    id_, level = vars[0].get('value'), int(vars[1].get('value'))
                else:
                    id_, level = vars[1].get('value'), int(vars[0].get('value'))

                objects = root.find('objects')
                object_ids_names = [obj.get('name') for obj in objects]

                result.append((id_, level, object_ids_names))

        return result


def main():
    parser = ArgumentParser()
    parser.add_argument(
        'arch_path', type=str, help='path for arhcives generation')
    parser.add_argument('csv_path', type=str, help='path for csv generation')
    args = parser.parse_args()

    processor = XmlCsvProcessor(
        zip_path=args.arch_path, csv_path=args.csv_path)

    processor.gen_xml_archives()
    processor.gen_csv_files()


if __name__ == '__main__':
    main()
