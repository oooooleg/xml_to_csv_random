import csv
import random
import os
import uuid
import xml.etree.ElementTree as ET
from argparse import ArgumentParser
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from tempfile import NamedTemporaryFile
from typing import List, Tuple
from zipfile import ZipFile

DEFAULT_ARC_COUNT = 50
DEFAULT_PROCESSES_COUNT = cpu_count()


class XmlCsvProcessor:
    def __init__(self, zip_path: str, csv_path: str) -> None:
        self._zip_path = zip_path
        self._csv_path = csv_path

    def gen_xml_archives(self, arc_count: int = DEFAULT_ARC_COUNT) -> None:
        '''
        Generates archives with XML files
        :param arc_count: number of result archives
        '''
        if not os.path.exists(self._zip_path):
            os.mkdir(self._zip_path)

        for _ in range(arc_count):
            et = XmlCsvProcessor._gen_single_xml_tree()
            self._write_xml_archive(et)

    @staticmethod
    def _gen_single_xml_tree() -> ET.ElementTree:
        root = ET.Element('root')
        et = ET.ElementTree(root)

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

        return et

    @staticmethod
    def _rand_str() -> str:
        result = []

        for _ in range(random.randint(4, 10)):
            ch = chr(ord('a') + random.randint(0, 25))
            result.append(ch)

        return ''.join(result)

    def _write_xml_archive(self, et: ET.ElementTree) -> None:
        ET.indent(et)

        with NamedTemporaryFile(dir=self._zip_path, prefix='',
                                suffix='.xml') as tmp:
            tmp_path = tmp.name
            tmp_basename = os.path.basename(tmp_path)
            zip_name = tmp_basename.split('.')[0] + '.zip'
            full_zip_path = os.path.join(self._zip_path, zip_name)

            with ZipFile(full_zip_path, mode='w') as arc:
                et.write(tmp)
                tmp.flush()
                arc.write(tmp_path, arcname=tmp_basename)

    def gen_csv_files(self, processes_count=DEFAULT_PROCESSES_COUNT) -> None:
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
            names_writer = csv.DictWriter(nf, fieldnames=('id', 'name'))
            names_writer.writeheader()

            for id_, level, object_ids_names in results:
                levels_writer.writerow({'id': id_, 'level': level})
                for name in object_ids_names:
                    names_writer.writerow({'id': id_, 'name': name})

    def _process_single_zip(self, zip_name: str) -> Tuple[str, int, List[str]]:
        zip_path = os.path.join(self._zip_path, zip_name)
        with ZipFile(zip_path) as zip_file:
            name = zip_file.namelist()[0]
            buf = zip_file.read(name)
            root = ET.fromstring(buf.decode())

        vars = root.findall('var')
        if vars[0].get('name') == 'id':
            id_, level = vars[0].get('value'), int(vars[1].get('value'))
        else:
            id_, level = vars[1].get('value'), int(vars[0].get('value'))

        objects = root.find('objects')
        object_ids_names = [obj.get('name') for obj in objects]

        return id_, level, object_ids_names


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
