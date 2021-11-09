import fdb
import csv
from datetime import datetime

from logger import Logger

login_fdb = 'login'
password_fdb = 'password'
port_fdb = '/1111'

log = Logger('Tax')


def get_time():
    return datetime.now().strftime('%Y%m%d-%H%M')


def check_fdb(name_out_file, paths_to_db):
    for ip, path in paths_to_db.items():
        try:
            con = fdb.connect(host=ip + port_fdb,
                              database=path,
                              user=login_fdb,
                              password=password_fdb,
                              charset='WIN1251')
            print(f'{ip} / {path} = True')

            cur = con.cursor()

            node_id = get_node_id(cur)
            dep_ids, dep_ids_name = get_dep_ids(cur, node_id)
            tax = get_taxsys(cur, dep_ids)
            write_data_csv(name_out_file, dep_ids_name, tax)
        except Exception as e:
            log.write_log(f'{path} Error connection to DB: {e} = False')


def get_node_id(cursor):
    cursor.execute(f'select node_id from organization')
    result = cursor.fetchone()[0]
    result = str(result + 1000)
    return result[1:]


def get_dep_ids(cursor, node):
    cursor.execute(f"select dep_id, dep_name from department where dep_id like '%_{node}%'")
    responce = cursor.fetchall()
    res_dict = dict()
    res_dep = []
    for r in responce:
        res_dict[r[0]] = r[1]
        res_dep.append(str(r[0]))
    dep = tuple(res_dep)
    return dep, res_dict


def get_taxsys(cursor, dep_ids):
    cursor.execute(f"select taxsys_id, ban_mark_items from department where dep_id in {dep_ids}")
    return cursor.fetchall()


def write_data_csv(name_out_file, dep, tax):
    try:
        with open(name_out_file, 'a', newline='') as f:
            spam_writer = csv.writer(f, delimiter=';', quotechar='|', quoting=csv.QUOTE_MINIMAL)

            for k, v, t in zip(dep.keys(), dep.values(), tax):
                print(k, v, t)
                spam_writer.writerow((k, v, t[0], t[1]))

    except Exception as ex:
        log.write_log(f'Error write file outfile.csv: {ex}')


def get_paths_to_db():
    paths = {}
    try:
        with open('ip_pharms.csv', newline='') as f:
            spamreader = csv.reader(f, delimiter=';', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            for row in spamreader:
                paths[row[0]] = row[1]
    except Exception as e:
        log.write_log(f'Error read file ip_pharms.csv: {e}')
    return paths


if __name__ == '__main__':
    name_out_file = get_time() + 'out_file.csv'
    paths_to_db = get_paths_to_db()
    check_fdb(name_out_file, paths_to_db)
