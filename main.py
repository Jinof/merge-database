import pymysql
from operator import itemgetter

# 让我们一起merge两个数据库吧
# 操作流程
# 1.统计passport要合并到nucos中的表p_tbs
# 2.统计passport和ncuos中重复的表duplicated_tbs
# 3.统计passport和ncuos中不一致的数据
# 4.合并passport和ncuos的duplicated_tbs, 并更新存在外健约束的表
# 5.合并passport和ncuos的非重复表
# 6.创建数据库, 线上测试
# 7.线上服务正式迁移数据库

connection = pymysql.connect(
    host='127.0.0.1',
    user='root',
    password='123456',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor,
)


def get_tbs(db: str) -> list:
    connection.select_db(db)
    cursor = connection.cursor()
    sql = 'show tables'
    cursor.execute(sql)
    kv_tbs = cursor.fetchall()
    tbs = []
    for kv in kv_tbs:
        for _, v in kv.items():
            tbs.append(v)
    cursor.close()
    return tbs


def get_p_tbs() -> list:
    return get_tbs("passport")


def get_n_tbs() -> list:
    return get_tbs("ncuos")


def count_duplicated_tbs(l1: list, l2: list) -> list:
    dup = []
    for v1 in l1:
        for v2 in l2:
            if v1 == v2:
                dup.append(v1)
    return dup


def handle_data(l1: list, l2: list) -> list:
    data = []
    for v1 in l1:
        has_equal = False
        for v2 in l2:
            if v1 == v2:
                has_equal = True
                data.append([v1, v2, v1 == v2])
        if has_equal is not True:
            data.append([v1, "-", False])

    return data


def gen_format_str() -> str:
    gap = 30
    gap_str = ""
    for i in range(0, 3):
        gap_str += "{" + str(i) + ":<" + str(gap) + "}"
    return gap_str


def get_prints_tbs(l1: list, l2: list) -> list:
    fmt_str = gen_format_str()
    pts = [fmt_str.format("passport", "nucos", "是否重复")]
    data = handle_data(l1, l2)
    data.sort(key=itemgetter(2))
    for i in data:
        pts.append(fmt_str.format(i[0], i[1], i[2]))

    return pts


def take_third(elem):
    return elem[2]


def get_tab_struct(d: str, t: str) -> list:
    cursor = connection.cursor()
    sql = """
        SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_DEFAULT, IS_NULLABLE FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = '{database}' AND TABLE_NAME = '{table}'
    """.format(database=d, table=t)
    cursor.execute(sql)
    struct = cursor.fetchall()
    cursor.close()
    return struct


def take_column_name(elem):
    return elem["COLUMN_NAME"]


def compare_struct(t: str) -> list:
    p_struct = get_tab_struct("passport", t)
    n_struct = get_tab_struct("ncuos", t)
    p_struct.sort(key=take_column_name)
    n_struct.sort(key=take_column_name)

    # 表结构没变跳过
    if p_struct == n_struct:
        return []

    p_set = set()
    n_set = set()
    for i in p_struct:
        p_set.add(i["COLUMN_NAME"] + " " + i["COLUMN_TYPE"] + " " + str(i["COLUMN_DEFAULT"]) + " " + i["IS_NULLABLE"])
    for i in n_struct:
        n_set.add(i["COLUMN_NAME"] + " " + i["COLUMN_TYPE"] + " " + str(i["COLUMN_DEFAULT"]) + " " + i["IS_NULLABLE"])

    return [(p_set | n_set) - (p_set & n_set)]


def get_count(d: str, t: str) -> str:
    cursor = connection.cursor()
    sql = """
        SELECT COUNT(*) FROM {database}.{table}
    """.format(database=d, table=t)
    cursor.execute(sql)
    count = cursor.fetchall()
    cursor.close()
    return count


def get_data(d: str, t: str) -> list:
    cursor = connection.cursor()
    sql = """
        SELECT * FROM {database}.{table}
    """.format(database=d, table=t)
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()
    return data


def analyze_data(d1: list, d2: list) -> [int, int]:
    changed = 0
    # 以少数据为基准放在d2
    if len(d1) < len(d2):
        d1, d2 = d2, d1
    new = len(d1) - len(d2)
    for i in range(0, len(d2)):
        l1, l2 = [], []
        # 以长度小的为基准放在l2
        if len(d1[0]) > len(d2[0]):
            l1, l2 = d1[i], d2[i]
        else:
            l1, l2 = d2[i], d1[i]
        for k, v in l2.items():
            if v != l1[k]:
                changed += 1
                break
    return changed, new


if __name__ == '__main__':
    p_tbs = get_p_tbs()
    n_tbs = get_n_tbs()
    pts = get_prints_tbs(p_tbs, n_tbs)
    for i in pts:
        print(i)

    dup_tbs = count_duplicated_tbs(p_tbs, n_tbs)
    struct_changed = {}
    for i in dup_tbs:
        different = compare_struct(i)
        if len(different) != 0:
            struct_changed[i] = different
    # print(struct_changed)
    export_tbs = []
    for i in p_tbs:
        if str.__contains__(i, "dm") or str.__contains__(i, "xiao_hei_wu") or \
                str.__contains__(i, "wp") or str.__contains__(i, "h5") or \
                str.__contains__(i, "h5") or str.__contains__(i, "deal") or \
                str.__contains__(i, "cet") or str.__contains__(i, "chat") or \
                str.__contains__(i, "notice") or str.__contains__(i, "society") or \
                str.__contains__(i, "test") or str.__contains__(i, "incu"):
            continue
        print(i)
        export_tbs.append(i)
    mysqldump = "mysqldump -uroot -p123456 -h127.0.0.1 -P3306 --column-statistics=0 --set-gtid-purged=OFF " \
                "--databases passport --tables"
    for i in export_tbs:
        mysqldump += " " + i
    mysqldump += ">/home/zjy/mysqldump/passport-mysqldump.sql"
    print(mysqldump)

    print("table", "passport", "ncuos", "是否不同")
    for i in dup_tbs:
        p_count = get_count("passport", i)
        n_count = get_count("ncuos", i)
        p_data = get_data("passport", i)
        n_data = get_data("ncuos", i)
        is_struct_changed = i in struct_changed
        if p_data != n_data:
            changed, new = analyze_data(p_data, n_data)
            print(i, p_count, n_count, "结构是否改变: " + str(is_struct_changed),
                  "修改了{changed}行, 新增{new}行".format(changed=changed, new=new),
                  struct_changed[i] if is_struct_changed else False)
