import os
import gzip

start_time = "2019-08-01"
end_time = "2019-08-01"


def is_in_time(path):
    time_path = path.find("2019-08")
    if start_time <= path[time_path:time_path+10] <= end_time:
        return True
    else:
        return False


def is_zhubi_raw_file(path):
    if os.path.isfile(path) and \
            is_in_time(path) and \
            path.find("zhubi") > 0 and not path.endswith(".gz") \
            and not path.endswith(".swp"):
        return True
    else:
        return False


def is_tick_raw_file(path):
    if os.path.isfile(path) \
            and is_in_time(path) \
            and path.find("tick") > 0 and not path.endswith(".gz") \
            and not path.endswith(".swp"):
        return True
    else:
        return False


def is_zhubi_gz_file(path):
    if os.path.isfile(path) \
            and is_in_time(path) \
            and path.find("zhubi") > 0 and path.endswith(".gz"):
        return True
    else:
        return False


def is_tick_gz_file(path):
    if os.path.isfile(path) \
            and is_in_time(path)  \
            and path.find("tick") > 0 and path.endswith(".gz"):
        return True
    else:
        return False


def is_gz_file(path):
    if os.path.isfile(path) \
            and is_in_time(path) \
            and path.endswith(".gz"):
        return True
    else:
        return False


# 解压gz，事实上就是读出当中的单一文件
def un_gz(file_name):
    """ungz zip file"""
    f_name = file_name.replace(".gz", "")
    # 获取文件的名称，去掉
    g_file = gzip.GzipFile(file_name)
    # 创建gzip对象
    open(f_name, "w").write(g_file.read().__str__())
    # gzip对象用read()打开后，写入open()建立的文件里。
    g_file.close()
    return f_name
    # 关闭gzip对象
