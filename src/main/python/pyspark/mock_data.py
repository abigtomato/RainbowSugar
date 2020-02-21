# 模拟数据，写入 src/main/resources/data 下的 pv_uv_data.csv


import time
import random


def mock(path):
    date = time.strftime('%Y-%m-%d')
    
    # ip = '192.168.' + str(random.randint(0, 255)) + '.' + str(random.randint(0, 255))
    # ip = '%s%s%s%s' % ('192.168.', str(random.randint(0, 255)), '.', str(random.randint(0, 255)))
    ip_list = ['192.168', str(random.randint(0, 255)), str(random.randint(0, 255))]
    ip = '.'.join(ip_list)
    
    # 5位的user_id
    user_id = get_user_id()
    
    locations = ['beijing', 'shanghai', 'guangzhou', 'shandong', 'shenzhen', 'chongqing']
    # random.randint(0, 5) 包含0也包含5
    location = locations[random.randint(0, 5)]
    
    for j in range(0, random.randint(1, 10)):
        websites = ['www.baidu.com', 'www.xiaomi.com', 'www.jd.com', 'www.taobao.com', 'www.qq.com', 'www.360.com', 'www.dangdang.com']
        website = websites[random.randint(0, 6)]
        
        operations = ['register', 'view', 'login', 'logout', 'buy', 'comment', 'jump']
        operation = operations[random.randint(0, 6)]
        
        one_info = date + ',' + ip + ',' + "uid" + user_id + ',' + location + ',' + website + ',' + operation
        print(one_info)
        write_log_to_file(path, one_info)


def get_user_id():
    id_str = str(random.randint(0, 99999))
    tmp_str = ""
    if len(id_str) < 5:
        for _ in range(0, (5 - len(id_str))):
            tmp_str += "0"
    return tmp_str + id_str


def write_log_to_file(path, log):
    # 'r'：读，'w'：写，'a'：追加
    # 'r+' == r+w（可读可写，文件若不存在就报错 IOError）
    # 'w+' == w+r（可读可写，文件若不存在就创建）
    # 'a+' == a+r（可追加可写，文件若不存在就创建）
    # 对应的，如果是二进制文件，就都加一个b： rb' 'wb' 'ab' 'rb+' 'wb+' 'ab+'
    with open(path, 'a+') as f:
        f.writelines(log + '\n')


if __name__ == '__main__':
    output_path = "../../resources/data/pv_uv_data.csv"
    for i in range(1, 10000):
        mock(output_path)
