# pv uv topN 统计示例


from pyspark import SparkConf, SparkContext


def get_top3_area(one):
    site = one[0]
    local_iterable = one[1]

    local_dic = {}
    for local in local_iterable:
        if local in local_dic:
            local_dic[local] += 1
        else:
            local_dic[local] = 1

    sort_list = sorted(local_dic.items(), key=lambda tp: tp[1], reverse=True)

    return site, sort_list[:3]


def get_user_count(one):
    uid = one[0]
    site_iterable = one[1]

    site_dic = {}
    for site in site_iterable:
        if site in site_dic:
            site_dic[site] += 1
        else:
            site_dic[site] = 1

    return_list = []
    for site, count in site_dic.items():
        return_list.append((site, (uid, count)))
    return return_list


def get_top3_user(one):
    site = one[0]
    uid_count_iterables = one[1]

    # 定长数组取top3
    top3_uid_count = ["", "", ""]
    for uid_count in uid_count_iterables:
        count = uid_count[1]
        for i in range(3):
            if top3_uid_count[i] == "":
                top3_uid_count[i] = uid_count
                break
            elif top3_uid_count[i][1] < count:
                for j in range(2, i, -1):
                    top3_uid_count[j] = top3_uid_count[j - 1]
                top3_uid_count[i] = uid_count
                break
    return site, top3_uid_count


if __name__ == '__main__':
    conf = SparkConf()\
        .setMaster("local")\
        .setAppName("pv_uv")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("../../resources/data/pv_uv_data.csv")

    # 1.统计pv
    lines.map(lambda line: (line.split(",")[4], 1))\
        .reduceByKey(lambda v1, v2: v1 + v2)\
        .sortBy(lambda tp: tp[1], ascending=False)\
        .foreach(print)

    # 2.统计uv
    lines.filter(lambda one: one.split(",")[3] == 'beijing')\
        .map(lambda line: line.split(",")[1] + "_" + line.split(",")[4])\
        .distinct()\
        .map(lambda one: (one.split("_")[1], 1))\
        .reduceByKey(lambda v1, v2: v1 + v2)\
        .sortBy(lambda tp: tp[1], ascending=False)\
        .foreach(print)

    # 3.统计每个网站最活跃的top3地区
    lines.map(lambda line: (line.split(",")[4], line.split(",")[3]))\
        .groupByKey()\
        .map(lambda one: get_top3_area(one))\
        .foreach(print)

    # 4.统计每个网站下最活跃的top3用户
    uid_site = lines\
        .map(lambda one: (one.split(",")[2], one.split(",")[4]))\
        .groupByKey()\
        .flatMap(lambda one: get_user_count(one))\
        .groupByKey()\
        .map(lambda one: get_top3_user(one))\
        .foreach(print)

    sc.stop()
