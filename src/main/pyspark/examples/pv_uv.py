from pyspark import SparkConf, SparkContext


def get_top3_user(one):
    site = one[0]
    localIterable = one[1]

    localDic = {}

    for local in localIterable:
        if local in localDic:
            localDic[local] += 1
        else:
            localDic[local] = 1

    sort_list = sorted(localDic.items(), key=lambda tp: tp[1], reverse=True)
    returnResult = []
    if len(sort_list) > 2:
        for i in range(2):
            returnResult.append(sort_list[i])
    else:
        returnResult = sort_list
    return site, returnResult


def get_user_count(one):
    uid = one[0]
    siteIterable = one[1]
    siteDic = {}
    for site in siteIterable:
        if site in siteDic:
            siteDic[site] += 1
        else:
            siteDic[site] = 1
    returnList = []
    for site, count in siteDic.items():
        returnList.append((site, (uid, count)))
    return returnList


def get_top3(one):
    site = one[0]
    uid_count_iterables = one[1]

    top3_uid_count = ["", "", ""]
    for uid_count in uid_count_iterables:
        uid = uid_count[0]
        count = uid_count[1]
        for i in range(3):
            if top3_uid_count[i] == "":
                top3_uid_count[i] = uid_count
                break
            elif top3_uid_count[i][1] < count:
                for j in range(2, i, -1):
                    top3_uid_count[j] = top3_uid_count[j-1]
                top3_uid_count[i] = uid_count
                break
    return site, top3_uid_count


if __name__ == '__main__':
    conf = SparkConf()
    conf.setMaster("local")
    conf.setAppName("pvuv")

    sc = SparkContext(conf=conf)
    lines = sc.textFile("../resources/data/pvuvdata")

    # 统计每个网站下最活跃的top3用户
    uid_site = lines.map(lambda one: (one.split("\t")[2], one.split("\t")[4]))
    site_uid_count = uid_site.groupByKey().flatMap(lambda one: get_user_count(one))
    site_uid_count.groupByKey().map(lambda one: get_top3(one)).foreach(print)

    # 统计每个网站最活跃的top2地区
    site_local = lines.map(lambda line: (line.split("\t")[4], line.split("\t")[3]))
    site_local.groupByKey().map(lambda one: get_top3_user(one)).foreach(print)

    # 统计uv
    ip_site = lines\
        .filter(lambda one: one.split("\t")[3] == 'beijing')\
        .map(lambda line: line.split("\t")[1] + "_" + line.split("\t")[4])
    ip_site.distinct()\
        .map(lambda one: (one.split("_")[1], 1))\
        .reduceByKey(lambda v1,v2:v1+v2).foreach(print)

    # 统计pv
    site_count = lines.map(lambda line: (line.split("\t")[4], 1))
    total_site_count = site_count.reduceByKey(lambda v1, v2: v1+v2)

    total_site_count.sortBy(lambda tp: tp[1], ascending=False).foreach(print)
