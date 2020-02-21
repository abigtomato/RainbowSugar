# 最小二乘法实现线性回归


import numpy as np
import matplotlib.pyplot as plt


# 平方损失函数
def compute_cost(w, b, points):
    total_cost = 0
    M = len(points)

    for i in range(M):
        x = points[i, 0]
        y = points[i, 1]
        total_cost += (y - w * x - b) ** 2

    return total_cost / M


# 定义求平均值的函数
def average(data):
    sum = 0
    num = len(data)

    for i in range(num):
        sum += data[i]

    return sum / num


# 定义核心拟合函数
def fit(points):
    M = len(points)
    x_bar = average(points[:, 0])

    sum_yx = 0
    sum_x2 = 0
    sum_delta = 0

    for i in range(M):
        x = points[i, 0]
        y = points[i, 1]
        sum_yx += y * (x - x_bar)
        sum_x2 += x ** 2
    w = sum_yx / (sum_x2 - M * (x_bar ** 2))

    for i in range(M):
        x = points[i, 0]
        y = points[i, 1]
        sum_delta += (y - w * x)
    b = sum_delta / M

    return w, b


if __name__ == '__main__':
    # 导入数据
    points = np.genfromtxt('./data.csv', delimiter=',')

    x = points[:, 0]
    y = points[:, 1]

    print(x)
    print(y)

    plt.scatter(x, y)
    plt.show()

    # 测试
    w, b = fit(points)
    print("w is: ", w)
    print("b is: ", b)

    cost = compute_cost(w, b, points)
    print("cost is: ", cost)

    # 绘制拟合曲线
    plt.scatter(x, y)

    pred_y = w * x + b
    plt.plot(x, pred_y, c='r')
    plt.show()
