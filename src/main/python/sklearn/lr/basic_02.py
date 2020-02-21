# sklearn 库实现线性回归


import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression


# 平方损失函数
def compute_cost(w, b, points):
    total_cost = 0
    M = len(points)

    for i in range(M):
        x = points[i, 0]
        y = points[i, 1]
        total_cost += (y - w * x - b) ** 2

    return total_cost / M


if __name__ == '__main__':
    # 导入数据
    points = np.genfromtxt('./data.csv', delimiter=',')

    x = points[:, 0]
    y = points[:, 1]

    print(x)
    print(y)

    plt.scatter(x, y)
    plt.show()

    # 使用sklearn库提供的线性回归算法
    lr = LinearRegression()

    new_x = x.reshape(-1, 1)
    new_y = y.reshape(-1, 1)

    print(new_x)
    print(new_y)

    lr.fit(new_x, new_y)

    w = lr.coef_[0][0]
    b = lr.intercept_[0]

    print("w is: ", w)
    print("b is: ", b)

    cost = compute_cost(w, b, points)

    print("cost is: ", cost)

    # 绘制拟合曲线
    plt.scatter(x, y)

    pred_y = w * x + b
    plt.plot(x, pred_y, c='r')
    plt.show()
