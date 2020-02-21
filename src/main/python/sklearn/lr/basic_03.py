# 梯度下降法实现线性回归

import numpy as np
import matplotlib.pyplot as plt


if __name__ == '__main__':
    # 导入数据
    points = np.genfromtxt('./data.csv', delimiter=',')

    x = points[:, 0]
    y = points[:, 1]

    print(x)
    print(y)

    plt.scatter(x, y)
    plt.show()
