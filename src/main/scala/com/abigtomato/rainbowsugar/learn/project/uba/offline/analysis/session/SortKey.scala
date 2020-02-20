package com.abigtomato.rainbowsugar.learn.project.uba.offline.analysis.session

/**
 * 自定义排序方式
 * @param clickCount
 * @param orderCount
 * @param payCount
 */
case class SortKey(clickCount: Long, orderCount: Long, payCount: Long) extends Ordered[SortKey] {

  /**
   * compare > 0 = this > that
   * compare < 0 = this < thar
   * @param that
   * @return
   */
  override def compare(that: SortKey): Int = {
    if ((this.clickCount - that.clickCount) != 0) {
      (this.clickCount - that.clickCount).toInt
    } else if ((this.orderCount - that.orderCount) != 0) {
      (this.orderCount - that.orderCount).toInt
    } else {
      (this.payCount - that.payCount).toInt
    }
  }
}
