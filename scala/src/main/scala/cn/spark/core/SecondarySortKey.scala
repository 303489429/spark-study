package scala.cn.spark.core

/**
  * Created by wangzhilong on 2017/6/5.
  */
class SecondarySortKey(val first: Int,val second: Int) extends Ordered[SecondarySortKey] with Serializable{
  override def compare(that: SecondarySortKey): Int = {
    if(this.first - that.first !=0 ){
      this.first - that.first
    }else {
      this.second - that.second
    }
  }
}
