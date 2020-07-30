package com.kevin.scalaLikejdbc

import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

object ScalaJdbcLike {
  def main(args: Array[String]): Unit = {
    //默认加载default配置信息
    DBs.setup()
    //加载自定义的fred配置信息
    DBs.setup('fred)
    //配置mysql
    DBs.setup()

    //查询数据并返回单个列，并将列数据封装到集合中
    val list = DB.readOnly({implicit session =>
      SQL("select content from post")
        .map(rs =>
          rs.string("content")).list().apply()
    })
    for(s <- list){
      println(s)
    }


  }


}
