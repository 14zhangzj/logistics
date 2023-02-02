package cn.itcast.logistics.test

import com.alibaba.fastjson.JSON

object FastJsonTest {
	
	def main(args: Array[String]): Unit = {
		
		val jsonStr: String = ""
		JSON.parseObject(jsonStr, classOf[BeanTest])
		
		val bean = new BeanTest(101, "zhangsan")
		val jsonBean: AnyRef = JSON.toJSONString(bean, true)
		
	}
	
}
