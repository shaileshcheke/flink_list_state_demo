package queryablestate.functions

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import queryablestate.datatypes.WordWithCount


class QueryableListStateFMImple extends RichFlatMapFunction[WordWithCount,Array[WordWithCount]]{
  
  private lazy val countState = getRuntimeContext.getState(countDesc)
  private val countDesc = new ValueStateDescriptor("total-count"
      ,TypeInformation.of(new TypeHint[Int]() {}), 0)
  
  private lazy val listState = getRuntimeContext.getListState(listStateDescriptor)
  private val listStateDescriptor = new ListStateDescriptor("series"
    ,TypeInformation.of(new TypeHint[WordWithCount]() {}))
  listStateDescriptor.setQueryable("series")
  
  
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
   }
  
  override def flatMap(value: WordWithCount, out: Collector[Array[WordWithCount]]): Unit={
    
    var count = countState.value
    val maxWindowSize: Int = 8
    if (count == maxWindowSize) {
      listState.clear()
      count = 0
    }
    
    listState.add(value)
    
    countState.update(count + 1)
  } 
  
}