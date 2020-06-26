package neoflex

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.tuple.{Tuple2 => FlinkTuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction
import org.apache.flink.state.api.{OperatorTransformation, Savepoint}

import scala.beans.BeanProperty
import scala.util.Try

object StateWriter extends App {
  val fieldDelimiter = ";"
  val params = getParams(args)
  val env = ExecutionEnvironment.getExecutionEnvironment
  val backEnd = new RocksDBStateBackend(params.rocksDbStateBackendUri)

//    class ValueStateBootstrapper extends KeyedStateBootstrapFunction[String, FlinkTuple2[String, String]] {
//      var valueState: ValueState[String] = _
//
//      override def open(parameters: Configuration): Unit = {
//        val valueDescriptor = new ValueStateDescriptor(params.valueStateName, Types.STRING)
//        valueState = getRuntimeContext.getState(valueDescriptor)
//      }
//
//      override def processElement(value: FlinkTuple2[String, String], ctx: KeyedStateBootstrapFunction[String, FlinkTuple2[String, String]]#Context): Unit = {
//        valueState.update(value.f1)
//      }
//    }
//
//    val valueStateDataSet = env
//      .readCsvFile(params.valueStateCsvFilePath)
//      .fieldDelimiter(fieldDelimiter)
//      .types(classOf[String], classOf[String])
//
//    val transformation = OperatorTransformation
//      .bootstrapWith(valueStateDataSet)
//      .keyBy(r => r.f0)
//      .transform(new ValueStateBootstrapper)

  class ListStateBootstrapper extends KeyedStateBootstrapFunction[String, ListStateValue] {
    var listState: ListState[String] = _

    override def open(parameters: Configuration): Unit = {
      val listDescriptor = new ListStateDescriptor(params.valueStateName, Types.STRING)
      listState = getRuntimeContext.getListState(listDescriptor)
    }

    override def processElement(value: ListStateValue, ctx: KeyedStateBootstrapFunction[String, ListStateValue]#Context): Unit = {
      listState.update(value.listValue)
    }
  }

  val listStateDataSet = env
    .readCsvFile(params.listStateCsvFilePath)
    .fieldDelimiter(fieldDelimiter).pojoType(classOf[ListStateValue], "key", "listValue")
//    .types(classOf[String], classOf[java.util.List[String]])

  val transformation = OperatorTransformation
    .bootstrapWith(listStateDataSet)
    .keyBy(_.key)
    .transform(new ListStateBootstrapper)

  Savepoint
    .load(env, params.savepointPath, backEnd)
    .removeOperator(params.operatorUid)
    .withOperator(params.operatorUid, transformation)
    .write(params.savepointPath + "_modified")

  env.execute("state write job")

  private def getParams(args: Array[String]): StateInteractorParams = {
    val savepointPath = Try(args(0)).toOption.getOrElse("C:/flink-data/savepoints/savepoint-342210-7879f8ccc844")
    val operatorUid = Try(args(1)).toOption.getOrElse("testOperator")
    val listStateName = Try(args(2)).toOption.getOrElse("testStringListState")
    val valueStateName = Try(args(3)).toOption.getOrElse("testValueState")
    val valueStateCsvFilePath = Try(args(4)).toOption.getOrElse("file:///home/osboxes/flink-data/valueState.csv")
    val listStateCsvFilePath = Try(args(5)).toOption.getOrElse("file:///home/osboxes/flink-data/listState.csv")
    val rocksDbStateBackendUri = Try(args(6)).toOption.getOrElse("file:///home/osboxes/flink-data/stateTestProjectReadState/")
    StateInteractorParams(savepointPath, operatorUid, listStateName, valueStateName, valueStateCsvFilePath, listStateCsvFilePath, rocksDbStateBackendUri)
  }
}

class ListStateValue(@BeanProperty var key: String, @BeanProperty var listValue: java.util.List[String])

case class StateInteractorParams(savepointPath: String,
                                 operatorUid: String,
                                 listStateName: String,
                                 valueStateName: String,
                                 valueStateCsvFilePath: String,
                                 listStateCsvFilePath: String,
                                 rocksDbStateBackendUri: String)
